(ns skuld.node
  "A single node in the Skuld cluster. Manages any number of vnodes."
  (:use skuld.util)
  (:require [skuld.vnode :as vnode]
            [skuld.net :as net]
            [skuld.flake :as flake]
            [skuld.clock-sync :as clock-sync]
            [skuld.aae :as aae]
            [skuld.task :as task]
            [skuld.curator :as curator]
            [clj-helix.manager :as helix]
            clj-helix.admin
            clj-helix.fsm
            [clj-helix.route :as route])
  (:import (java.util Arrays)
           com.aphyr.skuld.Bytes))

(defn vnodes
  "Returns a map of partitions to vnodes for a node."
  [node]
  @(:vnodes node))

(defn vnode
  "Returns a particular vnode for a node."
  [node partition-id]
  (get (vnodes node) partition-id))

(defn tasks
  "Given a node, emits all tasks for all vnodes."
  [node]
  (->> node vnodes vals (mapcat vnode/tasks)))

(defn helix-admin
  "Returns a HelixAdmin for a given node, via its participant."
  [node]
  (-> node :participant helix/admin))

(defn cluster-name
  "The name of a cluster a node is participating in."
  [node]
  (-> node :participant helix/cluster-name))

(def num-partitions
  "The number of partitions in the cluster."
  (memoize (fn [node]
             (-> node
                 helix-admin
                 (clj-helix.admin/resource-ideal-state
                   (cluster-name node)
                   :skuld)
                 .getNumPartitions))))

(def num-replicas
  "How many replicas are there for the Skuld resource?"
  (memoize (fn [node]
             (-> node
                 helix-admin
                 (clj-helix.admin/resource-ideal-state
                   (cluster-name node)
                   :skuld)
                 .getReplicas
                 Integer.))))

(defn partition-name
  "Calculates which partition is responsible for a given ID."
  [node ^Bytes id]
  (str "skuld_" (-> id
      .bytes
      Arrays/hashCode
      (mod (num-partitions node)))))

(defn all-partitions
  "A list of all partitions in the system."
  [node]
  (->> node
       num-partitions
       range
       (map (partial str "skuld_"))))

(defn peers
  "All peers which own a partition, or all peers in the cluster."
  ([node]
   (map #(select-keys % [:host :port])
        (route/instances (:router node) :skuld :peer)))
  ([node part]
   (map #(select-keys % [:host :port])
        (route/instances (:router node) :skuld part :peer))))

(defn preflist
  "Returns a set of nodes responsible for a Bytes id."
  [node ^Bytes id]
  (assert (not (nil? id)))
  (peers node (partition-name node id)))

(defn enqueue!
  "Proxies to enqueue-local on all nodes in the preflist for this task."
  [node msg]
  (let [task (task/task (:task msg))
        id (:id task)]
    (let [r (or (:w msg) 1)
          preflist (preflist node id)
          responses (net/sync-req! (:net node)
                                   preflist
                                   {:r r}
                                   {:type    :enqueue-local
                                    :task    task})
          acks (remove :error responses)]
      (if (<= r (count acks))
        {:n         (count acks)
         :id        id}
        {:n         (count acks)
         :id        id
         :error     (str "not enough acks from " (prn-str preflist))
         :responses responses}))))

(defn enqueue-local!
  "Enqueues a message on the local vnode for this task."
  [node msg]
  (let [task (:task msg)
        part (partition-name node (:id task))]
    (if-let [vnode (vnode node part)]
      (do (vnode/enqueue! vnode task)
          {:task-id (:id task)})
      {:error (str "I don't have partition" part "for task" (:id task))})))

(defn get-task
  "Gets the current state of a task."
  [node msg]
  (let [id (:id msg)
        r  (or (:r msg) 2)
        responses (net/sync-req! (:net node)
                                 (preflist node id)
                                 {:r r}
                                 {:type :get-task-local
                                  :id   id})
        acks (remove :error responses)
        task (->> responses (map :task) (reduce task/merge))]
    (if (<= r (count acks))
      {:n    (count acks)
       :task task}
      {:n    (count acks)
       :task task
       :error "not enough acks"
       :responses responses})))

(defn get-task-local
  "Gets the current state of a task from a local vnode"
  [node msg]
  (let [id (:id msg)
        part (partition-name node id)]
    (if-let [vnode (vnode node part)]
      {:task (vnode/get-task vnode id)}
      {:error (str "I don't have partition" part "for task" id)})))

(defn count-tasks
  "Estimates the total number of tasks in the system."
  [node msg]
  (let [parts  (set (all-partitions node))
        counts (atom {})
        done   (promise)]

    ; Issue requests to all nodes for their local couns
    (doseq [peer (route/instances (:router node) :skuld :peer)]
      (net/req! (:net node) [peer] {:r 1} {:type :count-tasks-local}
                [[response]]
                (let [remote-counts (:partitions response)
                      counts (swap! counts
                                    (partial merge-with max remote-counts))]
                  (when (= parts (set (keys counts)))
                    ; We've accrued a response for each partition.
                    (deliver done
                             {:partitions counts
                              :count (reduce + (vals counts))})))))

    (deref done 5000 {:error "timed out" :partitions @counts})))

(defn count-tasks-local
  "Estimates the total number of tasks on the local node."
  [node msg]
  {:partitions
   (reduce (fn [counts [k vnode]]
             (if (vnode/active? vnode)
               (assoc counts k (vnode/count-tasks vnode))
               counts))
           {}
           (vnodes node))})

(defn cover
  "Returns a map of nodes to lists of partitions on that node, such that each
  partition appears exactly once. Useful when you want to know something about
  every partition."
  [node]
  (->> node
       all-partitions
       (reduce (fn [m part]
                 (let [peer (first (peers node part))]
                   (update-in m [peer] conj part)))
               {})))

(defn coverage
  "Issues a query to a cover of nodes. The message sent to each peer will
  include a new key :partitions with a value like [\"skuld_0\" \"skuld_13\"
  ...].  The responses for this message should look like:

  {:partitions {\"skuld_0\" some-value \"skuld_13\" something-else ...}

  Coverage will return a map of partition names to one value for each
  partition."
  [node msg]
  (let [responses (atom {})
        done      (promise)
        cover     (cover node)
        all-parts (set (all-partitions node))]
    (doseq [[peer parts] cover]
      (net/req! (:net node) [peer] {} (assoc msg :partitions parts)
                [[response]]
                (let [responses (swap! responses merge (:partitions response))]
                  (when (= all-parts (set (keys responses)))
                    (deliver done responses)))))
    (or (deref done 5000 false)
        (throw (RuntimeException.
                 "did not receive a complete set of responses for coverage query")))))

(defn list-tasks
  "Lists all tasks in the system."
  [node msg]
  {:tasks (->> {:type :list-tasks-local}
               (coverage node)
               vals
               (apply sorted-interleave-by :id))})

(defn list-tasks-local
  [node msg]
  {:partitions (reduce (fn [m part]
                         (if-let [vnode (vnode node part)]
                           (if (vnode/active? vnode)
                             (assoc m part (vnode/tasks vnode))
                             m)
                           m))
                       {}
                       (:partitions msg))})

(defn claim-local!
  "Tries to claim a task from a local vnode."
  [node msg]
  {:task
   (->> node
        vnodes
        vals
        (filter vnode/leader?)
        (some (fn [vnode]
                (try 
                  (vnode/claim! vnode (or (:dt msg) 10000))
                  (catch Throwable t nil)))))})

(defn claim!
  "Tries to claim a task."
  [node msg]
  ; Try a local claim first
  (or (let [t (claim-local! node msg)]
        (and (:task t) t))
      ; Ask each peer in turn for a task
      (loop [[peer & peers] (shuffle (disj (set (peers node))
                                           (net/id (:net node))))]
        (if-not peer
          ; Done
          {}
          (do
            (let [[response] (net/sync-req! (:net node) [peer] {}
                                            (assoc msg :type :claim-local))]
              (if (:task response)
                response
                (recur peers))))))))

(defn request-claim!
  "Accepts a request from a leader to claim a given task."
  [node msg]
  (vnode/request-claim!
    (->> msg
         :id
         (partition-name node)
         (vnode node))
    msg))

(defn complete-local!
  "Completes a given task on a local vnode."
  [node msg]
  (let [part (->> msg :task-id (partition-name node))]
    (if-let [vnode (vnode node part)] 
      (do (vnode/complete! vnode msg)
          {:w 1})
      {:error (str "I don't have partition" part "for task" (:task-id msg))})))

(defn complete!
  "Completes a given task in a given run. Proxies to all nodes owning that
  task."
  [node msg]
  (let [w (or (:w msg) 2)
        responses (net/sync-req! (:net node)
                                 (preflist node (:task-id msg))
                                 {:r w}
                                 (merge msg {:type :complete-local
                                             :time (flake/linear-time)}))
        acks (remove :error responses)
        w'    (reduce + (map :w acks))]
    (if (<= w w')
      {:w w'}
      {:w w'
       :error "not enough nodes acknowledged request for complete"
       :responses responses})))

(defn wipe!
  "Wipes all data clean."
  [node msg]
  (net/sync-req! (:net node) (peers node) {} {:type :wipe-local})
  {})

(defn wipe-local!
  "Wipe all data on the local node."
  [node msg]
  (->> node vnodes vals (pmap vnode/wipe!) dorun)
  {})

(defn request-vote!
  "Handles a request for a vote from another node."
  [node msg]
  (if-let [vnode (vnode node (:partition msg))]
    (vnode/request-vote! vnode msg)
    (do
      {:error (str (net/id (:net node))
                           " has no vnode for " (:partition msg))})))

(defn handler
  "Returns a fn which handles messages for a node."
  [node]
  (fn handler [msg]
    ((case (:type msg)
       :enqueue            enqueue!
       :enqueue-local      enqueue-local!
       :get-task           get-task
       :get-task-local     get-task-local
       :count-tasks        count-tasks
       :count-tasks-local  count-tasks-local
       :list-tasks         list-tasks
       :list-tasks-local   list-tasks-local
       :claim              claim!
       :claim-local        claim-local!
       :request-claim      request-claim!
       :complete           complete!
       :complete-local     complete-local!
       :wipe               wipe!
       :wipe-local         wipe-local!
       :request-vote       request-vote!
       (constantly {:error (str "unknown message type" (:type msg))}))
     node msg)))

(def fsm-def (clj-helix.fsm/fsm-definition
               {:name   :skuld
                :states {:DROPPED {:transitions :offline}
                         :offline {:initial? true
                                   :transitions [:peer :DROPPED]}
                         :peer {:priority 1
                                :upper-bound :R
                                :transitions :offline}}}))

(defn fsm
  "Compiles a new FSM to manage a vnodes map. Takes an atom of partitions to
  vnodes, a net node, and a promise of a router."
  [vnodes curator net routerp]
  (clj-helix.fsm/fsm
    fsm-def
    (:offline :peer [part m c]
              (swap! vnodes (fn [vnodes]
                              (if-let [existing (get vnodes part)]
                                (do (vnode/revive! existing)
                                    vnodes)
                                (assoc vnodes part
                                       (vnode/vnode {:partition part
                                                     :curator curator
                                                     :router @routerp
                                                     :net net}))))))

    (:offline :DROPPED [part m c])

    (:peer :offline [part m c]
           (vnode/zombie! (get @vnodes part)))))

(defn node
  "Creates a new node with the given options.

  :zookeeper    \"localhost:2181\"
  :cluster      :skuld
  :host         \"127.0.0.1\"
  :port         13000"
  [opts]
  (let [zk      (get opts :zookeeper "localhost:2181")
        curator (curator/framework zk "skuld")
        host    (get opts :host "127.0.0.1")
        port    (get opts :port 13000)
        cluster (get opts :cluster :skuld)
        vnodes  (atom {})
        net         (net/node {:host host
                               :port port})
        routerp  (promise)
        fsm         (fsm vnodes curator net routerp)

        ; Initialize services
        controller  (helix/controller {:zookeeper zk
                                       :cluster cluster
                                       :instance {:host host :port port}})
        participant (helix/participant {:zookeeper zk
                                        :cluster cluster
                                        :instance {:host host :port port}
                                        :fsm fsm})
        router      (clj-helix.route/router! participant)
        _           (deliver routerp router)
        clock-sync  (clock-sync/service net router vnodes)
        aae         (aae/service net router vnodes)

        ; Construct node
        node {:host host
              :port port
              :net net
              :curator curator
              :router router
              :clock-sync clock-sync
              :aae aae
              :participant participant
              :controller controller
              :vnodes vnodes}]

    (net/add-handler! net (handler node))

    ; Start network node
    (net/start! net)

    node))

(defn controller
  "Creates a new controller, with the given options.
  
  :zookeeper    \"localhost:2181\"
  :cluster      :skuld
  :host         \"127.0.0.1\"
  :port         13000"
  [opts]
  (let [zk      (get opts :zookeeper "localhost:2181")
        host    (get opts :host "127.0.0.1")
        port    (get opts :port 13000)
        cluster (get opts :cluster :skuld)]
    {:host host
     :port port
     :zookeeper zk
     :controller (helix/controller {:zookeeper zk
                                    :cluster cluster
                                    :instance {:host host :port port}})}))

(defn shutdown!
  "Shuts down a node."
  [node]
  (when-let [c (:clock-sync node)] (clock-sync/shutdown! c))
  (when-let [aae (:aae node)]      (aae/shutdown! aae))
  (when-let [net (:net node)]      (net/shutdown! net))

  (->> (select-keys node [:participant :controller])
       vals
       (remove nil?)
       (map helix/disconnect!)
       dorun))
