(ns skuld.node
  "A single node in the Skuld cluster. Manages any number of vnodes."
  (:require [skuld.vnode :as vnode]
            [skuld.net :as net]
            [skuld.flake :as flake]
            [skuld.clock-sync :as clock-sync]
            [skuld.aae :as aae]
            [skuld.task :as task]
            [clj-helix.manager :as helix]
            clj-helix.admin
            clj-helix.fsm
            [clj-helix.route :as route])
  (:import (java.util Arrays)))

(defn tasks
  "Given an atom containing vnodes, emits all tasks for those vnodes."
  [vnodes]
  (mapcat vnode/tasks (vals @vnodes)))

(defn partition-name
  "Calculates which partition is responsible for a given ID."
  [num-partitions ^bytes id]
  (str "skuld_" (-> id
      Arrays/hashCode
      (mod num-partitions))))

(defn preflist
  "Returns a set of nodes responsible for a given ID."
  [router num-partitions ^bytes id]
  (assert (not (nil? id)))
  (route/instances router
                   :skuld
                   (partition-name num-partitions id)
                   :peer))

(defn num-partitions
  "The number of partitions in the cluster for a given participant."
  [participant]
  (-> participant
      helix/admin
      (clj-helix.admin/resource-ideal-state
        (helix/cluster-name participant)
        :skuld)
      .getNumPartitions))

(defn num-replicas
  "How many replicas are there for the Skuld resource?"
  [participant]
  (-> participant
      helix/admin
      (clj-helix.admin/resource-ideal-state
        (helix/cluster-name participant)
        :skuld)
      .getReplicas
      Integer.))

(defn majority
  "For N replicas, what would consititute a majority?"
  [n]
  (int (Math/floor (inc (/ n 2)))))

(defn enqueue
  "Proxies to enqueue-local on all nodes in the preflist for this task."
  [net router num-partitions n vnodes msg]
  (let [id (flake/id)
        task (assoc (:task msg) :id id)
        preflist (preflist router num-partitions id)]
;    (prn "Proxying enqueue to" (map :port preflist))
    (let [r (get msg :r 1)
          responses (net/sync-req! net preflist {:r r}
                                   {:type    :enqueue-local
                                    :task    task})
          acks (remove :error responses)]
      (if (<= r (count acks))
        {:n         (count acks)
         :task      task}
        {:n         (count acks)
         :error     (str "not enough acks")
         :responses responses}))))

(defn enqueue-local
  "Enqueues a message on the local vnode for this task."
  [num-partitions vnodes msg]
  (let [task (:task msg)
        part (partition-name num-partitions (:id task))]
    (if-let [vnode (get @vnodes part)]
      (do (vnode/enqueue vnode task)
          {:task-id (:id task)})
      {:error (str "I don't have partition" part "for task" (:id task))})))

(defn get-task
  "Gets the current state of a task."
  [net router num-partitions n vnodes msg]
  (let [id (:id msg)
        preflist (preflist router num-partitions id)
        r (get msg :r 1)
        responses (net/sync-req! net preflist {:r r}
                                 {:type :get-task-local
                                  :id   id})
        acks (remove :error responses)
        task (->> responses (map :task) (reduce task/merge-task))]
    (if (<= r (count acks))
      {:n    (count acks)
       :task task}
      {:n    (count acks)
       :task task
       :error "not enough acks"
       :responses responses})))

(defn get-task-local
  "Gets the current state of a task from a local vnode"
  [num-partitions vnodes msg]
  (let [id (:id msg)
        part (partition-name num-partitions id)]
    (if-let [vnode (get @vnodes part)]
      {:task (vnode/get-task vnode id)}
      {:error (str "I don't have partition" part "for task" id)})))

(defn handler
  "Returns a fn which handles messages for a node."
  [participant net router vnodes]
  (let [num-partitions (num-partitions participant)
        n              (num-replicas participant)]
    (fn handler [msg]
      (case (:type msg)
        :enqueue        (enqueue net router num-partitions n vnodes msg)
        :enqueue-local  (enqueue-local num-partitions vnodes msg)
        :get-task       (get-task net router num-partitions n vnodes msg)
        :get-task-local (get-task-local num-partitions vnodes msg)
        nil))))

(def fsm-def (clj-helix.fsm/fsm-definition
               {:name   :skuld
                :states {:DROPPED {:transitions :offline}
                         :offline {:initial? true
                                   :transitions [:peer :DROPPED]}
                         :peer {:priority 1
                                :upper-bound :R
                                :transitions :offline}}}))

(defn fsm
  "Compiles a new FSM to manage a vnodes map."
  [vnodes]
  (clj-helix.fsm/fsm
    fsm-def
    (:offline :peer [part m c]
              (swap! vnodes assoc part
                     (vnode/vnode {:partition part})))

    (:offline :DROPPED [part m c])

    (:peer :offline [part m c]
           (locking *out*
             (swap! vnodes dissoc part)))))

(defn node
  "Creates a new node with the given options.

  :zookeeper    \"localhost:2181\"
  :cluster      :skuld
  :host         \"127.0.0.1\"
  :port         13000"
  [opts]
  (let [zk      (get opts :zookeeper "localhost:2181")
        host    (get opts :host "127.0.0.1")
        port    (get opts :port 13000)
        cluster (get opts :cluster :skuld)
        vnodes  (atom {})
        fsm     (fsm vnodes)

;        _ (future
;            (loop []
;              (Thread/sleep 10000)
;              (prn :vnodes (keys @vnodes))
;              (prn (count (tasks vnodes)) "total tasks")
;              (recur)))

        controller  (helix/controller {:zookeeper zk
                                       :cluster cluster
                                       :instance {:host host :port port}})
        participant (helix/participant {:zookeeper zk
                                        :cluster cluster
                                        :instance {:host host :port port}
                                        :fsm fsm})
        router (clj-helix.route/router! participant)
        net (net/node {:host host
                       :port port})
        clock-sync (clock-sync/service net router vnodes)
        aae        (aae/service net router vnodes)]

    (net/add-handler! net (handler participant net router vnodes))

    ; Start network node
    (net/start! net)

    {:host host
     :port port
     :net net
     :router router
     :clock-sync clock-sync
     :aae aae
     :participant participant
     :controller controller
     :vnodes vnodes}))

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
