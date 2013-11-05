(ns skuld.node-test
  (:use clojure.tools.logging
        clojure.test
        skuld.zk-test
        skuld.util
        skuld.node)
  (:require [skuld.client    :as client]
            [skuld.admin     :as admin]
            [skuld.vnode     :as vnode]
            [skuld.curator   :as curator]
            [skuld.net       :as net]
            [skuld.task      :as task]
            [skuld.aae       :as aae]
            [skuld.politics  :as politics]
            [skuld.logging   :as logging]
            [clojure.set     :as set]
            [clj-http.client :as http]
            skuld.http
            skuld.flake-test
            clj-helix.admin)
  (:import com.aphyr.skuld.Bytes))

(defn admin
  [zk]
  (logging/with-level :warn ["org.apache.zookeeper" "org.apache.helix" "org.I0Itec.zkclient"]
    (admin/admin {:partitions 2
                  :replicas 3
                  :zookeeper zk})))

(def ^:dynamic *client* nil)
(def ^:dynamic *nodes* nil)
(def ^:dynamic *zk* nil)

(defn ensure-cluster!
  "Ensures a test cluster exists."
  [admin]
  (when-not (some #{"skuld"} (clj-helix.admin/clusters (:helix admin)))
    (admin/destroy-cluster! admin)
    (admin/create-cluster! admin)
    (dotimes [i 7]
      (admin/add-node! admin {:host "127.0.0.1" :port (+ 13000 i)}))))

(defn start-nodes!
  "Returns a vector of a bunch of started nodes."
  [zk]
  (let [nodes (->> (range 5)
                   (pmap #(wait-for-peers (node {:port (+ 13000 %)
                                                 :zookeeper zk})))
                   doall)
        vnodes (->> nodes
                    (map vnodes)
                    (mapcat vals))]
    (->> vnodes
         (pmap #(curator/reset!! (vnode/zk-leader %) {:epoch 0
                                                      :cohort #{}}))
         dorun)
    nodes))

(defn shutdown-nodes!
  "Shutdown a seq of nodes."
  [nodes]
  (->> nodes (pmap shutdown!) doall))

(defn partition-available?
  "Given a set of vnodes for a partition, do they comprise an available
  cohort?"
  [vnodes]
  (when-let [majority-epoch (majority-value (map vnode/epoch vnodes))]
    (some #(and (= majority-epoch (vnode/epoch %))
                (vnode/leader? %))
          vnodes)))

(defn elect!
  "Force election of a leader in all vnodes."
  [nodes]
  (loop [unelected (->> nodes
                        (map vnodes)
                        (mapcat vals)
                        (filter vnode/active?)
                        (group-by :partition)
                        vals
                        (remove partition-available?))]
      (when-not (empty? unelected)
        (locking *out*
          (debug (count unelected) "unelected partitions"))
;          (debug (map (partial map (juxt (comp :port vnode/net-id)
;                                       :partition
;                                       vnode/state))
;                    unelected)))
        (doseq [vnodes unelected]
          (with-redefs [vnode/election-timeout 0]
            (vnode/elect! (rand-nth vnodes))))
        (Thread/sleep 100)
        (recur (remove partition-available? unelected)))))

(defn once
  [f]
  (with-zk [zk]
    ; Set up cluster
    (let [admin (admin zk)]
      (try
        (logging/mute (ensure-cluster! admin))
        (admin/shutdown! admin)))

    ; Set up nodes
    (logging/suppress [#"^org\.apache\.helix"
                       #"^org\.apache\.zookeeper"
                       #"^org\.apache\.curator"
                       #"^org\.I0Itec\.zkclient"
                       "skuld.vnode"]
                      (binding [*zk*    zk
                                *nodes* (start-nodes! zk)]
                        (try
                          (binding [*client* (client/client *nodes*)]
                            (try
                              (f)
                              (finally
                                (client/shutdown! *client*))))
                          (finally
                            (shutdown-nodes! *nodes*)))))))

(defn each
  [f]
  ; If any nodes were killed by the test, re-initialize the cluster before
  ; proceeding.
  (if (not-any? shutdown? *nodes*)
    (do
      (client/wipe! *client*)
      (f))
    (do
      (info :repairing-cluster)
      (shutdown-nodes! *nodes*)
      (binding [*nodes* (start-nodes! *zk*)]
        (try
          (client/wipe! *client*)
          (f)
          (finally
            (shutdown-nodes! *nodes*)))))))

(use-fixtures :once once)
(use-fixtures :each each)

; (def byte-array-class ^:const (type (byte-array 0)))

(defn log-cohorts
  []
  (println "cohorts are\n" (->> *nodes*
                                (map vnodes)
                                (mapcat vals)
                                (filter vnode/leader?)
                                (map (juxt (comp :port vnode/net-id)
                                           :partition
                                           vnode/epoch
                                           (comp (partial map :port)
                                                 :cohort vnode/state)))
                                (map pr-str)
                                (interpose "\n")
                                (apply str))))

(defn log-counts
  []
  (->> *nodes*
       (mapcat (fn [node]
                 (->> node
                      vnodes
                      (map (fn [[part vnode]]
                             [(:port (net/id (:net node)))
                              part
                              (vnode/leader? vnode)
                              (vnode/count-tasks vnode)])))))
       (clojure.pprint/pprint)))

(deftest enqueue-test
  ; Enqueue a task
  (let [id (client/enqueue! *client* {:data "hi there"})]
    (is id)
    (is (instance? Bytes id))

    ; Read it back
    (is (= (client/get-task *client* {:r 3} id)
           {:id id
            :claims []
            :data "hi there"}))))

(deftest count-test
  ; We need leaders for queue counts to work.
  (elect! *nodes*)

  ; Enqueue a few tasks
  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:w 3} {:data "sup"}))

    (is (= n (client/count-tasks *client*)))

    (is (= n (client/count-queue *client*)))))

(deftest count-http-test
  (elect! *nodes*)

  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:w 3} {:data "sup"}))

    ; TODO: real tests
    (let [resp (http/get "http://127.0.0.1:13100/count_tasks")
          content-type (get-in resp [:headers "content-type"])]
      (is (= 200 (:status resp)))
      (is (= "application/json;charset=utf-8" content-type)))
    (let [resp (http/post "http://127.0.0.1:13100/count_tasks"
                          {:throw-exceptions false})]
      (is (= 405 (:status resp))))

    (let [resp (http/get "http://127.0.0.1:13100/count_queue")
          content-type (get-in resp [:headers "content-type"])]
      (is (= 200 (:status resp)))
      (is (= "application/json;charset=utf-8" content-type)))
    (let [resp (http/post "http://127.0.0.1:13100/count_queue"
                          {:throw-exceptions false})]
      (is (= 405 (:status resp))))))

(deftest list-tasks-test
  ; Enqueue
  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:w 3} {:data "sup"}))
   
    ; List
    (let [tasks (client/list-tasks *client*)]
      (is (= n (count tasks)))
      (is (= (sort (map :id tasks)) (map :id tasks)))
      (is (every? :data tasks)))))

(deftest list-tasks-http-test
  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:w 3} {:data "sup"}))

    ; TODO: real tests
    (let [resp (http/get "http://127.0.0.1:13100/list_tasks")
          content-type (get-in resp [:headers "content-type"])]
      (is (= 200 (:status resp)))
      (is (= "application/json;charset=utf-8" content-type)))
    (let [resp (http/post "http://127.0.0.1:13100/list_tasks"
                          {:throw-exceptions false})]
      (is (= 405 (:status resp))))))
