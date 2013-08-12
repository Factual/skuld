(ns skuld.node-test
  (:use [clj-helix.logging :only [mute]]
        clojure.tools.logging
        clojure.test)
  (:require [skuld.client :as client]
            [skuld.admin :as admin]
            [skuld.node :as node]
            [skuld.flake :as flake]
            [skuld.net :as net]
            clj-helix.admin)
  (:import com.aphyr.skuld.Bytes))

(flake/init!)

(def admin
  (admin/admin {:partitions 2 :replicas 3}))

(def ^:dynamic *client* nil)
(def ^:dynamic *nodes* nil)

(defn ensure-cluster!
  "Ensures a test cluster exists."
  []
  (when-not (some #{"skuld"} (clj-helix.admin/clusters (:helix admin)))
    (admin/destroy-cluster! admin)
    (admin/create-cluster! admin)
    (dotimes [i 7]
      (admin/add-node! admin {:host "127.0.0.1" :port (+ 13000 i)}))))

(defn start-nodes!
  "Returns a vector of a bunch of started nodes."
  []
  (->> (range 5)
       (pmap #(node/node {:port (+ 13000 %)}))
       doall))

(defn shutdown-nodes!
  "Shutdown a seq of nodes."
  [nodes]
  (->> nodes (pmap node/shutdown!) doall))

(use-fixtures :once
              ; Start cluster
              (fn [f] (mute (ensure-cluster!) (f)))

              ; Start nodes
              (fn [f]
                (mute
                  (binding [*nodes* (start-nodes!)]
                    (try
                      (f)
                      (finally
                        (shutdown-nodes! *nodes*))))))

              ; Start client
              (fn [f]
                (binding [*client* (client/client *nodes*)]
                  (try
                    (f)
                    (finally
                      (client/shutdown! *client*))))))

(use-fixtures :each
              ; Wipe cluster
              (fn [f]
                (client/wipe! *client*)
                (f)))

; (def byte-array-class ^:const (type (byte-array 0)))

(deftest enqueue-test
  ; Enqueue a task
  (let [id (client/enqueue! *client* {:payload "hi there"})]
    (is id)
    (is (instance? Bytes id))

    ; Read it back
    (is (= (client/get-task *client* id)
           {:id id
            :logs nil
            :payload "hi there"}))))

(deftest count-test
  ; Enqueue a few tasks
  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:payload "sup"}))

    (is (= n (client/count-tasks *client*)))))

(deftest list-tasks-test
  ; Enqueue
  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:payload "sup"}))
    
    ; List
    (let [tasks (client/list-tasks *client*)]
      (is (= n (count tasks)))
      (is (= (sort (map :id tasks)) (map :id tasks)))
      (is (some :payload tasks)))))
