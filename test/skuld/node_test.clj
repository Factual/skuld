(ns skuld.node-test
  (:use [clj-helix.logging :only [mute]]
        clojure.tools.logging
        clojure.test)
  (:require [skuld.admin :as admin]
            [skuld.node :as node]
            [skuld.flake :as flake]
            [skuld.net :as net]
            clj-helix.admin)
  (:import com.aphyr.skuld.Bytes))

(flake/init!)

(def admin
  (admin/admin {:partitions 2 :replicas 3}))

(def ^:dynamic *net* nil)
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

              ; Start net
              (fn [f]
                (binding [*net* (doto (net/node {:port 13100})
                                  (net/start!))]
                  (try
                    (f)
                    (finally
                      (net/shutdown! *net*)))))

              ; Start nodes
              (fn [f]
                (mute
                  (binding [*nodes* (start-nodes!)]
                    (try
                      (f)
                      (finally
                        (shutdown-nodes! *nodes*)))))))

(def byte-array-class ^:const (type (byte-array 0)))

(deftest enqueue-test
  ; Enqueue a task
  (let [id (-> *net*
               (net/sync-req! [{:host "127.0.0.1" :port 13000}]
                              {}
                              {:type :enqueue
                               :task {:payload "hi there"}})
               first
               :task
               :id)]
    (is (instance? byte-array-class id))

    ; Read it back
    (let [task (-> *net*
                   (net/sync-req! [(first *nodes*)]
                         {:r 3}
                         {:type :get-task
                          :id id})
                   first
                   :task)]
      (is (= task
             {:id (Bytes. id)
              :payload "hi there"})))))
