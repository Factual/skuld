(ns skuld.node-test
  (:use [clj-helix.logging :only [mute]]
        clojure.tools.logging
        clojure.test)
  (:require [skuld.admin :as admin]
            [skuld.node :as node]
            [skuld.flake :as flake]
            [skuld.net :as net]
            clj-helix.admin))

(flake/init!)

(def admin
  (admin/admin {:partitions 2 :replicas 3}))

(defn ensure-cluster!
  "Ensures a test cluster exists."
  []
  (when-not (some #{"skuld"} (clj-helix.admin/clusters (:helix admin)))
    (admin/destroy-cluster! admin)
    (admin/create-cluster! admin)
    (dotimes [i 7]
      (admin/add-node! admin {:host "127.0.0.1" :port (+ 13000 i)}))))

(def nodes (atom []))

(defn start-nodes!
  "Spin up a few nodes and binds them to the nodes atom."
  []
  (prn "Starting nodes.")
  (->> (range 5)
       (pmap #(node/node {:port (+ 13000 %)}))
       doall
       (reset! nodes))
  (prn "Nodes started."))

(defn shutdown-nodes!
  "Shutdown nodes in the nodes atom."
  []
  (prn "Shutting down nodes.")
  (->> nodes deref (pmap node/shutdown!) doall)
  (reset! nodes [])
  (prn "Nodes shutdown."))

(use-fixtures :once (fn [f] (mute (ensure-cluster!) (f))))
(use-fixtures :each (fn [f]
                      (mute
                        (try
                          (start-nodes!)
                          (f)
                          (finally
                            (shutdown-nodes!))))))

(deftest enqueue-test)
