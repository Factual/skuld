(ns skuld.admin
  "Administrative cluster-management tasks."
  (:require [clj-helix.admin :as helix]
            [skuld.node :as node]))

(defn admin
  "Create an administrator tool. Options:

  :cluster    the name of the skuld cluster (default \"skuld\")
  :zookeeper  a zookeeper connect string (\"localhost:2181\")
  :partitions number of partitions to shard into (1)
  :replicas   number of replicas to maintain (3)
  :max-partitions-per-node (:partitions)"
  [opts]
  {:helix (helix/helix-admin (get opts :zookeeper "localhost:2181"))
   :cluster (get opts :cluster "skuld")
   :partitions (get opts :partitions 1)
   :replicas   (get opts :replicas 3)
   :max-partitions-per-node (get opts :max-partitions-per-node
                                 (get opts :partitions 1))})

(defn shutdown!
  "Shuts down an admin tool."
  [admin]
  (.close (:helix admin)))

(defn destroy-cluster!
  [admin]
  (helix/drop-cluster (:helix admin)
                      (:cluster admin)))

(defn create-cluster!
  [admin]
  ; Create cluster itself
  (helix/add-cluster (:helix admin)
                     (:cluster admin))
  
  ; Set FSM
  (helix/add-fsm-definition (:helix admin)
                            (:cluster admin)
                            node/fsm-def)

  ; Create resource
  (helix/add-resource (:helix admin)
                      (:cluster admin)
                      {:resource   :skuld
                       :partitions (:partitions admin)
                       :max-partitions-per-node (:max-partitions-per-node admin)
                       :replicas   (:replicas admin)
                       :state-model (:name node/fsm-def)}))

(defn add-node!
  "Adds an node to the cluster."
  [admin node]
  (helix/add-instance (:helix admin) (:cluster admin)
                      (select-keys node [:host :port])))
