(ns skuld.aae
  "Active-anti-entropy service. This watches over local partitions and
  continually synchronizes their data with their peers."
  (:use clojure.tools.logging)
  (:require [skuld.vnode :as vnode]
            [skuld.net :as net]
            [merkle.kv.linear :as merkle]
            [clj-helix.route :as route]))

(defn merkle-tree
  "Computes a merkle-tree of a vnode."
  [vnode]
  (merkle/tree @(:tasks vnode)))

(defn handler
  "Returns a handler function that checks AAE messages against an atom wrapping
  a map of partitions to vnodes."
  [vnodes]
  (fn [msg]
    (when (= :aae (:type msg))
      (let [part (:part msg)]
        (when-let [vnode (get @vnodes part)]
          ; Diff against our local collection.
          (let [remote-tree (merkle/map->node (:tree msg))]
;            (prn "Remote tree is" remote-tree)
;            (prn "Local  tree is" (merkle-tree vnode))
;            (prn "Identical regions are" (merkle/identical-ranges (merkle-tree vnode) remote-tree))
;            (prn "Local tasks are" (seq @(:tasks vnode)))
            (let [diffs (merkle/diff @(:tasks vnode)
                                     (merkle-tree vnode)
                                     remote-tree)]
              (prn (count diffs) "diffs with peer")
              {:updates (vals diffs)})))))))

(defn sync-vnode!
  "Given a vnode, hashes it and initiates a sync with peers."
  [net router vnode]
  ; Compute tree
  (prn "Initiating AAE sync of" (:partition vnode)) 
  (let [t (-> vnode
              merkle-tree
              merkle/node->map)
        self (select-keys net [:host :port])
        peers (->> (route/instances router :skuld (:partition vnode) :peer)
                   (remove #(= self (select-keys % [:host :port]))))]

    ; Broadcast tree to peers
    (doseq [peer peers]
      (prn "Broadcasting initial merkle tree to" peer)
      (net/req! net [peer] {}
                {:type :aae
                 :part (:partition vnode)
                 :tree t}
                [[response]]
                ; And if we get responses, merge em.
                (when-let [updates (:updates response)]
                  (prn "Merging" (count updates) "AAE updates")
                  (dorun (map (partial vnode/merge-task! vnode) updates)))))))

(defn initiator
  "Periodically initiates sync with peers."
  [net router vnodes]
  (let [running (promise)]
    (future
      (Thread/sleep 10000)
      (loop []
        (try
          (->> vnodes
               deref
               vals
               (map (partial sync-vnode! net router))
               dorun)
          (catch Throwable t
            (warn t "aae caught")))

        (when (deref running 10000 true)
          (recur))))
    running))

(defn service
  "Creates a new AAE service."
  [net router vnodes]
  ; Register handler
  (net/add-handler! net (handler vnodes))
  (initiator net router vnodes))

(defn shutdown!
  "Stops an AAE service."
  [aae]
  (deliver aae false))
