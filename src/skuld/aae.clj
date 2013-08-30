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
  (merkle/tree (vnode/tasks vnode)
               :id
               identity))

(defn merge-updates!
  "Merges :updates from a message into the given vnode. Returns true if
  messages merged, nil otherwise."
  [vnode message]
  (when message
    (dorun (map (partial vnode/merge-task! vnode) (:updates message)))
    true))

(defn vnode
  "Gets the local vnode for an AAE message."
  [vnodes msg]
  (let [part (:partition msg)]
    (or (get @vnodes part)
        (throw (RuntimeException. "no such vnode" part)))))

(defn handle-tree
  "Returns the local {:tree ...} for a requested vnode."
  [vnodes msg]
  (let [vnode (vnode vnodes msg)]
    {:tree (merkle/node->map (merkle-tree vnode))}))

(defn handle-diff
  "Given a message with a requester's tree, computes diff and returns {:updates
  ...} for requester."
  [vnodes msg]
  (let [vnode (vnode vnodes msg)
        ; Diff against our local collection.
        remote-tree (merkle/map->node (:tree msg))
        diffs (merkle/diff (vnode/tasks vnode)
                           (merkle-tree vnode)
                           remote-tree
                           :id)]
    {:updates (vals diffs)}))

(defn handle-updates!
  "Given {:updates ...} from requester, applies them to local vnode and returns
  {}."
  [vnodes msg]
  (or
    (and (merge-updates! (vnode vnodes msg) msg)
         {})
    (throw (RuntimeException. "expected a :updates key."))))

(defn handler
  "Returns a handler function that checks AAE messages against an atom wrapping
  a map of partitions to vnodes."
  [vnodes]
  (fn [msg]
    (case (:type msg)
      :aae-tree    (handle-tree     vnodes msg)
      :aae-diff    (handle-diff     vnodes msg)
      :aae-updates (handle-updates! vnodes msg)
      nil)))

(defn sync-from!
  "Fills in the local vnode with tasks from the remote peer. If the remote peer
  is immutable, this means the local node will have a complete copy of the
  remote's data. Returns true if vnode copied; false otherwise."
  [vnode peer]
  (let [tree (-> vnode
              merkle-tree)
        tree (merkle/node->map tree)
        [res] (net/sync-req! (:net vnode) [peer] {}
                            {:type :aae-diff
                             :partition (:partition vnode)
                             :tree tree})]
    (merge-updates! vnode res)))

(defn sync-to!
  "Pushes data from a local vnode to the remote peer. If the local vnode is
  immutable, this means the remote peer will have a complete copy of the
  vnode's data. Returns true if vnode copied; false otherwise."
  [vnode peer]
  ; Get remote tree
  (when-let [response (-> vnode
                          :net
                          (net/sync-req! 
                            [peer]
                            {}
                            {:type :aae-tree
                             :partition (:partition vnode)})
                          first)]
                             
    ; Compute diffs
    (let [remote-tree (merkle/map->node (:tree response))
          updates (merkle/diff (vnode/tasks vnode)
                               (merkle-tree vnode)
                               remote-tree
                               :id)]

      ; Send updates
      (when-not (:error (net/sync-req! (:net vnode)
                                       [peer]
                                       {}
                                       {:type :aae-updates
                                        :partition (:partition vnode)
                                        :updates updates}))
        true))))

(defn sync-vnode!
  "Given a vnode, hashes it and syncs with peers."
  [net router vnode]
  (let [self (vnode/net-id vnode)
        peers (set (vnode/peers vnode))]
    (dorun (map (partial sync-from! vnode) (disj peers self)))))

(defn initiator
  "Periodically initiates sync with peers."
  [net router vnodes]
  (let [running (promise)]
    (future
      (when (deref running 10000 true)
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
            (recur)))))
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
