(ns skuld.vnode
  "A state machine which manages an instance of a partition on this node."
  (:use skuld.util
        clojure.tools.logging)
  (:require [skuld.task :as task]
            [skuld.net  :as net]
            [clj-helix.route :as route])
  (:import com.aphyr.skuld.Bytes))

(defn vnode
  "Create a new vnode. Options:
  
  :partition
  :state"
  [opts]
  {:partition (get opts :partition)
   :net       (:net opts)
   :router    (:router opts)
   :state     (atom {:type :follower
                     :leader nil
                     :epoch 0})
   :tasks     (atom (sorted-map))
   :claims    (atom (sorted-map))})

(defn peers
  "Peers for this vnode."
  [vnode]
  (map #(select-keys % [:host :port])
    (route/instances (:router vnode) :skuld (:partition vnode) :peer)))

(defn elect!
  "Attempt to become a primary. We do this by incrementing our local epoch,
  broadcasting a request for votes to all peers, and waiting for a majority of
  responses. If we establish a majority, then we're guaranteed that this node
  is the leader for a particular epoch and set of nodes."
  [vnode]
  ; First, compute the set of peers that will comprise the next epoch.
  (let [nodes (peers vnode)
        self (net/id (:net vnode))
        others (remove #{self} nodes)
        
        ; Increment the epoch and node set.
        epoch (-> vnode
                  :state
                  (swap! (fn [state]
                           (merge state {:epoch (inc (:epoch state))
                                         :leader self
                                         :type :candidate
                                         :nodes nodes})))
                  :epoch)
        
        ; How many nodes do we need for a majority? (remember, we already voted
        ; for ourself)
        maj   (majority (count nodes))

        ; Broadcast election message
        responses (net/sync-req! (:net vnode)
                                 others
                                 {:r (dec maj)}
                                 {:type :request-vote
                                  :partition (:partition vnode)
                                  :leader (net/id (:net vnode))
                                  :nodes nodes
                                  :epoch epoch})
        
        ; Count up positive responses
        votes (inc (count (filter :vote responses)))]

    (if (< votes maj)
      ; Did *not* receive sufficient votes.
      (do)
      (let [state (swap! (:state vnode)
                         (fn [state]
                           (if (= epoch (:epoch state))
                             ; Still waiting for responses
                             (assoc state :type :leader)
                             ; We voted for someone else in the meantime
                             state)))]))))

(defn request-vote!
  "Accepts a new-leader message from a peer, and returns a vote for the
  node--or an error if our epoch is current or newer. Returns a response
  message."
  [vnode msg]
  (let [{:keys [nodes epoch leader]} msg
    ; We set :vote to true only if we're voting for this node; otherwise it's
    ; false. Then we can just return our state to our peer.
    state (swap! (:state vnode) (fn [state]
                            (if (< (:epoch state) epoch)
                              ; The leader is ahead of us.
                              (merge state
                                     {:epoch epoch
                                      :nodes nodes
                                      :leader leader
                                      :type :follower
                                      :vote  true})
                              (assoc state :vote false))))]
    state))

(defn state
  "The current state of a vnode."
  [vnode]
  @(:state vnode))

(defn leader?
  "Is this vnode a leader?"
  [vnode]
  (= :leader (:type (state vnode))))

(defn follower?
  "Is this vnode a follower?"
  [vnode]
  (= :follower (:type (state vnode))))

(defn epoch
  "The current epoch for this vnode."
  [vnode]
  (:epoch (state vnode)))

(defn leader
  "What's the current leader for this vnode?"
  [vnode]
  (:leader (state vnode)))

(defn enqueue!
  "Enqueues a new task into this vnode."
  [vnode task]
  (let [id (:id task)]
    (swap! (:tasks vnode) assoc id (assoc task :id id))))

(defn merge-task!
  "Takes a task and merges it into this vnode."
  [vnode task]
  (swap! (:tasks vnode) update-in [(:id task)] task/merge task))

(defn get-task
  "Returns a specific task by ID."
  [vnode id]
  (-> vnode :tasks deref (get id)))

(defn ids
  "All task IDs in this vnode."
  [vnode]
  (keys @(:tasks vnode)))

(defn count-tasks
  "How many tasks are in this vnode?"
  [vnode]
  (count @(:tasks vnode)))

(defn tasks
  "All tasks in this vnode."
  [vnode]
  (vals @(:tasks vnode)))

(defn wipe!
  "Wipe a vnode's data clean."
  [vnode]
  (reset! (:tasks vnode) (sorted-map))
  (reset! (:claims vnode) (sorted-map))
  vnode)
