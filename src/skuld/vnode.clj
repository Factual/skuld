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
  "Attempt to become a primary. We need to ensure that:

  1. Leaders are logically sequential
  2. Each leader's claim set is a superset of the previous leader
  
  We have: a target cohort of nodes for the new epoch, provided by helix.
  Some previous cohort of nodes belonging to the old epoch, tracked by ZK.

  To become a leader, one must successfully:

  1. Read the previous epoch+cohort from ZK
  
  2. (optimization) Ensure that the previous epoch is strictly less than the
  epoch this node is going to be the leader for.
  
  3. Broadcast a claim message to the new cohort, union the old cohort

  4. Receive votes from a majority of the nodes in the old cohort

  5. Receive votes from a majority of the nodes in the new cohort

    - At this juncture, neither the new nor the old cohort can commit claims
  for our target epoch or lower, making the set of claims made in older epochs
  immutable. If we are beat by another node using a newer epoch, it will have
  recovered a superset of those claims; we'll fail to CAS in step 8, and no
  claims will be lost.

  6. Obtain all claims from a majority of the old cohort, and union them in to
  our local claim set.

    - This ensures that we are aware of all claims made prior to our epoch.

  7. Broadcast our local claim set to a majority of the new cohort.

    - This ensures that any *future* epoch will correctly recover our claim
      set. 6 + 7 provide a continuous history of claims, by induction.

  8. CAS our new epoch and cohort into zookeeper, ensuring that nobody else
     beat us to it.

  If any stage fails, delay randomly and retry.

  A note on zombies:

  Zombies are nodes which are ready to give up ownership of a vnode but cannot,
  because they may still be required to hand off their claim set. After step 8,
  we inform all zombies which are not a part of our new cohort that it is safe
  to drop their claim set."
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
