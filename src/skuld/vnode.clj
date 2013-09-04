(ns skuld.vnode
  "A state machine which manages an instance of a partition on this node."
  (:use skuld.util
        clojure.tools.logging)
  (:require [skuld.task       :as task]
            [skuld.db         :as db]
            [skuld.db.level   :as level]
            [skuld.net        :as net]
            [skuld.flake      :as flake]
            [skuld.curator    :as curator]
            [clj-helix.route  :as route]
            [clojure.set      :as set])
  (:import com.aphyr.skuld.Bytes))

; DEAL WITH IT
(in-ns 'skuld.aae)
(clojure.core/declare sync-from!)
(clojure.core/declare sync-to!)
(in-ns 'skuld.vnode)

(declare wipe!)

(defn vnode
  "Create a new vnode. Options:
  
  :partition
  :state"
  [opts]
  {:partition (:partition opts)
   :net       (:net opts)
   :router    (:router opts)
   :db        (level/open {:partition (:partition opts)
                           :host      (:host (:net opts))
                           :port      (:port (:net opts))})
   :last-leader-msg-time (atom Long/MIN_VALUE)
   :zk-leader (delay
                (curator/distributed-atom (:curator opts)
                                          (str "/" (:partition opts)
                                               "/leader")
                                          {:epoch 0
                                           :cohort #{}}))
   :state     (atom {:type :follower
                     :leader nil
                     :epoch 0})})

;; Leaders

(defmacro llog
  "Log leader election messages"
  [& args]
  `(locking *out*
    (info ~@args)))

(defn peers
  "Peers for this vnode."
  [vnode]
  (doall
    (map #(select-keys % [:host :port])
      (route/instances (:router vnode) :skuld (:partition vnode) :peer))))

(defn zk-leader
  "A curator distributed atom backed by Zookeeper, containing the current epoch
  and cohort."
  [vnode]
  @(:zk-leader vnode))

(defn net-id
  "The net id of the node hosting this vnode."
  [vnode]
  (net/id (:net vnode)))

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

(defn candidate?
  "Is this vnode a candidate?"
  [vnode]
  (= :candidate (:type (state vnode))))

(defn active?
  "Is this vnode a leader or peer?"
  [vnode]
  (-> vnode
      state
      :type
      #{:leader :follower :candidate}))

(defn zombie?
  "Is this vnode a zombie?"
  [vnode]
  (= :zombie (:type (state vnode))))

(defn dead?
  "Is this vnode dead?"
  [vnode]
  (= :dead (:type (state vnode))))

(defn epoch
  "The current epoch for this vnode."
  [vnode]
  (:epoch (state vnode)))

(defn leader
  "What's the current leader for this vnode?"
  [vnode]
  (:leader (state vnode)))

(def election-timeout
  "How long do we have to wait before initiating an election, in ms?"
  10000)

(defn suppress-election!
  [vnode msg]
  (when (= (:epoch msg) (epoch vnode))
    (swap! (:last-leader-msg-time vnode) max (flake/linear-time))))

(defn accept-newer-epoch!
  "Any node with newer epoch information than us can update us to that epoch
  and convert us to a follower. Returns state if epoch updated, nil otherwise."
  [vnode msg]
  (when-let [leader-epoch (:epoch msg)]
    (when (< (epoch vnode) leader-epoch)
      (let [state (-> vnode
                      :state
                      (swap! (fn [state]
                               (if (< (:epoch state) leader-epoch)
                                 {:type (if (= :zombie (:type state))
                                          :zombie
                                          :follower)
                                  :epoch leader-epoch
                                  :cohort (:cohort msg)
                                  :leader (:leader msg)
                                  :updated true}
                                 (assoc state dissoc :updated)))))]
        (when (:updated state)
          (suppress-election! vnode msg)
          (llog (net-id vnode) (:partition vnode)
                "assuming epoch" leader-epoch)
          state)))))

(defn request-vote!
  "Accepts a new-leader message from a peer, and returns a vote for the
  node--or an error if our epoch is current or newer. Returns a response
  message."
  [vnode msg]
  (if-let [state (accept-newer-epoch! vnode msg)]
    (assoc state :vote (net-id vnode))
    (state vnode)))

(defn demote!
  "Forces a leader to step down."
  [vnode]
  (locking vnode
    (info (net-id vnode) (:partition vnode) "demoted")
    (swap! (:state vnode) (fn [state]
                           (if (= :leader (:type state))
                             (assoc state :type :follower)
                             state)))))

(defn zombie!
  "Places the vnode into an immutable zombie mode, where it waits to hand off
  its data to a leader."
  [vnode]
  (locking vnode
    (info (net-id vnode) (:partition vnode) "now zombie")
    (swap! (:state vnode) assoc :type :zombie)))

(defn revive!
  "Converts dead or zombie vnodes into followers."
  [vnode]
  (locking vnode
    (info (net-id vnode) (:partition vnode) "revived!")
    (swap! (:state vnode) (fn [state]
                            (if (#{:zombie :dead} (:type state))
                              (assoc state :type :follower)
                              state)))))

(defn shutdown!
  "Converts a zombie to state :dead. Destroys all data on the vnode."
  [vnode]
  (locking vnode
    (when-not (= :dead @(:state vnode))
      (reset! (:state vnode) :dead)
      (wipe! vnode)
      (db/close! (:db vnode)))))

(defn majority-excluding-self
  "Given a vnode, and a set of nodes, how many responses from *other* nodes
  (i.e. assuming we vote yes for ourself) are required to comprise a majority
  of the set?"
  [vnode cohort]
  ((if (cohort (net-id vnode)) dec identity)
   (majority (count cohort))))

(defn sufficient-votes?
  "Given a vnode, old cohort, and new cohort, does the given collection of
  request-vote responses allow us to become a leader?"
  [vnode old-cohort new-cohort votes]
  (let [votes (set (keep :vote votes))]
    (and (<= (majority-excluding-self vnode old-cohort)
             (count (set/intersection old-cohort votes)))
         (<= (majority-excluding-self vnode new-cohort)
             (count (set/intersection new-cohort votes))))))

(defn sync-with-majority!
  "Tries to copy tasks to or from a majority of the given cohort, using
  sync-fn. Returns true if successful."
  [vnode cohort sync-fn]
  (not (pos? (loop [remaining (majority-excluding-self vnode cohort)
                    [node & more] (seq (disj cohort (net-id vnode)))]
               (if-not (and node (< 0 remaining))
                 ; Done
                 remaining
                 ; Copy data from another node and repeat
                 (recur (if (try (sync-fn vnode node)
                                 (catch RuntimeException e
                                   (locking *out*
                                     (error e "while synchronizing"
                                         (:partition vnode) "with" node))
                                   false))
                          (dec remaining)
                          remaining)
                        more))))))

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
  (llog (net-id vnode) (:partition vnode) "initiating election")
  (locking vnode
    (when (active? vnode)
      (llog :active)

      (when (< (+ @(:last-leader-msg-time vnode) election-timeout)
               (flake/linear-time))
        (llog "leader is outdated")

        ; First, compute the set of peers that will comprise the next epoch.
        (let [self       (net-id vnode)
              new-cohort (set (peers vnode))

              ; Increment the epoch and update the node set.
              epoch (-> vnode
                        :state
                        (swap! (fn [state]
                                 (merge state {:epoch (inc (:epoch state))
                                               :leader self
                                               :type :candidate
                                               :cohort new-cohort})))
                        :epoch)

              ; Check ZK's last leader information
              old (deref (zk-leader vnode))]
          (if (<= epoch (:epoch old))
            ; We're outdated; fast-forward to the new epoch.
            (do
              (llog "Outdated epoch relative to ZK; aborting election")
              (swap! (:state vnode) (fn [state]
                                      (if (<= (:epoch state) (:epoch old))
                                        (merge state {:epoch (:epoch old)
                                                      :leader false
                                                      :type :follower
                                                      :cohort (:cohort old)})
                                        state))))

            ; Issue requests to all nodes in old and new cohorts
            (let [old-cohort  (set (:cohort old))
                  responses   (atom (list))
                  accepted?   (promise)
                  peers       (disj (set/union new-cohort old-cohort) self)]
              (llog :old old-cohort)
              (llog :new new-cohort)
              (doseq [node peers]
                (net/req! (:net vnode) (list node) {:r 1}
                          {:type :request-vote
                           :partition (:partition vnode)
                           :leader self
                           :cohort new-cohort
                           :epoch epoch}
                          [[r]]
                          (let [rs (swap! responses conj r)]
                            (if (accept-newer-epoch! vnode r)
                              ; Cancel request; we saw a newer epoch from a peer.
                              (do
                                (deliver accepted? false)
                                (llog (net-id vnode) (:partition vnode)
                                      "aborting candidacy due to newer epoch"))
                              ; Have we enough votes?
                              (if (sufficient-votes? vnode old-cohort new-cohort rs)
                                (do
                                  (deliver accepted? true)
                                  (llog "Received enough votes:" rs))
                                (when (<= (count peers) (count rs))
                                  (deliver accepted? false)
                                  (llog "All votes in; giving up.")))))))

              ; Await responses
              (if-not (deref accepted? 5000 false)
                (llog (net-id vnode) (:partition vnode)
                      "election failed; not enough votes")

                ; Sync from old cohort.
                (if-not (sync-with-majority! vnode
                                             old-cohort
                                             skuld.aae/sync-from!)
                  (llog "Wasn't able to replicate from enough of old cohort; cannot become leader.")

                  ; Sync to new cohort.
                  (if-not (sync-with-majority! vnode
                                               new-cohort
                                               skuld.aae/sync-to!)
                    (error "Wasn't able to replicate to enough of new cohort; cannot become leader.")

                    ; Update ZK with new cohort and epoch--but only if nobody else
                    ; got there first.
                    (let [new-leader {:epoch epoch
                                      :cohort new-cohort}
                          set-leader (curator/swap!! (zk-leader vnode)
                                                     (fn [current]
                                                       (if (= old current)
                                                         new-leader
                                                         current)))]
                      (if (not= new-leader set-leader)
                        (llog (net-id vnode) (:partition vnode)
                              "election failed: another leader updated zk")

                        ; Success!
                        (let [state (swap! (:state vnode)
                                           (fn [state]
                                             (if (= epoch (:epoch state))
                                               ; Still waiting for responses
                                               (assoc state :type :leader)
                                               ; We voted for someone else in the
                                               ; meantime
                                               state)))]
                          (llog (net-id vnode) (:partition vnode)
                                "election successful: cohort now" epoch new-cohort))))))))))))))

;; Tasks

(defn merge-task!
  "Takes a task and merges it into this vnode."
  [vnode task]
  (db/merge-task! (:db vnode) task))

(defn get-task
  "Returns a specific task by ID."
  [vnode id]
  (db/get-task (:db vnode) id))

(defn ids
  "All task IDs in this vnode."
  [vnode]
  (db/ids (:db vnode)))

(defn count-tasks
  "How many tasks are in this vnode?"
  [vnode]
  (db/count-tasks (:db vnode)))

(defn tasks
  "All tasks in this vnode."
  [vnode]
  (db/tasks (:db vnode)))

(defn claimed
  "A subset of tasks which are claimed."
  [vnode]
  (->> vnode
       tasks
       (filter task/claimed?)))

(defn unclaimed
  "A subset of tasks which are eligible for claim."
  [vnode]
  (->> vnode
       tasks
       (remove task/completed?)
       (remove task/claimed?)))

(defn request-claim!
  "Applies a claim to a given task. Takes a message from a leader like
  
  {:epoch  The leader's epoch
   :id     The task ID
   :i      The index of the claim
   :claim  A claim map}

  ... and applies the given claim to our copy of that task. Returns an empty
  map if the claim is successful, or {:error ...} if the claim failed."
  [vnode {:keys [id i claim] :as msg}]
  (accept-newer-epoch! vnode msg)
  (suppress-election! vnode msg)
  (try
    (locking vnode
      (assert (not (zombie? vnode)))
      (if (= (:epoch msg) (epoch vnode))
        (do
          (db/claim-task! (:db vnode) id i claim)
          {})
        {:error (str "leader epoch " epoch
                     " does not match local epoch " (epoch vnode))}))
    (catch IllegalStateException e
      {:error (.getMessage e)})))

(defn claim!
  "Picks a task from this vnode and claims it for dt milliseconds. Returns the
  claimed task."
  [vnode dt]
  (let [state     (state vnode)
        cur-epoch (:epoch state)
        cohort    (:cohort state)
        ; How many followers need to ack us?
        maj       (-> cohort
                      (disj (net-id vnode))
                      count
                      majority
                      dec)]

    (when-not (= :leader (:type state))
      (throw (IllegalStateException. "can't initiate claim: not a leader.")))

    ; Attempt to claim a task locally.
    (when-let [task (db/claim-task! (:db vnode) dt)]
      (let [; Get claim details
            i     (dec (count (:claims task)))
            claim (nth (:claims task) i)

            ; Try to replicate claim remotely
            responses (net/sync-req! (:net vnode)
                                     (disj cohort (net-id vnode))
                                     {:r maj}
                                     {:type   :request-claim
                                      :epoch  cur-epoch
                                      :id     (:id task)
                                      :i      i
                                      :claim  claim})
            successes (count (remove :error responses))]
      
        ; Check that we're still in the same epoch; a leader could
        ; have subsumed us.
        (when (not= cur-epoch (epoch vnode))
          (throw (RuntimeException. (str "epoch changed from "
                                         cur-epoch
                                         " to "
                                         (epoch vnode)
                                         ", claim coordinator aborting"))))
    
    (if (<= maj successes)
      task
      (throw (RuntimeException. (str "needed " maj
                                     " acks from followers, only received "
                                     successes))))))))

(defn complete!
  "Completes the given task in the specified claim. Msg should contain:
  
  :task-id  The task identifier
  :claim-id The claim index
  :time     The time the task was completed at, in linear time."
  [vnode msg]
  (merge-task! vnode
               (-> vnode
                   (get-task (:task-id msg))
                   (task/complete (:claim-id msg) (:time msg)))))

(defn wipe!
  "Wipe a vnode's data clean."
  [vnode]
  (db/wipe! (:db vnode))
  vnode)
