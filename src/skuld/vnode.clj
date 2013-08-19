(ns skuld.vnode
  "A state machine which manages an instance of a partition on this node."
  (:use skuld.util
        clojure.tools.logging)
  (:require [skuld.task :as task]
            [skuld.net  :as net]
            [skuld.curator :as curator]
            [clj-helix.route :as route])
  (:import com.aphyr.skuld.Bytes))

(defn vnode
  "Create a new vnode. Options:
  
  :partition
  :state"
  [opts]
  {:partition (:partition opts)
   :net       (:net opts)
   :router    (:router opts)
   :zk-leader (delay
                (curator/distributed-atom (:curator opts)
                                          (str "/" (:partition opts)
                                               "/leader")
                                          {:epoch 0
                                           :cohort #{}}))
   :state     (atom {:type :follower
                     :leader nil
                     :epoch 0})
   :tasks     (atom (sorted-map))})

;; Leaders

(defn peers
  "Peers for this vnode."
  [vnode]
  (map #(select-keys % [:host :port])
    (route/instances (:router vnode) :skuld (:partition vnode) :peer)))

(defn zk-leader
  "A curator distributed atom backed by Zookeeper, containing the current epoch
  and cohort."
  [vnode]
  @(:zk-leader vnode))

(defn net-id
  "The net id of the node hosting this vnode."
  [vnode]
  (net/id (:net vnode)))

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
  (let [self   (net-id vnode)
        cohort (set (peers vnode))
        ; How many nodes do we need to hear from in the new cohort?
        maj    (if (cohort self)
                 (dec (majority (count cohort)))
                 (majority (count cohort)))
        others (remove #{self} cohort)

        ; Increment the epoch and update the node set.
        epoch (-> vnode
                  :state
                  (swap! (fn [state]
                           (merge state {:epoch (inc (:epoch state))
                                         :leader self
                                         :type :candidate
                                         :cohort cohort})))
                  :epoch)

        ; Check ZK's last leader information
        old (deref (zk-leader vnode))]
    (if (<= epoch (:epoch old))
      ; We're outdated; fast-forward to the new epoch.
      (swap! (:state vnode) (fn [state]
                              (merge state {:epoch (max (:epoch state)
                                                        (:epoch old))
                                            :leader nil
                                            :type :follower
                                            :cohort (:cohort old)})))

      (let [old-cohort (set (:cohort old))

            ; How many nodes do we need to hear from to constitute a majority?
            old-maj (if (old-cohort self)
                      (dec (majority (count old-cohort)))
                      (majority (count old-cohort)))

            ; Our vote request
            req {:type :request-vote
                 :partition (:partition vnode)
                 :leader (net/id (:net vnode))
                 :cohort cohort
                 :epoch epoch}

            ; Issue requests to old cohort
            old-responses (future
                            (net/sync-req! (:net vnode)
                                           (disj old-cohort self)
                                           {:r old-maj}
                                           req))
            ; Issue requests to new cohort
            responses (future
                        (net/sync-req! (:net vnode)
                                       (disj cohort self)
                                       {:r maj}
                                       req))

            ; Do we have the support of the old cohort?
            old-votes (count (filter :vote @old-responses))
            votes (count (filter :vote @responses))]

;        (prn old-votes "/" old-maj "from old cohort")
;        (prn votes "/" maj "from new cohort")

        (when (and (<= old-maj old-votes)
                   (<= maj votes))
          ; Todo: sync claim set from old cohort

          ; Update ZK with new cohort and epoch--but only if we won.
          (let [new-leader {:epoch epoch
                            :cohort cohort}
                set-leader (curator/swap!! (zk-leader vnode)
                                           (fn [current]
                                             (if (= old current)
                                               new-leader
                                               current)))]
            (if (= new-leader set-leader)
              ; Success!
              (let [state (swap! (:state vnode)
                                 (fn [state]
                                   (if (= epoch (:epoch state))
                                     ; Still waiting for responses
                                     (assoc state :type :leader)
                                     ; We voted for someone else in the meantime
                                     state)))]
                (prn (:partition vnode)
                     "election successful: cohort now" epoch cohort)
                ))))))))

(defn request-vote!
  "Accepts a new-leader message from a peer, and returns a vote for the
  node--or an error if our epoch is current or newer. Returns a response
  message."
  [vnode msg]
  (let [{:keys [cohort epoch leader]} msg
        ; We set :vote to true only if we're voting for this node; otherwise
        ; it's false. Then we can just return our state to our peer.
        state (swap! (:state vnode) (fn [state]
                                      (if (< (:epoch state) epoch)
                                        ; The leader is ahead of us.
                                        (merge state
                                               {:epoch epoch
                                                :cohort cohort
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

;; Tasks

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
       (remove task/claimed?)))

(defn request-claim!
  "Applies a claim to a given task. Takes a message from a leader like
  
  {:epoch  The leader's epoch
   :id     The task ID
   :i      The index of the claim
   :claim  A claim map}

  ... and applies the given claim to our copy of that task. Returns an empty
  map if the claim is successful, or {:error ...} if the claim failed."
  [vnode {:keys [id epoch claim]}]
  (try
    (locking vnode
      (if (= epoch (epoch vnode))
        (do
          (swap! (:tasks vnode)
                 (fn [tasks]
                   (assoc tasks id
                          (task/request-claim (get tasks id) claim))))
          {})
        {:error (str "leader epoch " epoch
                     " does not match local epoch " (epoch vnode))}))
    (catch IllegalStateException e
      {:error (.getMessage e)})))

(defn claim!
  "Picks a task from this vnode and claims it for dt milliseconds. Returns the
  claimed task."
  [vnode dt]
  ; Compute leader state
  (let [state     (state vnode)
        epoch     (:epoch state)
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
    (when-let [task (swap! (:tasks vnode)
                           (fn [tasks]
                             (let [t (->> tasks
                                          (remove task/claimed?)
                                          first
                                          task/claim)]
                               (assoc tasks (:id t) t))))]
      (let [; Get claim details
            i     (dec (count (:claims task)))
            claim (nth (:claims task) i)

            ; Try to replicate claim remotely
            responses (net/sync-req! (:net vnode)
                                     (disj cohort (net-id vnode))
                                     {:r maj}
                                     {:type   :request-claim
                                      :epoch  epoch
                                      :id     (:id task)
                                      :i      i
                                      :claim  claim})
            successes (count (remove :error responses))]
    (if (<= maj successes)
      task
      (throw (RuntimeException. (str "needed " maj
                                     " acks from followers, only received "
                                     successes))))))))
                            
(defn wipe!
  "Wipe a vnode's data clean."
  [vnode]
  (reset! (:tasks vnode) (sorted-map))
  vnode)

