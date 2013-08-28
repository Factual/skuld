(ns skuld.node-test
  (:use [clj-helix.logging :only [mute]]
        clojure.tools.logging
        clojure.test
        skuld.util
        skuld.node)
  (:require [skuld.client  :as client]
            [skuld.admin   :as admin]
            [skuld.vnode   :as vnode]
            [skuld.flake   :as flake]
            [skuld.curator :as curator]
            [skuld.net     :as net]
            [skuld.task    :as task]
            [skuld.aae     :as aae]
            [clojure.set   :as set]
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
  (let [nodes (->> (range 5)
                   (pmap #(node {:port (+ 13000 %)}))
                   doall)
        vnodes (->> nodes
                    (map vnodes)
                    (mapcat vals))]
    (doseq [vnode vnodes]
      (curator/reset!! (vnode/zk-leader vnode) {:epoch 0
                                                :cohort #{}}))
    nodes))

(defn shutdown-nodes!
  "Shutdown a seq of nodes."
  [nodes]
  (->> nodes (pmap shutdown!) doall))

(defn partition-available?
  "Given a set of vnodes for a partition, do they comprise an available
  cohort?"
  [vnodes]
  (when-let [majority-epoch (majority-value (map vnode/epoch vnodes))]
    (some #(and (= majority-epoch (vnode/epoch %))
                (vnode/leader? %))
          vnodes)))

(defn elect!
  "Force election of a leader in all vnodes."
  [nodes]
  (loop [unelected (->> nodes
                        (map vnodes)
                        (mapcat vals)
                        (filter vnode/active?)
                        (group-by :partition)
                        vals
                        (remove partition-available?))]
      (when-not (empty? unelected)
        (locking *out*
          (prn (count unelected) "unelected partitions"))
;          (prn (map (partial map (juxt (comp :port vnode/net-id)
;                                       :partition
;                                       vnode/state))
;                    unelected)))
        (doseq [vnodes unelected]
          (vnode/elect! (rand-nth vnodes)))
        (Thread/sleep 100)
        (recur (remove partition-available? unelected)))))

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
  (let [id (client/enqueue! *client* {:data "hi there"})]
    (is id)
    (is (instance? Bytes id))

    ; Read it back
    (is (= (client/get-task *client* {:r 3} id)
           {:id id
            :claims []
            :data "hi there"}))))

(deftest count-test
  ; Enqueue a few tasks
  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:w 3} {:data "sup"}))

    (is (= n (client/count-tasks *client*)))))

(deftest list-tasks-test
  ; Enqueue
  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:w 3} {:data "sup"}))
    
    ; List
    (let [tasks (client/list-tasks *client*)]
      (is (= n (count tasks)))
      (is (= (sort (map :id tasks)) (map :id tasks)))
      (is (every? :data tasks)))))

(defn test-election-consistent
  "Asserts that the current state of the given vnodes is consistent, from a
  leader-election perspective."
  [vnodes]
  ; Take a snapshot of the states (so we're operating on locally consistent
  ; information
  (let [states (->> vnodes
                    (map vnode/state)
                    (map (fn [vnode state]
                           (assoc state :id (net/id (:net vnode))))
                         vnodes)
                    doall)
        leaders (filter #(= :leader (:type %)) states)
        true-leader (promise)]

    ; Exactly one leader for each epoch
    (doseq [[epoch leaders] (group-by :epoch leaders)]
      (is (= 1 (count leaders))))

    ; For all leaders
    (doseq [leader leaders]
      ; Find all nodes which this leader could write to
      (let [cohort (->> states
                        (filter #(and (= :follower (:type %))
                                      (= (:epoch leader) (:epoch %))
                                      (= (:cohort leader) (:cohort %)))))]

        ; There should be exactly one leader which could satisfy a quorum
        (when (<= (majority (count (:cohort leader)))
                  (count cohort))
          (is (deliver true-leader leader)))))))

(deftest election-test
  (let [part "skuld_0"
        nodes (filter (fn [node]
                        (when-let [v (vnode node part)]
                          (vnode/active? v)))
                      *nodes*)
        vnodes (map #(vnode % part) nodes)]

    (testing "Initially"
      (test-election-consistent vnodes))

    (testing "Stress"
      (let [running (promise)]
        ; Measure consistency continuously
        (future
          (while (deref running 1 true)
            (test-election-consistent vnodes)))

        ; Initiate randomized elections
        (->> vnodes
             (map #(future
                     (dotimes [i (rand-int 10)]
                       (vnode/elect! %)
                       (Thread/sleep (rand-int 10)))))
             (map deref)
             doall)

        (deliver running false)
        (test-election-consistent vnodes)))))

(deftest claim-test
  (elect! *nodes*)
  (let [id (client/enqueue! *client* {:w 3} {:data "hi"})]
    (is id)
    (let [task (client/claim! *client* {:timeout 5000} 1000)]
      (is (= id (:id task)))
      (is (task/claimed? task)))))

(defn log-cohorts
  []
  (println "cohorts are\n" (->> *nodes*
                                (map vnodes)
                                (mapcat vals)
                                (filter vnode/leader?)
                                (map (juxt (comp :port vnode/net-id)
                                           :partition
                                           vnode/epoch
                                           (comp (partial map :port)
                                                 :cohort vnode/state)))
                                (map pr-str)
                                (interpose "\n")
                                (apply str))))

(defn log-counts
  []
  (->> *nodes*
       (mapcat (fn [node]
                 (->> node
                      vnodes
                      (map (fn [[part vnode]]
                             [(:port (net/id (:net node)))
                              part
                              (vnode/leader? vnode)
                              (vnode/count-tasks vnode)])))))
       (clojure.pprint/pprint)))

(deftest claim-stress-test
  (elect! *nodes*)

  (let [n 100
        ids (->> (repeatedly (fn []
                               (client/enqueue! *client* {:w 3} {:data "sup"})))
                 (take n)
                 doall)]

    (is (not-any? nil? ids))
    (is (= n (client/count-tasks *client*)))
    ; Claim all extant IDs
    (let [claims (loop [claims {}]
                   (if-let [t (client/claim! *client* 100000)]
                     (do
                       ; Make sure we never double-claim
                       (assert (not (get claims (:id t))))
                       (let [claims (assoc claims (:id t) t)]
                         (if (= (count ids) (count claims))
                           claims
                           (recur claims))))
                     ; Out of claims?
                     (do
                       claims)))]
    (is (= (count ids) (count claims)))
    (is (= (set (keys claims)) (set ids))))))

(deftest election-handoff-test
  ; Shut down normal AAE initiators; we don't want them recovering data behind
  ; our backs. ;-)
  (dorun (pmap (comp aae/shutdown! :aae) *nodes*))

  ; Enqueue something and claim it.
  (elect! *nodes*)
  (let [id (client/enqueue! *client* {:w 3} {:data "meow"})
        claim (client/claim! *client* 100000)]
    (is (= id (:id claim)))

    ; Now kill 2 of the nodes which own that id, leaving one copy
    (let [originals (filter (fn [node]
                              (->> node
                                   vnodes
                                   vals
                                   (mapcat vnode/tasks)
                                   (map :id)
                                   (some #{id})))
                            *nodes*)
          fallbacks (remove (set originals) *nodes*)
          [dead alive] (split-at 1 originals)
          _ (is (= 2 (count alive)))
          replacements (concat alive fallbacks)]

      ; Shut down 2/3 nodes
      (dorun (pmap shutdown! dead))
     
      ; At this point, only 2 nodes should have the claim
;      (->> replacements
;           (map vnodes)
;           (map vals)
;           (map (partial mapcat vnode/tasks))
;           clojure.pprint/pprint)

      ; Wait for the preflist to converge on the replacement cohort
      (while (not (and (= 3 (count (preflist (first alive) id)))
                       (set/subset?
                         (set (preflist (first alive) id))
                         (set (map (comp net/id :net) replacements)))))
        (Thread/sleep 1000))

      ; Elect a new cohort
      (elect! replacements)

      ; Verify that we cannot re-claim the element.
      (is (<= 2 (->> replacements
                     (map vnodes)
                     (map vals)
                     (map (partial mapcat vnode/tasks))
                     (map (partial some #(= id (:id %))))
                     (filter true?)
                     count)))))) 
