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
  (locking *out*
    (prn "ensuring leaders elected"))
  (loop [unelected (->> nodes
                        (map vnodes)
                        (mapcat vals)
                        (group-by :partition)
                        vals
                        (remove partition-available?))]
      (when-not (empty? unelected)
        (locking *out*
          (prn (count unelected) "unelected partitions:")
          (prn (map (partial map (juxt vnode/net-id :partition vnode/state))
                    unelected)))
        (doseq [vnodes unelected]
          (vnode/elect! (rand-nth vnodes)))
        (Thread/sleep 100)
        (recur (remove partition-available? unelected))))
  (prn "leader election complete"))

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
    (is (= (client/get-task *client* id)
           {:id id
            :logs nil
            :data "hi there"}))))

(deftest count-test
  ; Enqueue a few tasks
  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:data "sup"}))

    (is (= n (client/count-tasks *client*)))))

(deftest list-tasks-test
  ; Enqueue
  (let [n 10]
    (dotimes [i n]
      (client/enqueue! *client* {:data "sup"}))
    
    ; List
    (let [tasks (client/list-tasks *client*)]
      (is (= n (count tasks)))
      (is (= (sort (map :id tasks)) (map :id tasks)))
      (is (some :data tasks)))))

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
                        (filter #(and (= (:epoch leader) (:epoch %))
                                      (= (:cohort leader) (:cohort %)))))]
        ; The cohort should be a subset of the leader's known nodes
        (is (set/subset? (set (map :id cohort))
                         (set (:cohort leader))))

        ; And there should be exactly one leader which could satisfy a quorum
        (when (<= (majority (count (:cohort leader)))
                  (count cohort))
          (deliver true-leader leader))))))

(deftest election-test
  (let [part "skuld_0"
        nodes (filter #(vnode % part) *nodes*)
        vnodes (map #(vnode % part) nodes)]
    (is (= 3 (count nodes)))
    (is (apply = 3 (map (comp count vnode/peers) vnodes)))
    (is (apply = (map vnode/peers vnodes)))
    (is (every? identity vnodes))

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

(deftest ^:focus claim-test
  ; Enqueue
  (elect! *nodes*)
  (prn (->> *nodes*
            (map vnodes)
            (mapcat vals)
            (map (juxt vnode/net-id :partition vnode/leader?))))
  (client/enqueue! *client* {:data "hi"}))
;  (prn :client-got (client/claim! *client* {:timeout 50000} 1000)))
