(ns skuld.leader-test
    (:use clojure.tools.logging
                  clojure.test
                  skuld.util
                  skuld.node
                  skuld.node-test)

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

(use-fixtures :once once)
(use-fixtures :each each)

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
        (with-redefs [vnode/election-timeout 0]
          (->> vnodes
               (map #(future
                       (dotimes [i (rand-int 10)]
                         (vnode/elect! %))
                         (Thread/sleep (rand-int 10))))
               (map deref)
               doall))

        (deliver running false)
        (test-election-consistent vnodes)))))
