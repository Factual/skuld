(ns skuld.stress-test
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
            [skuld.politics :as politics]
            [skuld.net     :as net]
            [skuld.task    :as task]
            [skuld.aae     :as aae]
            [clojure.set   :as set]
            clj-helix.admin)
    (:import com.aphyr.skuld.Bytes))

(use-fixtures :once once)
(use-fixtures :each each)

(deftest claim-stress-test
  (prn :electing)
  (elect! *nodes*)
  (prn :elected)

  (let [n 100
        ids (->> (repeatedly (fn []
                               (prn :enqueuing)
                               (client/enqueue! *client* {:w 3} {:data "sup"})))
                 (take n)
                 doall)]

    (prn :enqueued)

    (is (not-any? nil? ids))
    (is (= n (client/count-tasks *client*)))
    ; Claim all extant IDs
    (let [claims (loop [claims {}]
                   (prn :claiming)
                   (if-let [t (client/claim! *client* 100000)]
                     (do
                       ; Make sure we never double-claim
                       (prn :claimed t)
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
  (dorun (pmap (comp politics/shutdown! :politics) *nodes*))

  (elect! *nodes*)
  
  ; Enqueue something and claim it.
  (let [id (client/enqueue! *client* {:w 3} {:data "meow"})
        _ (prn :claiming)
        claim (client/claim! *client* 100000)]
    (is (= id (:id claim)))

    ; Now kill 2 of the nodes which own that id, leaving one copy
    (let [originals (filter (fn [node]
                              (->> node
                                   vnodes
                                   vals
                                   (mapcat (fn [v]
                                             (try
                                               (vnode/tasks v)
                                               (catch RuntimeException e []))))
                                   (map :id)
                                   (some #{id})))
                            *nodes*)
          fallbacks (remove (set originals) *nodes*)
          [dead alive] (split-at 1 originals)
          _ (is (= 2 (count alive)))
          replacements (concat alive fallbacks)]

      ; Shut down 1/3 nodes
      (dorun (pmap shutdown! dead))

      ; At this point, 1-2 nodes should have the claim
      ;(->> replacements
      ;     (map vnodes)
      ;     (map vals)
      ;     (map (partial mapcat (fn [vnode]
      ;                            (try (doall (vnode/tasks vnode))
      ;                                 (catch RuntimeException e
      ;                                   (.printStackTrace e)
      ;                                   [])))))
      ;     clojure.pprint/pprint)

      ; Wait for the preflist to converge on the replacement cohort
      (while (not (and (= 3 (count (preflist (first alive) id)))
                       (set/subset?
                         (set (preflist (first alive) id))
                         (set (map (comp net/id :net) replacements)))))
        (prn (preflist (first alive) id))
        (Thread/sleep 1000))

      ; Elect a new cohort
      (elect! replacements)

      ; Verify that we cannot re-claim the element.
      (prn :checking-claim-present)
      (is (<= 2 (->> replacements
                     (map vnodes)
                     (map vals)
                     (map (partial mapcat vnode/tasks))
                     (map (partial some #(= id (:id %)))) 
                     (filter true?)
                     count)))

      (let [c (client/client [(select-keys (first alive) [:host :port])])]
        (try
          (is (not (client/claim! c 1000)))
          (finally
            (client/shutdown! c)))))))
