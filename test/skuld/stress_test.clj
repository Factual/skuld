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

;(deftest claim-stress-test
;  (prn :electing)
;  (elect! *nodes*)
;  (prn :elected)

;  (let [n 100
;        ids (->> (repeatedly (fn []
;                               (prn :enqueuing)
;                               (client/enqueue! *client* {:w 3} {:data "sup"})))
;                 (take n)
;                 doall)]

;    (prn :enqueued)

;    (is (not-any? nil? ids))
;    (is (= n (client/count-tasks *client*)))
;    ; Claim all extant IDs
;    (let [claims (loop [claims {}]
;                   (prn :claiming)
;                   (if-let [t (client/claim! *client* 100000)]
;                     (do
;                       ; Make sure we never double-claim
;                       (prn :claimed t)
;                       (assert (not (get claims (:id t))))
;                       (let [claims (assoc claims (:id t) t)]
;                         (if (= (count ids) (count claims))
;                           claims
;                           (recur claims))))
;                     ; Out of claims?
;                     (do
;                       claims)))]
;    (is (= (count ids) (count claims)))
;    (is (= (set (keys claims)) (set ids))))))

;(deftest election-handoff-test
;  ; Shut down normal AAE initiators; we don't want them recovering data behind
;  ; our backs. ;-)
;  (prn :shutting-down-daemons)
;  (dorun (pmap (comp aae/shutdown! :aae) *nodes*))
;  (dorun (pmap (comp politics/shutdown! :politics) *nodes*))

;  (prn :electing)
;  (elect! *nodes*)
  
;  ; Enqueue something and claim it.
;  (prn :enqueuing)
;  (let [id (client/enqueue! *client* {:w 3} {:data "meow"})
;        _ (prn :claiming)
;        claim (client/claim! *client* 100000)]
;    (is (= id (:id claim)))
;    (prn :claimed claim)

;    ; Now kill 2 of the nodes which own that id, leaving one copy
;    (let [originals (filter (fn [node]
;                              (->> node
;                                   vnodes
;                                   vals
;                                   (mapcat (fn [v]
;                                             (try
;                                               (vnode/tasks v)
;                                               (catch RuntimeException e []))))
;                                   (map :id)
;                                   (some #{id})))
;                            *nodes*)
;          fallbacks (remove (set originals) *nodes*)
;          [dead alive] (split-at 1 originals)
;          _ (is (= 2 (count alive)))
;          replacements (concat alive fallbacks)]

;      (prn :killing-nodes)

;      ; Shut down 2/3 nodes
;      (dorun (pmap shutdown! dead))

;      ; At this point, only 2 nodes should have the claim
;      (->> replacements
;           (map vnodes)
;           (map vals)
;           (map (partial mapcat vnode/tasks))
;           clojure.pprint/pprint)

;      ; Wait for the preflist to converge on the replacement cohort
;      (prn :waiting-for-preflists)
;      (while (not (and (= 3 (count (preflist (first alive) id)))
;                       (set/subset?
;                         (set (preflist (first alive) id))
;                         (set (map (comp net/id :net) replacements)))))
;        (Thread/sleep 1000))

;      ; Elect a new cohort
;      (prn :electing)
;      (elect! replacements)

;      ; Verify that we cannot re-claim the element.
;      (prn :checking-claim-present)
;      (is (<= 2 (->> replacements
;                     (map vnodes)
;                     (map vals)
;                     (map (partial mapcat vnode/tasks))
;                     (map (partial some #(= id (:id %)))) 
;                     (filter true?)
;                     count)))

;      (prn :trying-claim)
;      (is (not (client/claim! *client* 1000))))))
