(ns skuld.task
  "Operations on individual tasks. Tasks have this structure:

  {:id     (Bytes) a 20-byte unique identifier for the task
   :data   (bytes) an arbitrary payload
   :claims [...]   a vector of claims}

  A claim is a map of:

  {:start     (long) milliseconds in linear time
   :end       (long) milliseconds in linear time
   :completed (long) milliseconds in linear time}"
  (:refer-clojure :exclude [merge])
  (:use skuld.util)
  (:require [skuld.flake :as flake]
            [skuld.util :refer [fress-read fress-write]])
  (:import com.aphyr.skuld.Bytes))

(def clock-skew-buffer
  "We allow nodes and clocks to drift by this much."
  (* 1000 60))

(defn task
  "Creates a new task around the given map."
  [task]
  (clojure.core/merge task
                      {:id        (or (:id task) (Bytes. (flake/id)))
                       :claims    []}))

(defn new-claim
  "Creates a new claim, valid for dt milliseconds."
  [dt]
  (let [now (flake/linear-time)]
    {:start     now
     :end       (+ now dt)
     :completed nil
     :logs      []}))

(defn valid-claim?
  "Is a claim currently valid?"
  [claim]
  (when-let [end (:end claim)]
    (< (flake/linear-time)
       (+ end clock-skew-buffer))))

(defn claimed?
  "Is this task currently claimed?"
  [task]
  (try
    (some valid-claim? (:claims task))
    (catch Exception e
      (throw e))))

(declare completed?)

(defn request-claim
  "Tries to apply the given claim to the given task. Throws if the given claim
  would be inconsistent."
  [task idx claim]
  (when-not task
    (throw (IllegalStateException. "task is nil")))

  (when (completed? task)
    (throw (IllegalStateException. "task is completed")))

  (when (nth (:claims task) idx nil)
    (throw (IllegalStateException. (str "already have a claim for " idx))))

  (let [start (:start claim)]
    (if (some #(<= start (+ clock-skew-buffer (:end %))) (:claims task))
      (throw (IllegalStateException. "task already claimed"))
      (assoc task :claims
             (assocv (:claims task) idx claim)))))

(defn claim
  "Returns a copy of a task claimed for dt milliseconds. (last (:claims task))
  will be the claim applied. Throws if the task is presently claimed."
  [task dt]
  (request-claim task (count (:claims task)) (new-claim dt)))

(defn completed?
  "Is this task completed?"
  [task]
  (some :completed (:claims task)))

(defn complete
  "Returns a copy of the task, but completed. Takes a claim index, and a time
  to mark the task as completed at."
  [task claim-idx t]
  (assoc-in task [:claims claim-idx :completed] t))


(defn merge-by
  "Merges times by merge-fn"
  [merge-fn & times]
  (let [valid-times (filter (comp not nil?) times)]
    (if (empty? valid-times)
      nil
      (apply merge-fn valid-times))))

(defn merge-logs
  "Merge logs vectors by picking the first non-nil value from each index"
  [& logses]
  (->> logses
       (map count)
       (apply max)
       range
       (mapv (fn [i]
               (->> logses
                    (map #(nth % i nil))
                    (keep identity)
                    first)))))

(defn merge-claims
  "Merges a collection of vectors of claims together."
  [claims]
  (if (empty? claims)
    claims
    ; Determine how many claims there are
    (->> claims
         (map count)
         (apply max)
         range
         ; Combine the ith claim from each vector
         (mapv (fn [i]
                 (reduce (fn combine [merged claims]
                           (if-let [claim (nth claims i nil)]
                             (if merged
                               {:start     (merge-by min
                                                     (:start merged)
                                                     (:start claim))
                                :end       (merge-by max
                                                     (:end merged)
                                                     (:end claim))
                                :completed (merge-by min
                                                     (:completed merged)
                                                     (:completed claim))
                                :logs      (merge-logs (:logs merged)
                                                       (:logs claim))}
                               claim)
                             merged))
                         nil
                         claims))))))

(defn merge
  "Merges n tasks together. Associative, commutative, idempotent."
  [& tasks]
  (-> (apply clojure.core/merge tasks)
      (assoc :claims (merge-claims (map :claims tasks)))))
