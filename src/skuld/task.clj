(ns skuld.task
  "Operations on individual tasks. Tasks have this structure:

  {:id     (Bytes) a 20-byte unique identifier for the task
   :data   (bytes) an arbitrary payload
   :claims [...]   a vector of claims}

  A claim is a map of:

  {:start  (long) milliseconds in linear time
   :end    (long) milliseconds in linear time}"
  (:refer-clojure :exclude [merge])
  (:require [skuld.flake :as flake]
            [taoensso.nippy :as nippy])
  (:import com.aphyr.skuld.Bytes
           (java.io DataOutputStream
                    DataInputStream)))

(nippy/extend-freeze Bytes 1
                     [^Bytes b ^DataOutputStream out]
                     (.write out (.bytes b) 0 (alength (.bytes b))))

(nippy/extend-thaw 1
                   [^DataInputStream in]
                   (let [bytes (byte-array 20)]
                     (.read in bytes)
                     (Bytes. bytes))) ; nom nom nom

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
     :completed nil}))

(defn valid-claim?
  "Is a claim currently valid?"
  [claim]
  (< (flake/linear-time)
     (+ (:end claim) clock-skew-buffer)))

(defn claimed?
  "Is this task currently claimed?"
  [task]
  (some valid-claim? (:claims task)))

(declare completed?)

(defn request-claim
  "Tries to apply the given claim to the given task. Throws if the given claim
  would be inconsistent."
  [task idx claim]
  (when-not task
    (throw (IllegalStateException. "task is nil")))

  (when (completed? task)
    (throw (IllegalStateException. "task is completed")))

  (let [start (:start claim)]
    (if (some #(<= start (:end %)) (:claims task))
      (throw (IllegalStateException. "task already claimed"))
      (assoc-in task [:claims idx] claim))))

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

(defn mergev
  "Merges several vectors together, taking the first non-nil value for each
  index."
  ([]
   [])
  ([v]
   v)
  ([v & vs]
   (let [cnt (apply max (map count vs))
         vs (reverse (cons v vs))]
     (->> (range cnt)
          (map (fn [idx]
                 (some
                   #(nth % idx nil)
                   vs)))
          (into [])))))

(defn merge-completed
  "Merges n completed times together."
  [times]
  (reduce (fn [completed t]
            (cond
              (nil? t)         completed
              (nil? completed) t
              (< completed t)  completed
              :else            t))
          nil
          times))

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
                               {:start (min (:start merged)
                                            (:start claim))
                                :end   (max (:end merged)
                                            (:end claim))
                                :completed (merge-completed
                                             (list (:completed merged)
                                                   (:completed claim)))}
                               claim)
                             merged))
                         nil
                         claims))))))

(defn merge
  "Merges n tasks together. Associative, commutative, idempotent."
  [& tasks]
  (-> (apply clojure.core/merge tasks)
      (assoc :claims (merge-claims (map :claims tasks)))))
