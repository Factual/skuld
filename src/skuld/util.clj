(ns skuld.util
  "An Kitsch Sink"
  (:require
    [primitive-math :as p])
  (:import
    [java.util
     Collection
     PriorityQueue]))

(deftype SeqContainer [^long idx s]
  Comparable
  (equals [_ x]
    (and
      (instance? SeqContainer x)
      (p/== (.idx ^SeqContainer x) idx)
      (identical? (.s ^SeqContainer x) s)))
  (compareTo [_ x]
    (let [^SeqContainer x x
          cmp (compare (first s) (first (.s x)))]
      (if (p/zero? cmp)
        (compare idx (.idx x))
        cmp))))

(defn- sorted-interleave- [^PriorityQueue heap]
  (lazy-seq
    (when-let [^SeqContainer container (.poll heap)]
      (when-let [s' (seq (rest (.s container)))]
        (.offer heap (SeqContainer. (.idx container) s')))
      (cons (first (.s container))
        (sorted-interleave- heap)))))

(defn sorted-interleave
  "Given n sorted sequences, yields a lazy sequence which yields all elements
  in all n collections, in order."
  [& seqs]
  (sorted-interleave-
    (PriorityQueue.
      ^Collection
      (map #(SeqContainer. %1 %2) (range) seqs))))
