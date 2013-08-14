(ns skuld.util
  "An Kitsch Sink"
  (:require
    [primitive-math :as p])
  (:import
    [java.util
     Collection
     PriorityQueue]))

(deftype SeqContainer [key-fn ^long idx s]
  Comparable
  (equals [_ x]
    (and
      (instance? SeqContainer x)
      (p/== (.idx ^SeqContainer x) idx)
      (identical? (.s ^SeqContainer x) s)))
  (compareTo [_ x]
    (let [^SeqContainer x x
          cmp (compare (key-fn (first s)) (key-fn (first (.s x))))]
      (if (p/zero? cmp)
        (compare idx (.idx x))
        cmp))))

(defn- sorted-interleave- [key-fn ^PriorityQueue heap]
  (lazy-seq
    (loop [chunk-idx 0, buf (chunk-buffer 32)]
      (if (.isEmpty heap) 
        (chunk-cons (chunk buf) nil)
        (let [^SeqContainer container (.poll heap)]
          (chunk-append buf (first (.s container)))
          (when-let [s' (seq (rest (.s container)))]
            (.offer heap (SeqContainer. key-fn (.idx container) s')))
          (let [chunk-idx' (unchecked-inc chunk-idx)]
            (if (< chunk-idx' 32)
              (recur chunk-idx' buf)
              (chunk-cons
                (chunk buf)
                (sorted-interleave- key-fn heap)))))))))

(defn sorted-interleave-by
  "Like sorted-interleave, but takes a specific keyfn, like sort-by."
  [key-fn & seqs]
  (sorted-interleave-
    key-fn
    (PriorityQueue.
      ^Collection
      (map #(SeqContainer. key-fn %1 %2) (range) (remove empty? seqs)))))

(defn sorted-interleave
  "Given n sorted sequences, yields a lazy sequence which yields all elements
  in all n collections, in order."
  [& seqs]
  (apply sorted-interleave-by identity seqs))

(defn majority
  "For N replicas, what would consititute a majority?"
  [n]
  (int (Math/floor (inc (/ n 2)))))

(defn majority-value
  "What element of a collection appears greater than 50% of the time?"
  [coll]
  (let [m (majority (count coll))]
    (when-let [pair (->> coll
                         (group-by identity)
                         (filter (comp (partial <= m) count val))
                         first)]
      (key pair))))
