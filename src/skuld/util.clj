(ns skuld.util
  "An Kitsch Sink"
  (:require
    [clojure.data.fressian :as fress]
    [primitive-math :as p])
  (:import
    [com.aphyr.skuld Bytes]
    [org.fressian.handlers ReadHandler WriteHandler]
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
  (if (zero? n)
    0
    (int (Math/floor (inc (/ n 2))))))

(defn majority-value
  "What element of a collection appears greater than 50% of the time?"
  [coll]
  (let [m (majority (count coll))]
    (when-let [pair (->> coll
                         (group-by identity)
                         (filter (comp (partial <= m) count val))
                         first)]
      (key pair))))

(defn assocv
  "Like assoc for vectors, but unlike assoc, allows you to assoc in indices
  which are greater than (count v)."
  [v idx value]
  (let [c (count v)]
    (if (<= idx c)
      (assoc v idx value)
      (-> v
          (concat (repeat (- idx c) nil)
                  (list value))
          vec))))

(defn update
  "Like update-in, but takes a single key. Given a map, a key, a function, and
  args, updates the value of (get map key) to be (f current-value arg1 arg2
  ...)"
  [m k f & args]
  (assoc m k (apply f (get m k) args)))

(defn update!
  "Transient version of update"
  [m k f & args]
  (assoc! m k (apply f (get m k) args)))

(defmacro compare+
  "Expands into a comparison between a and b on the basis of function f1, then
  f2, etc."
  [a b f & fs]
  (if (empty? fs)
    `(compare (~f ~a) (~f ~b))
    `(let [x# (compare (~f ~a) (~f ~b))]
       (if-not (zero? x#)
         x#
         (compare+ ~a ~b ~@fs)))))

;; Fressian
(def ^:private bytes-write-handler
  {Bytes
    {"skuld-bytes"
      (reify WriteHandler
        (fress/write [_ w bs]
          (.writeTag w "skuld-bytes" 1)
          (.writeBytes w (.bytes ^Bytes bs))))}})

(def ^:private bytes-read-handler
   {"skuld-bytes"
    (reify ReadHandler (fress/read [_ rdr tag component-count]
                         (Bytes. (.readObject rdr))))})

(def ^:private skuld-write-handlers
  (-> (conj bytes-write-handler fress/clojure-write-handlers)
    fress/associative-lookup
    fress/inheritance-lookup))

(def ^:private skuld-read-handlers
  (fress/associative-lookup (conj bytes-read-handler
                                  fress/clojure-read-handlers)))

(defn fress-write [x] (fress/write x :handlers skuld-write-handlers))

(defn fress-read [x] (fress/read x :handlers skuld-read-handlers))
