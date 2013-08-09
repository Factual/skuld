(ns skuld.util
  "An Kitch Sink"
  (:import (java.util.concurrent ConcurrentSkipListSet)))

(defn sorted-interleave-helper
  [seqs ^ConcurrentSkipListSet heads]
  (when-let [[element idx] (.pollFirst heads)]
    (let [s (nth seqs idx)]
      (when-let [e (first s)]
        (.add heads [e idx]))
      (cons element
            (lazy-seq (sorted-interleave-helper
                        (assoc seqs idx (next s))
                        heads))))))

(defn sorted-interleave-by
  "Like sorted-interleave, but takes a specific keyfn, like sort-by."
  [keyfn & seqs]
  ; We keep a sorted map of the heads of each seq. Given seqs a, b, and c, with
  ; elements a0, a1, ...:
  ;
  ; heads: #{[a0 0]
  ;          [c0 2]
  ;          [b0 1]}
  ;
  ; To get the next element, we just remove the first entry in the map, and
  ; replace it with the head of the successor. ConcurrentSkipListMapeeds a
  ; stable comparator, so we use the sequence position.
  (let [heads (ConcurrentSkipListSet.
                (fn [a b]
                  (let [x (compare (keyfn (first a)) (keyfn (first b)))]
                    (if-not (zero? x)
                      x
                      (compare (nth a 1) (nth b 1))))))]

    ; Fill out initial heads    
    (dotimes [i (count seqs)]
      (let [s (nth seqs i)]
        (when (first s)
          (.add heads [(first s) i]))))

    (sorted-interleave-helper (mapv next seqs) heads)))

(defn sorted-interleave
  "Given n sorted sequences, yields a lazy sequence which yields all elements
  in all n collections, in order."
  [& seqs]
  (apply sorted-interleave-by identity seqs))
