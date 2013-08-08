(ns skuld.task
  "Operations on individual tasks."
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

(defn map*
  "Like map, but runs as long as any seq has elements, padding with nil."
  [f & seqs]
  (lazy-seq
    (if (some seq seqs)
      (cons (apply f (map #(when (seq %) (first %)) seqs))
            (apply map* f (map rest seqs)))
      ())))

(defn merge-logs
  "Merges two sets of logs together."
  [logs1 logs2]
  (map* (partial map* #(or %1 %2)) logs1 logs2))

(defn merge
  "Merges two tasks together. Associative, commutative, idempotent."
  [t1 t2]
  (-> t1
      (clojure.core/merge t2)
      (assoc :logs (merge-logs (:logs t1) (:logs t2)))))
