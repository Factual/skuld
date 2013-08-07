(ns skuld.task
  "Operations on individual tasks."
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

(def clock-skew-limit
  "The number of milliseconds we allow clocks, nodes, messages, and networks to
  drift."
  60000)

(defn claimed?
  "Is a task currently claimed?"
  [task]
  (let [claims (:claims task)]
    (and (not (empty? claims))
         (->> claims
              last
              :end
              (+ clock-skew-limit)
              (< (flake/linear-time))))))

(defn merge-task
  [t1 t2]
  (merge t1 t2))
