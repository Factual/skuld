(ns skuld.task
  "Operations on individual tasks."
  (:require [skuld.flake :as flake]))

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
