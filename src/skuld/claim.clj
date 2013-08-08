(ns skuld.claim
  (:refer-clojure :exclude [merge])
  (:require [skuld.flake :as flake]))

(defn claim
  "Generates a new claim valid for dt millis."
  [task-id epoch dt]
  (let [now (flake/linear-time)]
    {:epoch epoch
     :start now
     :end (+ now dt)}))

(def clock-skew-limit
  "The number of milliseconds we allow clocks, nodes, messages, and networks to
  drift."
  60000)

(defn current?
  "Is a claim currently held?"
  [claim]
  (-> claim
      :end
      (+ clock-skew-limit)
      (< (flake/linear-time))))

(defn merge
  [c1 c2]
  (if (< (:epoch c1) (:epoch c2))
    c2
    c1))
