(ns skuld.politics
  "Periodically initiates the election cycle."
  (:require [skuld.vnode :as vnode])
  (:use clojure.tools.logging))

(defn service
  "Creates a new politics service."
  [vnodes]
  (let [running (promise)]
    (future
      (loop []
        (try
          (->> vnodes
               deref
               vals
               shuffle
               (pmap
                 (fn [vnode]
                   (try
                     (Thread/sleep (rand-int 100))
                     (vnode/elect! vnode)
                     (catch Throwable t
                       (warn t "electing" (:partition vnode))))))
               dorun)
            (deref running 1000 true)
          (catch Throwable t
            (warn t "in election cycle")))))
    running))

(defn shutdown!
  "Shuts down a politics service."
  [politics]
  (deliver politics false))
