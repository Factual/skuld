(ns skuld.politics
  "Periodically initiates the election cycle."
  (:require [skuld.vnode :as vnode]
            [clojure.tools.logging :refer [warn]]))

(defn service
  "Creates a new politics service."
  [vnodes]
  (let [running (promise)]
    (future
      (loop []
        (try
          (when-let [vnodes (-> vnodes deref vals)]
            (->> vnodes
                 shuffle
                 (pmap
                   (fn [vnode]
                     (try
                       (Thread/sleep (rand-int 300))
                       (if (vnode/leader? vnode)
                         (vnode/broadcast-heartbeat! vnode)
                         (vnode/elect! vnode))
                       (catch Throwable t
                         (warn t "exception while electing" (:partition vnode))))))
               dorun))
          (catch Throwable t
            (warn t "exception in election cycle")))
        (when (deref running 1000 true)
          (recur))))
    running))

(defn shutdown!
  "Shuts down a politics service."
  [politics]
  (deliver politics false))
