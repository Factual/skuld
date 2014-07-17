(ns skuld.scanner
  "Periodically scans over vnodes to rebuild internal data structures."
  (:use clojure.tools.logging)
  (:require [skuld.vnode :as vnode]
            [skuld.queue :as queue]))

(def interval
  "How long to sleep between scanning runs, in ms"
  1000)

(defn scan!
  "Scans over all tasks in a vnode."
  [queue vnode]
  (if (vnode/leader? vnode)
    (doseq [task (vnode/tasks vnode)]
      ; Ensure the task is in the queue if this vnode is a leader.
      (queue/update! queue task))))

(defn service
  "Starts a new scanning service. Takes an atom wrapping a map of partitions to
  vnodes, and a queue."
  [vnodes queue]
  (let [running (promise)]
    (future
      (when (deref running interval true)
        (loop []
          (try
            (->> vnodes
                 deref
                 vals
                 (map (partial scan! queue))
                 dorun)
            (catch Throwable t
              (warn t "queue refiller caught")))

          (when (deref running interval true)
            (recur)))))

    running))

(defn shutdown!
  "Stops a scanning service."
  [scanner]
  (deliver scanner false))
