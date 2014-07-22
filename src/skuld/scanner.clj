(ns skuld.scanner
  "Periodically scans over vnodes to rebuild internal data structures."
  (:use clojure.tools.logging)
  (:require [skuld.queue :as queue]))

; DEAL WITH IT
(in-ns 'skuld.vnode)
(clojure.core/declare tasks)
(in-ns 'skuld.scanner)


(def interval
  "How long to sleep between scanning runs, in ms"
  1000)

(defn scan!
  "Scans over all tasks in a vnode."
  [queue vnode]
  (doseq [task (skuld.vnode/tasks vnode)]
    (queue/update! queue task)))

(defn service
  "Starts a new scanning service. Takes an atom wrapping a map of partitions to
  vnodes, and a queue."
  [vnode queue]
  (let [running (promise)]
    (future
      (when (deref running interval true)
        (loop []
          (try
            (scan! queue vnode)
            (catch Throwable t
              (warn t "queue refiller caught")))

          (when (deref running interval true)
            (recur)))))

    running))

(defn shutdown!
  "Stops a scanning service."
  [scanner]
  (deliver scanner false))
