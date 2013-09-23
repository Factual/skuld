(ns skuld.queue
  "A node service which actually provides queuing semantics. You know,
  ordering, priorities, etc.
  
  Our general strategy with this queue is to be approximately consistent. If
  claim fails, come back and get another task. If a task is missing from this
  queue, AAE will recover it when it does a scan over the DB."
  (:import (java.util.concurrent TimeUnit
                                 ConcurrentSkipListSet))
  (:use [skuld.util :exclude [update!]]
        clojure.tools.logging)
  (:require [skuld.task :as task]))

(defprotocol Queue
  (update! [queue task])
  (poll!   [queue]))

(defrecord Task [id priority]
  Comparable
  (compareTo [a b]
    (compare+ a b :priority :id)))

(defn queue
  "Creates a new queue."
  []
  (ConcurrentSkipListSet.))

(extend-type ConcurrentSkipListSet
  Queue
  (update! [q task]
    (if (or (nil? task)
            (task/claimed? task)
            (task/completed? task))
      (.remove q (Task. (:id task)
                        (:priority task)))
      (.add q (Task. (:id task)
                     (:priority task)))))

  (poll! [q]
    (.pollFirst q)))
