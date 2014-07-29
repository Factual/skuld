(ns skuld.queue
  "A node service which actually provides queuing semantics. You know,
  ordering, priorities, etc.
  
  Our general strategy with this queue is to be approximately consistent. If
  claim fails, come back and get another task. If a task is missing from this
  queue, AAE will recover it when it does a scan over the DB.
  
  There's one queue per node. That queue is a rough approximation of all tasks
  which could be claimed on the local node--e.g. it contains only tasks for
  which the local node has a leader vnode."
  (:import (java.util.concurrent TimeUnit
                                 ConcurrentSkipListSet))
  (:use [skuld.util :exclude [update!]]
        clojure.tools.logging)
  (:require [skuld.task :as task]
            [skuld.flake :as flake]))

(defrecord Task [id priority]
  Comparable
  (compareTo [a b]
    (compare+ a b :priority :id)))

(defn queues
  "Creates a new set of queues."
  []
  (atom {}))

(defn named-queue
  []
  {:queue         (ConcurrentSkipListSet.)
   :last-modified (atom (flake/linear-time))})

(defn touch-named-queue
  [named-queue]
  (swap! (:last-modified named-queue) max (flake/linear-time)))


(defn update!
  "Update a task in the queue"
  [queues task]
  (let [queue-name (:queue task)]
    (if-not queue-name
      (throw (IllegalArgumentException. "Task did not specify a queue")))

    (let [named-queue (if-let [queue (get @queues queue-name)]
                        queue
                        (get
                          (swap! queues
                                 (fn [queues]
                                   (if-not (get queues queue-name)
                                     (assoc queues queue-name (named-queue))
                                     queues)))
                          queue-name))
          q ^ConcurrentSkipListSet (:queue named-queue)]

      ; Mark the last time the queue was used
      (swap! (:last-modified named-queue) max (flake/linear-time))

      (if (or (nil? task)
              (task/claimed? task)
              (task/completed? task))
        (.remove q (Task. (:id task)
                          (:priority task)))
        (.add q (Task. (:id task)
                       (:priority task)))))))

(defn poll!
  [queues queue-name]
  (if-let [named-queue (get @queues queue-name)]
    (.pollFirst ^ConcurrentSkipListSet (:queue named-queue))))

(defn count-queue
  [queues queue-name]
  (if-let [named-queue (get @queues queue-name)]
    (count (:queue named-queue))
    0))