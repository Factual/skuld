(ns skuld.vnode
  "A state machine which manages an instance of a partition on this node."
  (:require [skuld.task :as task])
  (:import com.aphyr.skuld.Bytes))

(defn vnode
  "Create a new vnode. Options:
  
  :partition
  :state"
  [opts]
  {:partition (get opts partition)
   :state (get opts :state :peer)
   :tasks (atom {})})

(defn enqueue
  "Enqueues a new task into this vnode."
  [vnode task]
  (assert (:id task))
  (swap! (:tasks vnode) assoc (Bytes. (:id task)) task))

(defn ids
  "All task IDs in this vnode."
  [vnode]
  (keys @(:tasks vnode)))

(defn tasks
  "All tasks in this vnode."
  [vnode]
  (vals @(:tasks vnode)))

(defn completed
  "All completed tasks in this vnode."
  [vnode]
  (vals @(:tasks vnode)))

(defn claimed
  "All claimed tasks in this vnode."
  [vnode]
  (->> vnode tasks (filter task/claimed?)))

(defn unclaimed
  "All unclaimed tasks in this vnode."
  [vnode]
  (->> vnode tasks (remove task/claimed?)))
