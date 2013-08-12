(ns skuld.vnode
  "A state machine which manages an instance of a partition on this node."
  (:require [skuld.task :as task])
  (:import com.aphyr.skuld.Bytes))

(defn vnode
  "Create a new vnode. Options:
  
  :partition
  :state"
  [opts]
  {:partition (get opts :partition)
   :state (get opts :state :peer)
   :tasks (atom (sorted-map))
   :claims (atom (sorted-map))})

(defn enqueue!
  "Enqueues a new task into this vnode."
  [vnode task]
  (let [id (:id task)]
    (swap! (:tasks vnode) assoc id (assoc task :id id))))

(defn merge-task!
  "Takes a task and merges it into this vnode."
  [vnode task]
  (swap! (:tasks vnode) update-in [(:id task)] task/merge task))

(defn get-task
  "Returns a specific task by ID."
  [vnode id]
  (-> vnode :tasks deref (get id)))

(defn ids
  "All task IDs in this vnode."
  [vnode]
  (keys @(:tasks vnode)))

(defn count-tasks
  "How many tasks are in this vnode?"
  [vnode]
  (count @(:tasks vnode)))

(defn tasks
  "All tasks in this vnode."
  [vnode]
  (vals @(:tasks vnode)))

(defn wipe!
  "Wipe a vnode's data clean."
  [vnode]
  (reset! (:tasks vnode) (sorted-map))
  (reset! (:claims vnode) (sorted-map))
  vnode)
