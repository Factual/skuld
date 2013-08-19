(ns skuld.client
  (:require [skuld.net :as net]))

(defn client
  "Constructs a client which talks to a set of peers."
  [peers]
  {:peers peers
   :net   (doto (net/node {:server? false})
            (net/start!))})

(defn shutdown!
  "Shuts down a client."
  [client]
  (net/shutdown! (:net client)))

(defn peer
  "Select a peer for a client."
  [node]
  (rand-nth (:peers node)))

(defn sync-req!
  [client opts msg]
  (-> client
      :net
      (net/sync-req! (list (peer client)) opts msg)
      first))

(defn wipe! [client]
  (sync-req! client {} {:type :wipe}))

(defn enqueue!
  "Enqueue a single task. Returns a task ID."
  [client task]
  (:id (sync-req! client {} {:type :enqueue
                             :task task})))

(defn claim!
  "Claim a task for dt milliseconds. Returns a task."
  [client dt]
  (:task (sync-req! client {} {:type :claim
                               :dt   dt})))

(defn get-task
  "Gets a task by ID."
  [client task-id]
  (:task (sync-req! client {} {:type :get-task
                               :r    3
                               :id   task-id})))

(defn count-tasks
  "Returns a count of how many tasks are in the cluster."
  [client]
  (:count (sync-req! client {} {:type :count-tasks})))

(defn list-tasks
  "Returns a list of tasks."
  [client]
  (:tasks (sync-req! client {} {:type :list-tasks})))
