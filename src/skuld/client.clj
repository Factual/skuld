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
  (let [res (-> client
                :net
                (net/sync-req! (list (peer client)) opts msg)
                first)]
    (when (nil? res)
      (throw (RuntimeException. "request timed out.")))
    (when-let [error (:error res)]
      (throw (RuntimeException. (str "server error: " error))))
    res))

(defn wipe! [client]
  (sync-req! client {} {:type :wipe}))

(defn enqueue!
  "Enqueue a single task. Returns a task ID."
  ([client task]
   (enqueue! client {} task))
  ([client opts task]
   (:id (sync-req! client {} {:type :enqueue
                              :w    (get opts :w 1)
                              :task task}))))

(defn claim!
  "Claim a task for dt milliseconds. Returns a task."
  ([client dt]
   (claim! client {} dt))
  ([client opts dt]
   (:task (sync-req! client opts {:type :claim
                                  :dt   dt}))))

(defn complete!
  "Complete a task with the given task ID and claim ID."
  ([client task-id claim-id]
   (complete! client {} task-id claim-id))
  ([client opts task-id claim-id]
   (:w (sync-req! client {} {:type      :complete
                             :w         (:w opts)
                             :task-id   task-id
                             :claim-id  claim-id}))))

(defn get-task
  "Gets a task by ID."
  ([client task-id]
   (get-task client {} task-id))
  ([client opts task-id]
   (:task (sync-req! client {} {:type :get-task
                                :r    (:r opts)
                                :id   task-id}))))

(defn count-tasks
  "Returns a count of how many tasks are in the cluster."
  [client]
  (:count (sync-req! client {} {:type :count-tasks})))

(defn list-tasks
  "Returns a list of tasks."
  [client]
  (:tasks (sync-req! client {} {:type :list-tasks})))
