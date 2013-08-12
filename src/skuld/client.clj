(ns skuld.client
  (:require [skuld.net :as net]))

(defn wipe! [net peer]
  (net/sync-req! net
                 [peer]
                 {}
                 {:type :wipe}))

(defn enqueue!
  "Enqueue a single task."
  [net peer task]
  (first
    (net/sync-req! net
                   [peer]
                   {}
                   {:type :enqueue
                    :task task})))

(defn count-tasks
  [net peer]
  (first
    (net/sync-req! net
                   [peer]
                   {}
                   {:type :count-tasks})))

(defn list-tasks
  [net peer]
  (first
    (net/sync-req! net [peer]
                   {}
                   {:type :list-tasks})))
