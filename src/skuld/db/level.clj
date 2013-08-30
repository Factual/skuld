(ns skuld.db.level
  (:import (java.io Closeable)
           (com.aphyr.skuld Bytes))
  (:require [clj-leveldb :as level]
            [taoensso.nippy :as nippy]
            [skuld.task :as task])
  (:use skuld.db
        clojure.tools.logging))

(defrecord Level [level count-cache]
  DB
  (ids [db]
    (map #(Bytes. (first %)) (level/iterator level)))

  (tasks [db]
    (map #(nth % 1) (level/iterator level)))

  (count-tasks [db]
    @count-cache)

  (get-task [db task-id]
    (level/get level (.bytes ^Bytes task-id)))

  (claim-task! [db dt]
    (locking db
      (when-let [task (->> db
                           tasks
                           (remove task/claimed?)
                           (remove task/completed?)
                           first)]
        (let [claimed (task/claim task dt)]
          (level/put level
                     (.bytes ^Bytes (:id task))
                     (task/claim task dt))
          claimed))))

  (claim-task! [db id i claim]
    (locking db
      (->> (-> db
               (get-task id)
               (task/request-claim i claim))
           (level/put level (.bytes ^Bytes id)))))

  (merge-task! [db task]
    (locking db
      (when-not (let [current (get-task db (:id task))]
                  (level/put level (.bytes ^Bytes (:id task))
                             (task/merge current task))
                  current)
        ; We enqueued something new!
        (swap! count-cache inc))))

  (close! [db]
    (.close ^Closeable level))
  
  (wipe! [db]
    (locking db
      (->> level
           level/iterator
           (map (comp (partial level/delete level) first))
           dorun)
      (reset! count-cache 0))))

(defn open
  "Start a database service. Initializes the local DB storage and binds the
  database for further calls. Options:
  
  :host
  :port
  :partition
  :data-dir"
  [opts]
  (let [level (level/create-db (path (assoc opts :ext "level"))
                               :val-decoder #(and % (nippy/thaw %))
                               :val-encoder #(and % (nippy/freeze %)))
        c (count (level/iterator level))]
    (Level. level (atom c))))
