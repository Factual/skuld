(ns skuld.db.level
  (:import (java.io Closeable)
           (com.aphyr.skuld Bytes))
  (:require [clj-leveldb :as level]
            [skuld.task :as task]
            [skuld.util :refer [fress-read fress-write]])
  (:use skuld.db
        clojure.tools.logging))

(defrecord Level [level count-cache running]
  DB
  (ids [db]
    (assert @running)
    (map #(Bytes. (first %)) (level/iterator level)))

  (tasks [db]
    (assert @running)
    (map #(nth % 1) (level/iterator level)))

  (count-tasks [db]
    @count-cache)

  (get-task [db task-id]
    (assert @running)
    (level/get level (.bytes ^Bytes task-id)))

  (claim-task! [db task-id dt]
    (locking db
      (assert @running)
      (when-let [task (get-task db task-id)]
        (let [claimed (task/claim task dt)]
          (level/put level
                     (.bytes ^Bytes (:id task))
                     (task/claim task dt))
          claimed))))

  (claim-task! [db id i claim]
    (locking db
      (assert @running)
      (->> (-> db
               (get-task id)
               (task/request-claim i claim))
           (level/put level (.bytes ^Bytes id)))))

  (merge-task! [db task]
    (locking db
      (assert @running)
      (when-not (let [current (get-task db (:id task))]
                  (level/put level (.bytes ^Bytes (:id task))
                             (task/merge current task))
                  current)
        ; We enqueued something new!
        (swap! count-cache inc))))

  (close! [db]
    (swap! running (fn [was]
                     (when was
                       (.close ^Closeable level)
                       false))))
  
  (wipe! [db]
    (locking db
      (when @running
        (->> level
             level/iterator
             (map (comp (partial level/delete level) first))
             dorun)
        (reset! count-cache 0)))))

(defn open
  "Start a database service. Initializes the local DB storage and binds the
  database for further calls. Options:
  
  :host
  :port
  :partition
  :data-dir"
  [opts]
  (let [level (level/create-db (path! (assoc opts :ext "level"))
                               {:val-decoder #(and % (fress-read %))
                                :val-encoder #(and % (fress-write %))})
        c (count (level/iterator level))]
    (Level. level (atom c) (atom true))))
