(ns skuld.level
  (:import (java.io Closeable)
           (com.aphyr.skuld Bytes))
  (:require [clj-leveldb :as level]
            [taoensso.nippy :as nippy]
            [skuld.task :as task]))

(defprotocol DB
  (ids [db])
  (tasks [db])
  (count-tasks [db])
  (get-task    [db ^Bytes id])
  (merge-task! [db task])
  (close! [db])
  (wipe! [db]))

(defrecord Level [level count-cache]
  DB
  (ids [db]
    (map #(Bytes. (first %)) (level/iterator level)))

  (tasks [db]
    (map #(nth % 1) (level/iterator level)))

  (count-tasks [db]
    @count-cache)

  (get-task [db task-id]
    (level/get level (.bytes task-id)))

  (merge-task! [db task]
    (when-not
      (locking db
        (let [current (get-task db (:id task))]
          (level/put level (.bytes ^Bytes (:id task))
                     (task/merge current task))
          current)
        (swap! count-cache inc))))

  (close! [db]
    (.close ^Closeable level))
  
  (wipe! [db]
    (->> level
         level/iterator
         (map (comp (partial level/delete level) first))
         dorun)
    (locking db
      (reset! count-cache (count (level/iterator level))))))

(defn open
  "Start a database service. Initializes the local DB storage and binds the
  database for further calls. Options:
  
  :host
  :port
  :partition
  :data-dir"
  [opts]
  (let [level (level/create-db (or (:data-dir opts)
                                   (str "skuld-"
                                        (:host opts) ":" (:port opts) "-"
                                        (:partition opts)
                                        ".level"))
                               :val-decoder #(and % (nippy/thaw %))
                               :val-encoder #(and % (nippy/freeze %)))
        c (count (level/iterator level))]
    (Level. level (atom c))))
