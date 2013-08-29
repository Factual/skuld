(ns skuld.db
  "Database backend interface."
  (:require [clojure.java.io :as io]))

(defprotocol DB
    (ids [db])
    (tasks [db])
    (count-tasks [db])
    (get-task    [db ^Bytes id])
    (merge-task! [db task])
    (close! [db])
    (wipe! [db]))

(defn path
  "Ensures the given data path exists and returns it. Options:
  
  :host
  :port
  :partition
  :ext"
  [{:keys [host port partition ext]}]
  (let [path (io/file "data"
                      (str host ":" port)
                      (str partition "." ext))]
    (io/make-parents path)
    (str path)))
