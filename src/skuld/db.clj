(ns skuld.db
  "Database backend interface."
  (:import (java.io File))
  (:require [potemkin :refer [defprotocol+]]
            [clojure.java.io :as io]))

(defprotocol+ DB
    (ids [db])
    (tasks [db])
    (unclaimed [db])
    (count-tasks [db])
    (get-task    [db ^Bytes id])
    (claim-task! [db ^Bytes id dt]
                 [db ^Bytes id i claim])
    (merge-task! [db task])
    (close! [db])
    (wipe! [db]))

(defn path
  "Constructs a path name for storing data in a vnode. Options:

  :host
  :port
  :partition
  :ext"
  [{:keys [host port partition ext]}]
  (-> "data"
      (io/file (str host ":" port)
               (str partition "." ext))
      str))

(defn path!
  "Like path, but ensures the path exists."
  [opts]
  (let [p (path opts)]
    (.mkdirs (io/file p))
    p))

(defn rm-rf! [path]
  (let [f (fn [f ^File p]
            (when (.isDirectory p)
              (doseq [child (.listFiles p)]
                (f f child)))
            (io/delete-file p))]
    (f f (io/file path))))

(defn destroy-data!
  "Removes all data for the given vnode."
  [opts]
  (rm-rf! (path opts)))

(defn local-data?
  "Is there local data for the given vnode?"
  [opts]
  (-> opts path io/file .exists))
