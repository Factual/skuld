(ns skuld.zk-test
  "Supports zookeeper testing."
  (:require [skuld.logging :as logging])
  (:import (org.apache.curator.test TestingServer)))

(defmacro with-zk
  "Evaluates body with a zookeeper server running, and the connect string bound
  to the given variable. Ensures the ZK server is shut down at the end of the
  body. Example:
  
  (with-zk [zk-string]
    (connect-to zk-string)
    ...)"
  [[connect-string] & body]
  `(logging/suppress
     ["org.apache.zookeeper" "org.apache.helix" "org.apache.curator" "org.I0Itec.zkclient" "org.apache.zookeeper.server.SessionTrackerImpl"]
     (let [zk#             (TestingServer.)
           ~connect-string (.getConnectString zk#)]
       (try
         ~@body
         (finally
           (.stop zk#)
           (.close zk#))))))
