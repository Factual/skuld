(ns skuld.zk-test
  "Supports zookeeper testing."
  (:import (org.apache.curator.test TestingServer)))

(defmacro with-zk
  "Evaluates body with a zookeeper server running, and the connect string bound
  to the given variable. Ensures the ZK server is shut down at the end of the
  body. Example:
  
  (with-zk [zk-string]
    (connect-to zk-string)
    ...)"
  [[connect-string] & body]
  `(let [zk#             (TestingServer.)
         ~connect-string (.getConnectString zk#)]
     (try
       ~@body
       (finally
         (.close zk#)))))
