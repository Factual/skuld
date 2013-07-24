(ns skuld.net-test
  (:use skuld.net
        clojure.test))

(deftest basics
  (let [node (node {:handler prn})]
    (try (start! node)
         (finally
           (shutdown! node)))))
