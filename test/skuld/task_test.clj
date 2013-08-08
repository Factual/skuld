(ns skuld.task-test
  (:refer-clojure :exclude [merge])
  (:use skuld.task
        clojure.test)
  (:require [skuld.flake :as flake]))

(flake/init!)

(deftest merge-test
  (is (= (merge nil {:payload "hi"})
         {:payload "hi"
          :logs []}))
  (is (= (merge {:payload "hi"} nil)
         {:payload "hi"
          :logs []}))

  (is (= (merge {:logs [[:a nil :c]]}
                {:logs [[:a :b :c] nil [:d]]})
         {:logs [[:a :b :c]
                 []
                 [:d]]})))
