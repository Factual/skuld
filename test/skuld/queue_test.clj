(ns skuld.queue-test
  (:use skuld.queue
        clojure.test)
  (:require [skuld.task :as task]))

(deftest enqueue-test
  (let [q (queue)]
    (is (zero? (count q)))

    (update! q {:id 0 :foo :bar})
    (is (= 1 (count q)))

    (update! q {:id 0 :bar :baz})
    (is (= 1 (count q)))

    (is (= (->Task 0 nil) (poll! q)))
    (is (= nil (poll! q)))))

(deftest order-test
  (let [q (queue)]
    (update! q {:id 1 :priority 1})
    (update! q {:id 3 :priority 0})
    (update! q {:id 2 :priority 2})
    (update! q {:id 0 :priority 2})

    (is (= [3 1 0 2 nil]
           (->> (partial poll! q)
                repeatedly
                (take 5)
                (map :id))))))
