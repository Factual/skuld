(ns skuld.queue-test
  (:require [skuld.queue :refer :all]
            [skuld.task :as task]
            [clojure.test :refer :all]))

(skuld.flake/init!)

(deftest enqueue-test
  (let [q (queues)]
    (is (zero? (count-queue q "one")))

    (update! q {:queue "one" :id 0 :foo :bar})
    (is (= 1 (count-queue q "one")))

    (update! q {:queue "one" :id 0 :bar :baz})
    (is (= 1 (count-queue q "one")))

    (is (= (->Task 0 nil) (poll! q "one")))
    (is (= nil (poll! q "one")))))

(deftest order-test
  (let [q (queues)]
    (update! q {:queue "two" :id 1 :priority 1})
    (update! q {:queue "one" :id 1 :priority 1})
    (update! q {:queue "one" :id 3 :priority 0})
    (update! q {:queue "one" :id 2 :priority 2})
    (update! q {:queue "one" :id 0 :priority 2})

    (is (= [3 1 0 2 nil]
           (->> (partial poll! q "one")
                repeatedly
                (take 5)
                (map :id))))))
