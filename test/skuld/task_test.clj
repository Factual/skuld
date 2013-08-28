(ns skuld.task-test
  (:refer-clojure :exclude [merge])
  (:use skuld.task
        clojure.test)
  (:require [skuld.flake :as flake]))

(flake/init!)

(deftest mergev-test
  (are [a b] (= (apply mergev a) b)
       [] []
       [[]] []
       [[1 2 3]] [1 2 3]
       [nil [1 2 3]] [1 2 3]
       [[4 5 6] [1 2 nil]] [1 2 6]
       [[1] [nil 2]] [1 2]
       [[nil 2] [1 nil 3]] [1 2 3]
       [[nil 1] [nil 2] [nil 3]] [nil 3]
       ))

(deftest merge-claim-test
  (testing "empty"
    (is (= (merge-claims [])
           [])))

  (testing "one claim set"
    (is (= (merge-claims [[{:start 1 :end 2} nil {:start 3 :end 5}]])
                          [{:start 1 :end 2} nil {:start 3 :end 5}])))
  
  (testing "several claim sets"
    (is (= (->> (merge-claims
                  [[{:start 0 :end 2} nil               {:start 6 :end 9}]
                   [nil               nil                                ]
                   [nil               {:start 3 :end 5}                  ]
                   [{:start 1 :end 3} {:start 4 :end 5} nil              ]])
                (map #(select-keys % [:start :end])))
           [{:start 0 :end 3} {:start 3 :end 5} {:start 6 :end 9}]))))

(deftest merge-test
  (testing "empty"
    (is (= (merge)
           {:claims []})))

  (testing "one"
    (let [t (task {:data :hi})]
      (is (= (merge t) t))))

  (testing "more"
    (let [t (task {:data :hi})]
      (is (= (merge (assoc t :claims [{:start 0 :end 1}])
                    (assoc t :claims [nil nil {:start 2 :end 4}]))
             (assoc t :claims [{:start 0 :end 1}
                                nil
                                {:start 2 :end 4}])))))

  (testing "completed"
    (let [t (task {:data :hi})]
      (is (= (merge (assoc t :claims [{:start 0 :end 1 :completed 100}])
                    (assoc t :claims [{:start 2 :end 4 :completed 50}]))
             (assoc t :claims [{:start 0 :end 4 :completed 50}]))))))

(deftest claim-test
  (let [t (claim (task {:data :hi}) 10)]
    (is (claimed? t)))

  (with-redefs [clock-skew-buffer 0]
    (let [t (claim (task {:data :hi}) 0)]
      (Thread/sleep 1)
      (is (not (claimed? t))))))

(deftest complete-test
  (let [t (claim (task {:data :meow}) 10)]
    (is (not (completed? t)))
    (let [t' (complete t 0 50)]
      (testing "is completed"
        (is (completed? t'))
        (is (= 50 (:completed (first (:claims t'))))))

      (testing "is not claimable"
        (is (thrown? IllegalStateException
                     (claim t' 10)))))))
