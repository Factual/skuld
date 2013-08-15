(ns skuld.task-test
  (:refer-clojure :exclude [merge])
  (:use skuld.task
        clojure.test)
  (:require [skuld.flake :as flake]))

(flake/init!)

(deftest merge-claim-test
  (testing "empty"
    (is (= (merge-claims [])
           [])))

  (testing "one claim set"
    (is (= (merge-claims [[{:start 1 :end 2} nil {:start 3 :end 5}]])
                          [{:start 1 :end 2} nil {:start 3 :end 5}])))
  
  (testing "several claim sets"
    (is (= (merge-claims
             [[{:start 0 :end 2} nil               {:start 6 :end 9}]
              [nil               nil                                ]
              [nil               {:start 3 :end 5}                  ]
              [{:start 1 :end 3} {:start 4 :end 5} nil              ]])

              [{:start 0 :end 3} {:start 3 :end 5} {:start 6 :end 9}]))))

(deftest merge-test
  (testing "empty"
    (is (= (merge)
           {:claims []})))

  (testing "one"
    (let [t (task :hi)]
      (is (= (merge t) t))))

  (testing "more"
    (let [t (task :hi)]
      (is (= (merge (assoc t :claims [{:start 0 :end 1}])
                    (assoc t :claims [nil nil {:start 2 :end 4}]))
             (assoc t :claims [{:start 0 :end 1}
                                nil
                                {:start 2 :end 4}]))))))
