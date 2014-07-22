(ns skuld.task-test
  (:refer-clojure :exclude [merge])
  (:use skuld.task
        clojure.test)
  (:require skuld.flake-test
            [skuld.flake :as flake]))


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
             (assoc t :claims [{:start 0 :end 4 :completed 50}])))))

  (testing "completed without start"
    (let [t (task {:data :hi})]
      (is (= (merge (assoc t :claims [{:start 0 :end 1 :completed 100}])
                    (assoc t :claims [{:start 2 :end 4}])
                    (assoc t :claims [{:completed 50}]))
             (assoc t :claims [{:start 0 :end 4 :completed 50}]))))))


(deftest claim-test
  (let [t (claim (task {:data :hi}) 10)]
    (is (claimed? t)))

  (with-redefs [clock-skew-buffer 0]
    (let [t (claim (task {:data :hi}) 0)]
      (Thread/sleep 1)
      (is (not (claimed? t))))))

(deftest request-claim-test
  (testing "nil"
    (is (thrown? IllegalStateException
                 (request-claim nil 0 {:start 0 :end 1}))))

  (testing "completed"
    (is (thrown? IllegalStateException
                 (-> (task {:data :cat})
                     (claim 10)
                     (complete 0 0)
                     (request-claim 0 {:start 0 :end 1})))))

  (testing "conflicting index"
    (is (thrown? IllegalStateException
                 (with-redefs [clock-skew-buffer 0]
                   (-> (task {:data :kitten})
                       (request-claim 0 {:start 0 :end 10})
                       (request-claim 0 {:start 10000000 :end 20000000}))))))

  (testing "conflicting claim"
    (is (thrown? IllegalStateException
                 (with-redefs [clock-skew-buffer 0]
                   (-> (task {:data :kit})
                       (request-claim 0 {:start 0 :end 10})
                       (request-claim 1 {:start 9 :end 11}))))))
    
    (is (thrown? IllegalStateException
                 (let [t (flake/linear-time)]
                   (with-redefs [clock-skew-buffer 10]
                     (-> (task {:data :kit})
                         (request-claim 0 {:start 0 :end 10})
                         (request-claim 1 {:start 19 :end 29}))))))

  (testing "valid claim"
    (is (with-redefs [clock-skew-buffer 0]
          (-> (task {:data :treat})
              (request-claim 0 {:start 0 :end 10})
              (request-claim 1 {:start 11 :end 21})
              :claims
              (nth 1)
              (= {:start 11 :end 21}))))

    (is (with-redefs [clock-skew-buffer 10]
          (-> (task {:data :treat})
              (request-claim 0 {:start 0 :end 10})
              (request-claim 1 {:start 21 :end 31})
              :claims
              (nth 1)
              (= {:start 21 :end 31})))))

  (testing "sparse claim"
    (is (-> (task {:data :treat})
            (request-claim 10 {:start 0 :end 10})
            :claims
            (nth 10)
            (= {:start 0 :end 10})))))

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
