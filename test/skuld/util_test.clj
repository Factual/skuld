(ns skuld.util-test
  (:use skuld.util
        clojure.test))

(deftest sorted-interleave-test
  (is (= [1 2 3 4 5 6 7]
         (sorted-interleave [1 5]
                            []
                            [2 4 6 7]
                            nil
                            [3])))
  (testing "infinite"
    (is (= [1 1 2 2 3 3 4 5 6 7]
           (->> (iterate inc 1)
                (sorted-interleave [1 2 3])
                (take 10)))))

  (testing "bench"
    (time (dorun (sorted-interleave (take 1000000 (iterate inc 0))
                                    (take 1000000 (iterate inc 1))
                                    (range 500 1000000))))))
