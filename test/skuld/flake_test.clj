(ns skuld.flake-test
  (:use skuld.flake
        clojure.test
        [criterium.core :only [bench]])
  (:require [clojure.data.codec.base64 :as base64]))

(init!)

(deftest linear-time-test
  (dotimes [i 10]
    (Thread/sleep 1)
    (is (>= 1 (Math/abs (- (System/currentTimeMillis)
                           (linear-time)))))))

(deftest id-test
  (let [ids (take 1000 (repeatedly id))]
    (is (= ids (sort ids)))
    (is (= ids (distinct ids)))))

(deftest ^:bench perf-test
  (bench (id)))
