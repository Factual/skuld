(ns skuld.flake-test
  (:use skuld.flake
        clojure.test
        [criterium.core :only [quick-bench]])
  (:require [clojure.data.codec.base64 :as base64])
  (:import (java.util Arrays)
           (com.google.common.primitives UnsignedBytes)))

(in-ns 'skuld.flake)
(def node-id (constantly (byte-array 6)))
(in-ns 'skuld.flake-test)

(init!)

(deftest linear-time-test
  (dotimes [i 10]
    (Thread/sleep 1)
    (is (>= 1 (Math/abs ^long (- (System/currentTimeMillis)
                                 (linear-time)))))))

(deftest id-test
  (let [ids (->> (repeatedly id)
                 (take 10000))]
    (is (= ids (sort (UnsignedBytes/lexicographicalComparator) ids)))
    (is (= ids (distinct ids)))))

(deftest node-test
  (is (Arrays/equals ^bytes (node-fragment) ^bytes (node-fragment))))

(deftest ^:bench perf-test
  (quick-bench (id)))
