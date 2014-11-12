(ns skuld.db.level-test
  (:require [skuld.db.level :refer :all]
            [skuld.db :refer :all]
            [skuld.flake-test :as flake]
            [skuld.task :as task]
            [clojure.test :refer :all]))

(def ^:dynamic *db*)

(use-fixtures :each (fn [f]
                      (binding [*db* (open {:host "localhost"
                                            :port 0
                                            :partition "skuld-0"})]
                        (try
                          (wipe! *db*)
                          (f)
                          (finally
                            (close! *db*))))))

(deftest exists-test
  (let [vnode {:host "localhost" :port 0 :partition "skuld-1" :ext "test"}]
    (is (not (local-data? vnode)))
    (path! vnode)
    (is (local-data? vnode))
    (destroy-data! vnode)
    (is (not (local-data? vnode)))))

(deftest merge-test
  (let [t (task/task {:data "meow"})]
    (merge-task! *db* t)
    (is (= t (get-task *db* (:id t))))

    (let [t  (task/task {:data "mause"})
          t' (task/claim t 123)]
      (merge-task! *db* t')
      (is (= t' (get-task *db* (:id t))))

      (merge-task! *db* t)
      (is (= t' (get-task *db* (:id t)))))))

(deftest count-test
  (is (= 0 (count-tasks *db*)))
  (dotimes [i 1000]
    (merge-task! *db* (task/task {})))
  (is (= 1000 (count-tasks *db*)))
  (wipe! *db*)
  (is (= 0 (count-tasks *db*))))

(deftest tasks-test
  (let [ts (->> (fn [] {:data (rand)})
                repeatedly
                (map task/task)
                (take 100)
                doall
                shuffle)]
    (doseq [t ts]
      (merge-task! *db* t))
    (is (= (sort-by :id ts)
           (tasks *db*)))))
