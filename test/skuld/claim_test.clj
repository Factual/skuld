(ns skuld.claim-test
  (:use clojure.tools.logging
        clojure.test
        skuld.util
        skuld.node
        skuld.node-test)

  (:require [skuld.client  :as client]
            [skuld.admin   :as admin]
            [skuld.vnode   :as vnode]
            [skuld.flake   :as flake]
            [skuld.curator :as curator]
            [skuld.net     :as net]
            [skuld.task    :as task]
            [skuld.aae     :as aae]
            [clojure.set   :as set]
            [skuld.logging :as logging]
            clj-helix.admin)
  (:import com.aphyr.skuld.Bytes))

(use-fixtures :once once)
(use-fixtures :each each)


(deftest claim-test
  (elect! *nodes*)
  (let [id (client/enqueue! *client* {:w 3} {:queue "queue1" :data "hi"})]
    (is id)
    (let [task (client/claim! *client* "queue1" 1000)]
      (is (= id (:id task)))
      (is (task/claimed? task)))))

(deftest reclaim-test
  (elect! *nodes*)
  (with-redefs [task/clock-skew-buffer 500]
    (let [id (client/enqueue! *client* {:w 3} {:queue "queue2" :data "maus"})]
      (is (= id (:id (client/claim! *client* "queue2" 1000))))
      
      ; Can't reclaim, because it's already claimed
      (is (nil? (client/claim! *client* "queue2" 1000)))
      
      ; Can't reclaim after 1000ms because clock skew buffer still holds
      (Thread/sleep 1001)
      (is (nil? (client/claim! *client* "queue2" 1000)))
      
      ; But after the buffer has elapsed, good to go. 
      (Thread/sleep 1500)

      (let [t (client/claim! *client* "queue2" 1000)]
        (is (= id (:id t)))
        (is (= 2 (count (:claims t))))
        (is (= "maus" (:data t)))))))

(deftest complete-test
  (elect! *nodes*)
  (with-redefs [task/clock-skew-buffer 0]
    (let [id (client/enqueue! *client* {:queue "queue3" :data "sup"})]
      (is (client/claim! *client* "queue3" 1))

      ; Isn't completed
      (is (not (task/completed? (client/get-task *client* id))))

      (client/complete! *client* id 0)

      ; Is completed
      (is (task/completed? (client/get-task *client* id)))

      ; Can't re-claim.
      (Thread/sleep 2)
      (is (nil? (client/claim! *client* "queue3" 100))))))
