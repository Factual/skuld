(ns skuld.claim-test
  (:use [clj-helix.logging :only [mute]]
        clojure.tools.logging
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
            clj-helix.admin)
  (:import com.aphyr.skuld.Bytes))

(flake/init!)

(use-fixtures :once once)
(use-fixtures :each each)

(deftest reclaim-test
  (with-redefs [task/clock-skew-buffer 50]
    (let [id (client/enqueue! *client* {:w 3} {:data "maus"})]
      (elect! *nodes*)
      (is (= id (:id (client/claim! *client* 100))))
      
      ; Can't reclaim, because it's already claimed
      (is (nil? (client/claim! *client* 100)))
      
      ; Can't reclaim after 100ms because clock skew buffer still holds
      (Thread/sleep 101)
      (is (nil? (client/claim! *client* 100)))
      
      ; But after the buffer has elapsed, good to go. 
      (Thread/sleep 50)
      (let [t (client/claim! *client* 100)]
        (is (= id (:id t)))
        (is (= 2 (count (:claims t))))
        (is (= "maus" (:data t)))))))

(deftest complete-test
  (with-redefs [task/clock-skew-buffer 0]
    (elect! *nodes*)
    (let [id (client/enqueue! *client* {:data "sup"})]
      (is (client/claim! *client* 1))

      ; Isn't completed
      (is (not (task/completed? (client/get-task *client* id))))

      (client/complete! *client* id 0)

      ; Is completed
      (is (task/completed? (client/get-task *client* id)))

      ; Can't re-claim.
      (Thread/sleep 2)
      (is (nil? (client/claim! *client* 100))))))
