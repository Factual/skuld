(ns skuld.net-test
  (:use skuld.net
        clojure.test)
  (:import java.util.concurrent.CountDownLatch)
  (:require [skuld.flake :as flake]
            [clojure.set :as set]))

(flake/init!)

(deftest solipsist
  (let [node (node {:host "127.0.0.1" :port 13000})
        log  (atom [])
        msgs [{:x 1}
              {:x "foo"}
              {:x {:hi 2}}
              {:set #{:a "foo"} :vec [1/2 'yo]}
              {:done? true}]
        done (promise)]
    (try
      (add-handler! node (fn [msg]
                           (swap! log conj msg)
                           (when (:done? msg)
                             (deliver done true))
                           nil))
      (start! node)
      (doseq [m msgs] (send-sync! node node m))
      @done
      (is (= msgs @log))

      (finally
        (shutdown! node)))))

(deftest ring
  "Pass an integer around a ring."
  (let [n 2
        limit 1000
        nodes (->> (range n)
                   (map (fn [i] (node {:port (+ 13000 i)}))))
        x     (atom 0)
        done  (promise)]
    (try
      (->> nodes
           (map-indexed
             (fn [i node]
               (add-handler! node
                             (let [next-node (nth nodes (mod (inc i) n))]
                               (fn [msg]
                                 (assert (= @x msg))
                                 (if (<= limit @x)
                                   ; Done
                                   (deliver done true)
                                   ; Forward along
                                   (send! node next-node (swap! x inc)))
                                 nil)))))
           dorun)
      (dorun (pmap start! nodes))

      ; Start things rolling
      (send! (first nodes) (second nodes) @x)

      ; Wait
      @done

      (is (= limit @x))

      (finally (dorun (pmap shutdown! nodes))))))

(deftest echo-test
  "Sends a request and waits for an echo server to respond."
  (let [a (node {:port 13001})
        b (node {:port 13002})
        done (promise)]
    (try
      ; Make B an echo server.
      (add-handler! b identity)
      (start! a)
      (start! b)

      (req! a [b] {} {:echo? :echo!}
            [responses]
            (is (= 1 (count responses)))
            (deliver done true))

      (is (deref done 5000 false))

      (finally
        (shutdown! a)
        (shutdown! b)))))

(deftest scatter-test
  "Broadcasts requests to several nodes."
  (let [n 5
        nodes (->> (range n)
                   (map (fn [i] (node {:port (+ 13000 i)}))))
        responses  (promise)
        done       (CountDownLatch. n)]
    (try
      (doseq [node nodes]
        (add-handler! node (fn [msg]
                             (.countDown done)
                             {:i-am (:port node)})))
      (dorun (pmap start! nodes))

      (req! (first nodes) nodes {:r 3} {}
            [rs]
            (deliver responses rs))

      (or (deref responses 5000 false) (throw (RuntimeException. "stuck")))
      (is (<= 3 (count @responses)))
      (is (= (distinct @responses) @responses))
      (is (set/subset? (set (map :i-am @responses)) (set (map :port nodes))))

      (.await done)

      (finally
        (dorun (pmap shutdown! nodes))))))

(deftest timeout-test
  "Verifies that timeouts work correctly."
  (let [a (node {:port 13000})
        b (node {:port 13001})
        rs (promise)
        done (promise)]
    (try
      ; Here's a slow handler for B
      (add-handler! b (fn [x] (Thread/sleep 3000) (deliver done true) x))
      (dorun (pmap start! [a b]))

      (req! a [b] {:r 1 :timeout 1000} {:hi :there}
            [responses]
            (deliver rs responses))
      
      (is (= [] @rs))
      @done
      (Thread/sleep 100)

      (finally
        (dorun (pmap shutdown! [a b]))))))
