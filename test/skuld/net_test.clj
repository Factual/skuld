(ns skuld.net-test
  (:use skuld.net
        clojure.test))

(deftest solipsist
  (let [node (node {:host "127.0.0.1" :port 13000})
        log  (atom [])
        msgs [1 2 3 {:hi 2} #{:a "foo" [1/2 'yo]} :done]
        done (promise)]
    (try
      (add-handler! node (fn [msg]
                           (swap! log conj msg)
                           (when (= :done msg)
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
