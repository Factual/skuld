;(ns skuld.clock-sync
;  "Exchanges heartbeats with peers to ensure clocks are synchronized."
;  (:require [skuld.net :as net]
;            [clj-helix.route :as route]))
;
;(defn clock-sync
;  "Creates a new clock-sync service."
;  [net router]
;  ; Register handler
;  (net/add-handler!
;    (fn [msg]
;      (when (= (:type msg) :clock-sync)
;        (prn "Clock sync msg received:" (:time msg)))))
;  
;  (let [running (promise)]
;    ; Periodically emit heartbeats to peers
;    (future
;      (loop []
;        (try
;          (doseq [peer (route/instances router :skuld :peer)]
;            (prn "Emitting clock-sync message to" peer)
;            (net/send! net peer {:type :clock-sync
;                                 :time (double
;                                         (/ (System/currentTimeMillis) 1000))}))
;          (catch Throwable t
;            (prn "clock-sync caught" t)))
;        
;        (when-not (deref running 1000 true)
;          (recur))))
;
;    running))
;
;(defn shutdown!
;  "Stop a clock-sync service."
;  [clock-sync]
;  (deliver clock-sync false))
