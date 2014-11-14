(ns skuld.clock-sync
  "Exchanges heartbeats with peers to ensure clocks are synchronized."
  (:require [skuld.net :as net]
            [skuld.flake :as flake]
            [clj-helix.route :as route]
            [clojure.tools.logging :refer [warn warnf]]))

(defn service
  "Creates a new clock-sync service."
  [net router vnodes]
  ; Register handler
  (net/add-handler! net
                    (fn [msg]
                      (when (= (:type msg) :clock-sync)
                        (let [delta (- (flake/linear-time) (:time msg))]
                          (when (< 30000 delta)
                            (warn "Clock skew with"
                                  (pr-str (:node msg))
                                  "is"
                                  delta
                                  "milliseconds!")))
                        {})))

  (let [running (promise)]
    ; Periodically emit heartbeats to peers
    (future
      (when (deref running 10000 true)
        (loop []
          (try
            (->> vnodes
                 deref
                 keys
                 (mapcat #(route/instances router :skuld % :peer))
                 set
                 (map (fn [peer]
                        (try
                          (net/send! net peer {:type :clock-sync
                                               :node (select-keys net [:host :port])
                                               :time (flake/linear-time)})
                          (catch io.netty.channel.ChannelException ex
                            (warnf "clock-sync to %s: %s" peer ex))
                          (catch Throwable t
                            (warn t "clock-sync to " peer)))))
                 dorun)
            (catch Throwable t
              (warn t "clock-sync caught")))

          (when (deref running 10000 true)
            (recur)))))

    running))

(defn shutdown!
  "Stop a clock-sync service."
  [clock-sync]
  (deliver clock-sync false))
