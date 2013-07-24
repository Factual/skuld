(ns skuld.node
  "A single node in the Skuld cluster. Manages any number of vnodes."
  (:require [skuld.vnode :as vnode]
            [skuld.net :as net]
;            [skuld.clock-sync :as clock-sync]
            [clj-helix.manager :as helix]
            clj-helix.fsm
            clj-helix.route))

(def fsm-def (clj-helix.fsm/fsm-definition
               {:name   :skuld
                :states {:offline {:initial? true
                                   :transitions :peer}
                         :peer {:priority 1
                                :upper-bound :R
                                :transitions [:DROPPED :offline]}}}))

(defn node
  "Creates a new node with the given options.

  :zookeeper    \"localhost:2181\"
  :cluster      :skuld
  :host         \"127.0.0.1\"
  :port         13000"
  [opts]
  (let [zk      (get opts :zookeeper "localhost:2181")
        host    (get opts :host "127.0.0.1")
        port    (get opts :port 13000)
        cluster (get opts :cluster :skuld)
        vnodes  (atom {})
        fsm     (clj-helix.fsm/fsm
                  fsm-def
                  (:offline :peer [part m c]
                            (locking *out*
                              (prn part "Coming online; silvering"))
                            (dotimes [i 5]
                              (Thread/sleep 1000)
                              (locking *out*
                                (print ".") (flush)))
                           (locking *out*
                             (println "\n" part "ready")))


                  (:peer :offline [part m c]
                            (locking *out*
                              (prn part "Offline"))))

;        controller  (helix/controller {:zookeeper zk
;                                       :cluster cluster
;                                       :instance {:host host :port port}})
        participant (helix/participant {:zookeeper zk
                                        :cluster cluster
                                        :instance {:host host :port port}
                                        :fsm fsm})
        router (clj-helix.route/router! participant)

        net (net/node {:host host
                       :port port})]

;        clock-sync (clock-sync/clock-sync net router)]

    ; Start network node
    (net/start! net)

    {:host host
     :port port
     :net net
     :router router
;     :clock-sync clock-sync
     :participant participant
     :controller nil
     :vnodes vnodes}))

(defn controller
  "Creates a new controller, with the given options.
  
  :zookeeper    \"localhost:2181\"
  :cluster      :skuld
  :host         \"127.0.0.1\"
  :port         13000"
  [opts]
  (let [zk      (get opts :zookeeper "localhost:2181")
        host    (get opts :host "127.0.0.1")
        port    (get opts :port 13000)
        cluster (get opts :cluster :skuld)]
    {:host host
     :port port
     :zookeeper zk
     :controller (helix/controller {:zookeeper zk
                                    :cluster cluster
                                    :instance {:host host :port port}})}))

(defn shutdown!
  "Shuts down a node."
  [node]
;  (when-let [c (:clock-sync node)]
;    (clock-sync/shutdown! c))

  (->> (select-keys node [:participant :controller])
       vals
       (remove nil?)
       (map helix/disconnect!)
       dorun))
