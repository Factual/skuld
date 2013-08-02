(ns skuld.bin
  (:use [clj-helix.logging :only [mute]]
        [clojure.tools.cli :only [cli]])
  (:require [skuld.admin :as admin]
            [skuld.node :as node]
            [skuld.flake :as flake])
  (:import (sun.misc Signal SignalHandler))
  (:gen-class))

(defn parse-int
  "Parse an integer."
  [^String s]
  (Integer. s))

(defmacro signal
  "Adds a signal handler."
  [signal & body]
  `(when-not (.contains (System/getProperty "os.name") "Windows")
     (Signal/handle
       (Signal. (name ~signal))
       (reify SignalHandler
         (handle [this# sig#]
           ~@body)))))

(def admin-spec
  [["-z" "--zookeeper" "Zookeeper connection string"
     :default "localhost:2181"]
    ["-c" "--cluster" "Cluster name"
     :default "skuld"]
    ["-partitions" "--partitions" "Number of partitions"
     :default 1 :parse-fn parse-int]
    ["-r" "--replicas"   "Number of replicas"
     :default 3 :parse-fn parse-int]])

(def node-spec
  [["-p" "--port" "Port"     :default "13000"     :parse-fn parse-int]
   ["-h" "--host" "Hostname" :default "127.0.0.1"]])

; Cluster configuration
(defn cluster-create [& args]
  (let [[opts _ _] (apply cli args admin-spec)]
    (admin/create-cluster! (admin/admin opts))))

(defn cluster-destroy [& args]
  (let [[opts _ _] (apply cli args admin-spec)]
    (admin/destroy-cluster! (admin/admin opts))))

(defn cluster-add [& args]
  (let [[opts _ _] (apply cli args (concat node-spec admin-spec))]
    (admin/add-node! (admin/admin opts)
                     (select-keys opts [:host :port]))))

(defn cluster
  [cmd & args]
  (apply (case cmd
           "create"  cluster-create
           "add"     cluster-add
           "destroy" cluster-destroy)
         args))

; Node management
(defn controller [& args]
  (let [[opts _ _] (apply cli args node-spec)
        controller (node/controller opts)]
      
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (bound-fn []
                                 (node/shutdown! controller))))

    (println "Controller started.")
    (prn controller)
    @(promise)))

(defn start [& args]
  (flake/init!)
  (let [[opts _ _]  (apply cli args node-spec)
        node        (node/node opts)]

    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (bound-fn []
                                 (node/shutdown! node))))

    (prn :started node)
    @(promise)))

(defn -main
  [cmd & args]
  (try
      (apply (case cmd
               "cluster" cluster
               "controller" controller
               "start" start)
             args)
    (catch Throwable t
      (.printStackTrace t)
      (System/exit 1))))
