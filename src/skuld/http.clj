(ns skuld.http
  (:require [clout.core :refer [route-compile route-matches]]
            [clojure.tools.logging :refer :all]
            [ring.adapter.jetty :refer [run-jetty]]
            [skuld.node :as node]))



(defn make-handler
  [node]
  (fn [req]
    (condp route-matches req
      "/list_tasks" (node/list-tasks node {}))))

(defn service
  [node port]
  (let [handler (make-handler node)]
    (run-jetty handler {:host (:host node) :port port})))

(defn shutdown!
  [jetty]
  (.doStop jetty))
