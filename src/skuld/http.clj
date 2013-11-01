(ns skuld.http
  (:require [clout.core :refer [route-compile route-matches]]
            [clojure.tools.logging :refer :all]
            [ring.adapter.jetty :refer [run-jetty]]
            [skuld.node :as node])
  (:import org.eclipse.jetty.server.Server))

(defn make-handler
  [node]
  (fn [req]
    (condp route-matches req
      "/list_tasks" (node/list-tasks node {}))))

(defn service
  [node port]
  (let [handler (make-handler node)
        jetty   (run-jetty (fn [] {}) {:host (:host node)
                                       :port port
                                       :join? false})]
    jetty))

(defn shutdown!
  [^Server jetty]
  (.stop jetty))
