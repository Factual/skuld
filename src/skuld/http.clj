(ns skuld.http
  (:require [clout.core :refer [route-compile route-matches]]
            [clojure.tools.logging :refer :all]
            [ring.adapter.jetty :refer [run-jetty]]
            [skuld.node :as node])
  (:import org.eclipse.jetty.server.Server))

(defn- http-response
  "Given a status and body and optionally a headers map, returns a ring
  response."
  [status body & [headers]]
  {:status status
   :headers (or headers {})
   :body body})

(defn- endpoint
  "Defines an HTTP endpoint with an allowed request method. Takes an allowed
  method, a request map, and the response body."
  [allowed-method req resp-body]
  (if (= (:request-method req) allowed-method)
    (http-response 200 resp-body)
    (http-response 405 "Method Not Allowed")))

(def ^:private GET (partial endpoint :get))

(defn- make-handler
  "Given a node, constructs the handler function. Returns a response map."
  [node]
  (fn [req]
    (condp route-matches req
      "/count_queue" (GET req (pr-str (node/count-queue node {})))
      "/count_tasks" (GET req (pr-str (node/count-tasks node {})))
      "/list_tasks"  (GET req (pr-str (node/list-tasks node {})))
      (http-response 404 "Not Found"))))

(defn service
  "Given a node and port, constructs a Jetty instance."
  [node port]
  (info "Starting HTTP server on" (str (:host node) ":" port))
  (let [handler (make-handler node)
        jetty   (run-jetty handler {:host (:host node)
                                    :port port
                                    :join? false})]
    jetty))

(defn shutdown!
  "Stops a given Jetty instance."
  [^Server jetty]
  (.stop jetty))
