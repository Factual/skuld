(ns skuld.http
  (:require [cheshire.core :as json]
            [cheshire.generate :refer [add-encoder encode-str]]
            [clout.core :refer [route-compile route-matches]]
            [clojure.tools.logging :refer :all]
            [ring.adapter.jetty :refer [run-jetty]]
            [skuld.node :as node])
  (:import [com.aphyr.skuld Bytes]
           [org.eclipse.jetty.server Server]))

;; Custom Cheshire encoder for the Bytes type
(add-encoder Bytes encode-str)

(defn- unhexify [hex]
  (apply str
    (map
      (fn [[x y]] (char (Integer/parseInt (str x y) 16)))
        (partition 2 hex))))

(defn- http-response
  "Given a status and body and optionally a headers map, returns a ring
  response."
  [status body & [headers]]
  {:status status
   :headers (or headers {})
   :body body})

(def ^:private ok-response (partial http-response 200))
(def ^:static ^:private not-found (http-response 404 "Not Found"))

(defn- serialize
  [req resp-body]
  (let [accept (get-in req [:headers "Accept"] "application/json")
        json-ct {"Content-Type" "application/json;charset=utf-8"}]
    (condp re-find accept
      #"^application/(vnd.+)?json" [(json/generate-string resp-body) json-ct]
      [(json/generate-string resp-body) json-ct])))

(defn- endpoint
  "Defines an HTTP endpoint with an allowed request method. Takes an allowed
  method, a request map, and the response body."
  [allowed-method req resp-body]
  (if (= (:request-method req) allowed-method)
    (if resp-body
      (apply ok-response (serialize req resp-body))
      not-found)
    (http-response 405 "Method Not Allowed")))

(def ^:private GET (partial endpoint :get))

(defn- make-handler
  "Given a node, constructs the handler function. Returns a response map."
  [node]
  (fn [req]
    (condp route-matches req
      "/queue/count" (GET req (node/count-queue node {}))
      "/tasks/count" (GET req (node/count-tasks node {}))
      "/tasks/list"  (GET req (node/list-tasks node {}))
      "/tasks/:id"   :>> (fn [params]
                           (let [id (-> params :id unhexify .getBytes Bytes.)
                                 msg {:id id}
                                 ret (node/get-task node msg)]
                             (GET req (dissoc ret :responses))))
      not-found)))

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
