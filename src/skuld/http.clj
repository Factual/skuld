(ns skuld.http
  (:require [cheshire.core :as json]
            [cheshire.generate :refer [add-encoder encode-str]]
            [clout.core :refer [route-compile route-matches]]
            [clojure.data.codec.base64 :as b64]
            [clojure.tools.logging :refer :all]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.middleware.json :refer [wrap-json-body]]
            [skuld.node :as node])
  (:import [com.aphyr.skuld Bytes]
           [com.fasterxml.jackson.core JsonGenerator JsonParseException]
           [org.eclipse.jetty.server Server]))

(defn- encode-bytes
  "Encode a bytes to the json generator."
  [^Bytes b ^JsonGenerator jg]
  (.writeString jg (-> b .bytes b64/encode String.)))

;; Custom Cheshire encoder for the Bytes type
(add-encoder Bytes encode-bytes)

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
  "Given a request map and a response body, serializes the response body first
  based on `Accept` header and then falling back to JSON. Currently only
  serializes to JSON. Returns the body `Content-Type` header in a vector."
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
(def ^:private POST (partial endpoint :post))

(defn- b64->id
  "Coerces a base64-encoded id into a Bytes type."
  [b64-id]
  (-> b64-id .getBytes b64/decode Bytes.))

(defn- make-handler
  "Given a node, constructs the handler function. Returns a response map."
  [node]
  (fn [req]
    (condp route-matches req
      "/queue/count"        (GET req (node/count-queue node {}))

      ;; TODO: Make sure we return something meaningful to the client
      "/tasks/claim/:id"    :>> (fn [{:keys [id]}]
                                  (let [msg {:id (b64->id id)}
                                        ret (node/claim! node msg)
                                        cnt (-> ret :task :claims count dec)]
                                    (GET req {:claim-id cnt})))

      ;; TODO: Pass in `claim-id` value
      "/tasks/complete/:id" :>> (fn [{:keys [id]}]
                                  (let [id  (b64->id id)
                                        msg {:task-id id
                                             :claim-id (Integer/parseInt "0")}
                                        ret (node/complete! node msg)]
                                    (GET req (dissoc ret :responses))))
      "/tasks/count"        (GET req (node/count-tasks node {}))

      ;; TODO: The `/tasks/enqueue` endpoint is pretty messy currently
      "/tasks/enqueue"      (let [;; Explicitly suck out the task key to avoid
                                  ;; passing bad params to `node/enqueue!`
                                  msg {:task (-> req :body :task)}]
                              (try (let [ret (node/enqueue! node msg)]
                                     (POST req (dissoc ret :responses)))
                                ;; Handle vnode assertion; return an error to
                                ;; the client
                                (catch java.lang.AssertionError e
                                  (POST req {:error (.getMessage e)}))))
      "/tasks/list"         (GET req (node/list-tasks node {}))

      ;; TODO: Pass in `r` value
      "/tasks/:id"          :>> (fn [{:keys [id]}]
                                  (let [msg {:id (b64->id id)}
                                        ret (node/get-task node msg)]
                                    (GET req (dissoc ret :responses))))
      "/request-vote"       (let [part (-> req :body :partition)
                                  msg {:partition part}]
                              (POST req (node/request-vote! node msg)))
      "/wipe"               (GET req (node/wipe! node {}))
      not-found)))

(defn- wrap-json-body-safe
  "A wrapper for `wrap-json-body` which catches JSON parsing exceptions and
  returns a Bad Request."
  [handler & [opts]]
  (let [request-handler (wrap-json-body handler opts)]
    (fn [request]
      (try (request-handler request)
        (catch JsonParseException e
          (handler request)  ;; resolve request before generating a response
          (http-response 400 "Bad Request"))))))

(defn service
  "Given a node and port, constructs a Jetty instance."
  [node port]
  (info "Starting HTTP server on" (str (:host node) ":" port))
  (let [handler (->
                  (make-handler node)
                  (wrap-json-body-safe {:keywords? true}))
        jetty   (run-jetty handler {:host (:host node)
                                    :port port
                                    :join? false})]
    jetty))

(defn shutdown!
  "Stops a given Jetty instance."
  [^Server jetty]
  (.stop jetty))
