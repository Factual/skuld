(ns skuld.http
  "An HTTP interface to a Skuld node."
  (:require [cheshire.core             :as json]
            [cheshire.generate         :refer [add-encoder encode-str]]
            [clout.core                :refer [route-compile route-matches]]
            [clojure.data.codec.base64 :as b64]
            [clojure.tools.logging     :refer :all]
            [clojure.walk              :refer [keywordize-keys]]
            [ring.adapter.jetty        :refer [run-jetty]]
            [ring.middleware.json      :refer [wrap-json-body]]
            [ring.util.codec           :refer [form-decode]]
            [skuld.node                :as node])
  (:import [com.aphyr.skuld Bytes]
           [com.fasterxml.jackson.core JsonGenerator JsonParseException]
           [org.eclipse.jetty.server Server]))

(defn- encode-bytes
  "Encode a bytes to the json generator."
  [^Bytes b ^JsonGenerator jg]
  (.writeString jg (-> ^Bytes b .bytes b64/encode String.)))

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
(def ^:private bad-request (partial http-response 400))
(def ^:private not-found (partial http-response 404))

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
  [allowed-method req resp-body & [http-resp-fn]]
  (let [http-resp (or http-resp-fn ok-response)]
    (if (= (:request-method req) allowed-method)
      (if resp-body
        (apply http-resp (serialize req resp-body))
        (not-found "Not Found"))
      (http-response 405 "Method Not Allowed"))))

(def ^:private GET (partial endpoint :get))
(def ^:private POST (partial endpoint :post))

(defn- b64->id
  "Coerces a base64-encoded id into a Bytes type."
  [^String b64-id]
  (-> b64-id (.replaceAll "-" "+") (.replaceAll "_" "/") .getBytes b64/decode Bytes.))

(defn- b64->bytes
  "Coerce a base64-encoded value into a Bytes type."
  [^String b64-str]
  (-> b64-str .getBytes b64/decode Bytes.))

(defn- parse-int
  "Safely coerces a string into an integer. If the conversion is impossible,
  returns a fallback value if provided or nil."
  [s & [fallback]]
  (try (Integer/parseInt s)
    (catch Exception e
      fallback)))

(defn- count-queue
  "Like `node/count-queue`, but wrapped around an HTTP request."
  [node req]
  (let [r     (-> req :query-params :r parse-int)
        queue (-> req :query-params :queue)]
    (GET req (node/count-queue node {:r     r
                                     :queue queue}))))

(defn- claim!
  "Like `node/claim!`, but wrapped around an HTTP request."
  [node req]
  (let [dt    (-> req :body :dt)
        queue (-> req :body :queue)
        ret (node/claim! node {:queue queue
                               :dt    dt})]
    (POST req (dissoc ret :request-id))))

(defn- complete!
  "Like `node/complete!`, but wrapped around an HTTP request."
  [node req id]
  (let [id  (b64->id id)
        cid (-> req :body :cid)
        w   (-> req :query-params :w parse-int)
        msg {:task-id id :claim-id cid :w w}
        ret (node/complete! node msg)]
    (POST req (dissoc ret :responses))))

(defn- update!
  "Like `node/update!`, but wrapped around an HTTP request."
  [node req id]
  (let [id  (b64->id id)
        cid (-> req :body :cid)
        lid (-> req :body :lid)
        msg (-> req :body :msg b64->bytes)
        w   (-> req :query-params :w parse-int)
        msg {:task-id id :claim-id cid :log-id lid :message msg :w w}
        ret (node/update! node msg)]
    (POST req (dissoc ret :responses))))

(defn- count-tasks
  "Like `node/count-tasks`, but wrapped around an HTTP request."
  [node req]
  (GET req (node/count-tasks node {})))

(defn- enqueue!
  "Like `node/enqueue!`, but wrapped around an HTTP request."
  [node req]
  (if-let [;; Explicitly suck out the task key to avoid passing bad params to
           ;; `node/enqueue!`
           task (-> req :body :task)]
    (try (let [w   (-> req :body :w)
               msg {:task task :w w}
               ret (node/enqueue! node msg)]
           (POST req (dissoc ret :responses)))
         ;; Handle vnode assertion; return an error to
         ;; the client
         (catch java.lang.AssertionError e
           (let [err {:error (.getMessage e)}]
             (POST req err bad-request))))
              ;; Missing parameters, i.e. POST body
              (let [err {:error "Missing required params"}]
                (POST req err bad-request))))

(defn- list-tasks
  "Like `node/list-tasks`, but wrapped around an HTTP request."
  [node req]
  (GET req (node/list-tasks node {})))

(defn- get-task
  "Like `node/get-task`, but wrapped around an HTTP request."
  [node req id]
  (let [r   (-> req :query-params :r parse-int)
        msg {:id (b64->id id) :r r}
        ret (node/get-task node msg)]
    (if-not (-> ret :task :id)
      (GET req {:error "No such task"} not-found)
      (GET req (dissoc ret :responses)))))

(defn- make-handler
  "Given a node, constructs the handler function. Returns a response map."
  [node]
  (fn [req]
    (condp route-matches req
      "/queue/count"        (count-queue node req)
      "/tasks/claim"        (claim! node req)
      "/tasks/update/:id"   :>> (fn [{:keys [id]}] (update! node req id))
      "/tasks/complete/:id" :>> (fn [{:keys [id]}] (complete! node req id))
      "/tasks/count"        (count-tasks node req)
      "/tasks/enqueue"      (enqueue! node req)
      "/tasks/list"         (list-tasks node req)
      "/tasks/:id"          :>> (fn [{:keys [id]}] (get-task node req id))
      not-found)))

;; Lifted from `ring.middleware.params`
(defn- parse-params [params encoding keywords?]
  (let [params (if keywords?
                 (keywordize-keys (form-decode params encoding))
                 (form-decode params encoding))]
    (if (map? params) params {})))

(defn- assoc-query-params
  "Parse and assoc parameters from the query string with the request."
  [request encoding keywords?]
  (merge-with merge request
    (if-let [query-string (:query-string request)]
      (let [params (parse-params query-string encoding keywords?)]
        {:query-params params})
      {:query-params {}})))

(defn- wrap-params
  "A middleware that attempts to parse incoming query strings into maps."
  [handler & [opts]]
  (fn [request]
    (let [encoding (or (:encoding opts)
                       (:character-encoding request)
                       "UTF-8")
          keywords? (:keywords? opts)
          request (if (:query-params request)
                    request
                    (assoc-query-params request encoding keywords?))]
      (handler request))))

(defn- wrap-json-body-safe
  "A wrapper for `wrap-json-body` which catches JSON parsing exceptions and
  returns a Bad Request."
  [handler & [opts]]
  (let [request-handler (wrap-json-body handler opts)]
    (fn [request]
      (try (request-handler request)
        (catch JsonParseException e
          (handler request)  ;; resolve request before generating a response
          (bad-request "Bad Request"))))))

(defn service
  "Given a node and port, constructs a Jetty instance."
  [node port]
  (info "Starting HTTP server on" (str (:host node) ":" port))
  (let [handler (->
                  (make-handler node)
                  (wrap-json-body-safe {:keywords? true})
                  (wrap-params {:keywords? true}))
        jetty   (run-jetty handler {:host  (:host node)
                                    :port  port
                                    :join? false})]
    jetty))

(defn shutdown!
  "Stops a given Jetty instance."
  [^Server jetty]
  (.stop jetty))
