(ns skuld.net
  "Handles network communication between nodes. Automatically maintains TCP
  connections encapsulated in a single stateful component. Allows users to
  register callbacks to receive messages."
  (:require [clojure.edn :as edn]
            [skuld.flake :as flake]
            [clojure.stacktrace :as trace]
            [hexdump.core :as hex]
            [skuld.util :refer [fress-read fress-write]])
  (:use clojure.tools.logging
        skuld.util) 
  (:import (com.aphyr.skuld Bytes)
           (java.io ByteArrayInputStream
                    DataInputStream
                    InputStreamReader
                    PushbackReader)
           (java.net InetSocketAddress ConnectException)
           (java.nio.charset Charset)
           (java.util List)
           (java.util.concurrent TimeUnit
                                 CountDownLatch)
           (io.netty.bootstrap Bootstrap
                               ServerBootstrap)
           (io.netty.buffer ByteBuf
                            ByteBufInputStream
                            ByteBufOutputStream
                            Unpooled)
           (io.netty.channel Channel
                             ChannelFuture
                             ChannelHandlerContext
                             ChannelInboundMessageHandlerAdapter
                             ChannelInitializer
                             ChannelOption
                             ChannelStateHandlerAdapter
                             DefaultEventExecutorGroup)
           (io.netty.channel.socket SocketChannel)
           (io.netty.channel.socket.nio NioEventLoopGroup
                                        NioSocketChannel
                                        NioServerSocketChannel)
           (io.netty.handler.codec MessageToMessageCodec)
           (io.netty.handler.codec.protobuf ProtobufVarint32FrameDecoder
                                            ProtobufVarint32LengthFieldPrepender)
           (io.netty.util Attribute
                          AttributeKey)
           (java.nio.channels ClosedChannelException)))

(def logger (agent nil))
(defn log-print
  [_ & things]
  (apply println things))
(defn log-
  [& things]
  (apply send-off logger log-print things))

(declare started?)
(declare handle-response!)

(defonce event-executor
  (DefaultEventExecutorGroup. 32))

(defn protobuf-varint32-frame-decoder []
  (ProtobufVarint32FrameDecoder.))

(def protobuf-varint32-frame-encoder
  (ProtobufVarint32LengthFieldPrepender.))

(defn edn-codec []
  (proxy [MessageToMessageCodec]
    [(into-array Class [ByteBuf]) (into-array Class [Object])]

    (encode [^ChannelHandlerContext ctx object]
      (->> object pr-str .getBytes Unpooled/wrappedBuffer))

    (decode [^ChannelHandlerContext ctx ^ByteBuf buffer]
      (with-open [is (ByteBufInputStream. buffer)
                  i  (InputStreamReader. is)
                  r  (PushbackReader. i)]
        (binding [*read-eval* false]
;          (debug "Got" (.toString buffer (Charset/forName "UTF-8")))
          (edn/read r))))))

(defn fressian-codec []
  (proxy [MessageToMessageCodec]
    [(into-array Class [ByteBuf]) (into-array Class [Object])]

    (encode [^ChannelHandlerContext ctx object]
      (-> object fress-write Unpooled/wrappedBuffer))

    (decode [^ChannelHandlerContext ctx ^ByteBuf buffer]
      (let [a (byte-array (.readableBytes buffer))]
        (.readBytes buffer a)
;      (with-open [is (ByteBufInputStream. buffer)
;                  dis (DataInputStream. is)]
;        (try
          (binding [*read-eval* false]
            (fress-read a))))))
;          (catch Exception e
;            (hex/hexdump (seq a))
;            (throw e)))

(defn handler
  "Returns a Netty handler that calls f with its messages, and writes non-nil
  return values back. Response will automatically have :request-id assoc'd into
  them."
  [f]
  (proxy [ChannelInboundMessageHandlerAdapter] [(into-array Class [Object])]
    (messageReceived [^ChannelHandlerContext ctx message]
      ; Pretty sure we're seeing deadlocks due to threadpool starvation.
      ; Let's try futures here.
      (future
        (when-let [response (try (f message)
                                 (catch Throwable t
                                        (warn t "net: server handler caught an exception")
                                        {:error (str (.getMessage t)
                                                     (with-out-str
                                                       (trace/print-cause-trace t)))}))]
          (.write ctx (assoc response :request-id (:request-id message))))))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
      (let [local (.. ctx channel localAddress)
            remote (.. ctx channel remoteAddress)]
        (condp instance? cause
          ClosedChannelException (warnf "net: server handler found a closed channel between %s -> %s" local remote)
          (warn cause "net: server handler caught exception with %s -> %s" local remote))))))

(defonce peer-attr
  (AttributeKey. "skuld-peer"))

(defn server
  "Starts a netty server for this node."
  [node]
  (let [bootstrap ^ServerBootstrap (ServerBootstrap.)
        boss-group (NioEventLoopGroup.)
        worker-group (NioEventLoopGroup.)]
    (try
      (doto bootstrap
        (.group boss-group worker-group)
        (.channel NioServerSocketChannel)
        (.localAddress ^String (:host node) ^int (int (:port node)))
        (.option ChannelOption/SO_BACKLOG (int 100))
        (.childOption ChannelOption/TCP_NODELAY true)
        (.childHandler
          (proxy [ChannelInitializer] []
            (initChannel [^SocketChannel ch]
              (.. ch
                  (pipeline)
                  (addLast "fdecoder" (protobuf-varint32-frame-decoder))
                  (addLast "fencoder" protobuf-varint32-frame-encoder)
                  (addLast "codec" (fressian-codec))
                  (addLast event-executor "handler"
                          (handler @(:handler node))))))))

      {:listener (.. bootstrap bind sync)
       :bootstrap bootstrap
       :boss-group boss-group
       :worker-group worker-group}

      (catch Throwable t
        (.shutdown boss-group)
        (.shutdown worker-group)
        (.awaitTermination boss-group 30 TimeUnit/SECONDS)
        (.awaitTermination worker-group 30 TimeUnit/SECONDS)
        (throw t)))))

(defn inactive-client-handler
  "When client conns go inactive, unregisters them from the corresponding
  node."
  [node]
  (proxy [ChannelStateHandlerAdapter] []
    (^void channelInactive [^ChannelHandlerContext ctx]
      (let [peer (.. ctx channel (attr peer-attr) get)]
        ; Dissoc from node's map
        (swap! (:conns node)
               (fn [conns]
                 (let [ch (get conns peer)]
                   (if (= ch (.channel ctx))
                     ; Yep, remove ourselves.
                     (dissoc conns peer)
                     conns))))))))

(defn client-response-handler
  "When messages arrive, routes them through the node's request map."
  [node]
  (let [requests (:requests node)]
    (proxy [ChannelInboundMessageHandlerAdapter] [(into-array Class [Object])]
      (messageReceived [^ChannelHandlerContext ctx message]
        (when-let [id (:request-id message)]
          (try
            (handle-response! requests id message)
            (catch Throwable t
              (warn t "node handler caught")))))
      (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
        (let [peer (.. ctx channel (attr peer-attr) get)]
          (condp instance? cause
            ConnectException (warnf "client handle caught exception with %s: %s" peer (.getMessage cause))
                             (warn cause "client handle caught exception with" peer)))))))

(defn client
  [node]
  (let [bootstrap ^Bootstrap (Bootstrap.)
        group- (NioEventLoopGroup. 16)]
    (try
      (doto bootstrap
        (.group group-)
        (.option ChannelOption/TCP_NODELAY true)
        (.option ChannelOption/SO_KEEPALIVE true)
        (.channel NioSocketChannel)
        (.handler (proxy [ChannelInitializer] []
                    (initChannel [^SocketChannel ch]
                      (.. ch
                          (pipeline)
                          (addLast "fdecoder" (protobuf-varint32-frame-decoder))
                          (addLast "fencoder" protobuf-varint32-frame-encoder)
                          (addLast "codec" (fressian-codec))
                          (addLast "inactive" (inactive-client-handler node))
                          (addLast event-executor "handler"
                                  (client-response-handler node)))))))
      {:bootstrap bootstrap
       :group group-}

      (catch Throwable t
        (.shutdown group-)
        (.awaitTermination group- 30 TimeUnit/SECONDS)
        (throw t)))))

(defn string-id
  "A string identifier for a node."
  [node]
  (str (:host node) ":" (:port node)))

(defn id
  "A map identifier for a node."
  [node]
  {:host (:host node)
   :port (:port node)})

(defn connect
  "Opens a new client connection to the given peer, identified by host and
  port. Returns a netty Channel."
  [node peer]
  (assert (started? node))
  (assert (integer? (:port peer))
          (str (pr-str peer) " peer doesn't have an integer port"))
  (assert (string? (:host peer))
          (str (pr-str peer) " peer doesn't have a string host"))

  (let [^Bootstrap bootstrap (:bootstrap @(:client node))]
    (locking bootstrap
      (let [cf (.. bootstrap
                   (remoteAddress ^String (:host peer) (int (:port peer)))
                   (connect))
            ch (.channel cf)]
        ; Store the peer ID in the channel's attributes for later.
        (.. ch
            (attr peer-attr)
            (set (string-id peer)))
        (.sync cf)
        ch))))

(defn ^Channel conn
  "Find or create a client to the given peer."
  [node peer]
  (let [id    (string-id peer)
        conns (:conns node)]
    ; Standard double-locking strategy
    (or (get @conns id)
        ; Open a connection
        (let [conn (connect node peer)]
          ; Lock and see if we lost a race
          (locking conns
            (if-let [existing-conn (get @conns id)]
              (do
                ; Abandon new conn
                (.close ^Channel conn)
                existing-conn)
              (do
                ; Use new conn
                (swap! conns assoc id conn)
                conn)))))))

(defn send!
  "Sends a message to a peer."
  [node peer msg]
  (assert (started? node))
  (let [c (conn node peer)]
;    (log- (:port node) "->" (:port peer) (pr-str msg))
    (.write c msg))
  node)

(defn send-sync!
  "Sends a message to a peer, blocking for the write to complete."
  [node peer msg]
  (-> (conn node peer)
      (.write msg)
      (.sync))
  node)

(defn compile-handler
  "Given a set of n functions, returns a function which invokes each function
  with a received message, returning the first non-nil return value. The first
  function which returns non-nil terminates execution."
  [functions]
  (if (empty? functions)
    (constantly nil)
    (fn compiled-handler [msg]
      (loop [[f & fns] functions]
        (when f
          (if-let [value (f msg)]
            value
            (recur fns)))))))

(defn add-handler!
  "Registers a handler for incoming messages."
  [node f]
  (locking (:handler node)
    (if (deref (:handler node))
      (throw (IllegalStateException.
               "node already running; can't add handlers now"))
      (swap! (:handlers node) conj f)))
  node)

; High-level messaging primitives

; A Request represents a particular request made to N nodes. It includes a
; unique identifier, the time (in nanoTime) the request is valid until, some
; debugging information, the number of responses necessary to satisfy the
; request, and a mutable list of responses received. Finally, takes a function
; to invoke with responses, when r have been accrued.
(defrecord Request [debug ^bytes id ^long max-time ^int r responses f])

(defn ^Request new-request
  "Constructs a new Request. Automatically generates an ID, and computes the
  max-time by adding the timeout to the current time.
  
  Options:
  
  :debug    Debugging info
  :timeout  Milliseconds to wait
  :r        Number of responses to await
  :f        A callback to receive response"
  [opts]
  (Request. (:debug opts)
            (flake/id)
            (+ (flake/linear-time) (get opts :timeout 5000))
            (or (:r opts) 1)
            (atom (list))
            (:f opts)))

(defn request!
  "Given a node, sends a message to several nodes, and invokes f when r have
  responded within the timeout. The message must be a map, and a :request-id
  field will be automatically assoc'ed on to it. Returns the Request. Options:

  :debug
  :timeout
  :r
  :f"
  [node peers opts msg]
  (assert (started? node))
  (let [r   (majority (count peers))
        req (new-request (assoc opts :r (or (:r opts)
                                            (majority (count peers)))))
        id  (.id req)
        msg (assoc msg :request-id id)]

    ; Save request
    (swap! (:requests node) assoc (Bytes. id) req)

    ; Send messages
    (doseq [peer peers]
      (try
        (send! node peer msg)
        (catch io.netty.channel.ChannelException e
          (handle-response! (:requests node) id nil))))))

(defmacro req!
  "Like request!, but with the body captured into the callback function.
  Captures file and line number and merges into the debug map.

  (req! node some-peers {:timeout 3 :r 2}
    {:type :anti-entropy :keys [a b c]}
  
    [responses]
    (assert (= 3 responses))
    (doseq [r responses]
      (prn :got r)))"
  [node peers opts msg binding-form & body]
  (let [debug (meta &form)]
    `(let [opts# (-> ~opts
                     (update-in [:debug] merge ~debug)
                     (assoc :f (fn ~binding-form ~@body)))]
       (request! ~node ~peers opts# ~msg))))

(defmacro sync-req!
  "Like request!, but *returns* a list of responses, synchronously."
  [node peers opts msg]
  (let [debug (meta &form)]
    `(let [p# (promise)
           opts# (-> ~opts
                     (update-in [:debug] merge ~debug)
                     (assoc :f (fn [rs#] (deliver p# rs#))))]
       (request! ~node ~peers opts# ~msg)
       @p#)))

(defn expired-request?
  "Is the given request past its timeout?"
  [request]
  (< (:max-time request) (flake/linear-time)))

(defn gc-requests!
  "Given an atom mapping ids to Requests, expires timed-out requests."
  [reqs]
  (let [expired (->> reqs
                     deref
                     (filter (comp expired-request? val)))]
    (doseq [[id ^Request req] expired]
      (swap! reqs dissoc id)
      ((.f req) @(.responses req)))))

(defn shutdown-requests!
  "Given a mapping of ids to Requests, expire all of the requests."
  [requests]
  (doseq [[id ^Request req] requests]
    ((.f req) @(.responses req))))

(defn periodically-gc-requests!
  "Starts a thread to GC requests. Returns a promise which, when set to false,
  shuts down."
  [reqs]
  (let [p (promise)]
    (future
      (loop []
        (when (deref p 1000 true)
          (try
            (gc-requests! reqs)
            (catch Throwable t
              (warn t "Caught while GC-ing request map")))
          (recur))))
    p))

(defn handle-response!
  "Given an atom mapping ids to Requests, slots this response into the
  appropriate request. When the response is fulfilled, removes it from the
  requests atom."
  [reqs id response]
  (let [id (Bytes. id)]
    (when-let [req ^Request (get @reqs id)]
      (let [responses (swap! (.responses req) conj response)]
        (when (= (:r req) (count responses))
          (swap! reqs dissoc id)
          ((.f req) responses))))))

; Node constructor
(defn node
  "Creates a new network node. Options:
  
  :host
  :port
  :server? Should we be a server as well? (default true)

  Handlers are invoked with each message, and may return an response to be sent
  back to the client."
  [opts]
  (let [host (get opts :host "127.0.0.1")
        port (get opts :port 13000)]
    {:host host
     :port port
     :handlers (atom [])
     :handler  (atom nil)
     :server?  (get opts :server? true)
     :server   (atom nil)
     :client   (atom nil)
     :conns    (atom {})
     :gc       (atom nil)
     :requests (atom {})}))

;; Lifecycle management--start, stop, etc.
(defn started?
  "Is node started?"
  [node]
  (when-let [h (:handler node)]
    (not (nil? @h))))

(defn start!
  "Starts the node, when all handlers have been registered."
  [node]
  (locking (:handler node)
    (when-not (started? node)
      (let [handler (compile-handler (deref (:handlers node)))]
        (reset! (:handler node) handler)
        (when (:server? node) 
          (reset! (:server node) (server node)))
        (reset! (:client node) (client node))
        (reset! (:gc     node) (periodically-gc-requests! (:requests node)))
        node))))

(defn close-conns!
  "Closes all open connections on a node."
  [node]
  (doseq [[peer ^Channel channel] @(:conns node)]
    (.close channel)))

(defn shutdown-client!
  "Shuts down a netty client for a node."
  [client]
  (when client
    (let [^NioEventLoopGroup g (:group client)]
      (.shutdown g)
      (.awaitTermination g 30 TimeUnit/SECONDS))))

(defn shutdown-server!
  "Shuts down a netty server for a node."
  [server]
  (when server
    (let [^NioEventLoopGroup g1 (:boss-group server)
          ^NioEventLoopGroup g2 (:worker-group server)]
      (.shutdown g1)
      (.shutdown g2)
      (.awaitTermination g1 30 TimeUnit/SECONDS)
      (.awaitTermination g2 30 TimeUnit/SECONDS))))

(defn shutdown!
  "Shuts down a node."
  [node]
  (locking (:handler node)
    (close-conns! node)
    (shutdown-client! @(:client node))
    (shutdown-server! @(:server node))
    (shutdown-requests! @(:requests node))
    (deliver @(:gc node) false)
    (reset! (:requests node) {})
    (reset! (:conns node) {})
    (reset! (:handler node) nil)
    node))
