(ns skuld.net
  "Handles network communication between nodes. Automatically maintains TCP
  connections encapsulated in a single stateful component. Allows users to
  register callbacks to receive messages."
  (:require [clojure.edn :as edn]
            [taoensso.nippy :as nippy])
;  (:use clojure.tools.logging) 
  (:import (java.io ByteArrayInputStream
                    DataInputStream
                    InputStreamReader
                    PushbackReader)
           (java.net InetSocketAddress)
           (java.util List)
           (java.util.concurrent TimeUnit)
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
                          AttributeKey)))

(def logger (agent nil))
(defn log-print
  [_ & things]
  (apply println things))
(defn log
  [& things]
  (apply send-off logger log-print things))

(declare started?)

(defonce event-executor
  (DefaultEventExecutorGroup. 16))

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
          (edn/read r))))))

(defn nippy-codec []
  (proxy [MessageToMessageCodec]
    [(into-array Class [ByteBuf]) (into-array Class [Object])]

    (encode [^ChannelHandlerContext ctx object]
      (->> object nippy/freeze Unpooled/wrappedBuffer))

    (decode [^ChannelHandlerContext ctx ^ByteBuf buffer]
      (let [a (byte-array (.readableBytes buffer))]
        (.readBytes buffer a)
;      (with-open [is (ByteBufInputStream. buffer)
;                  dis (DataInputStream. is)]
        (binding [*read-eval* false]
          (nippy/thaw a))))))

(defn handler
  "Returns a Netty handler that calls f with its messages, and writes
  non-nil return values back."
  [f]
  (proxy [ChannelInboundMessageHandlerAdapter] [(into-array Class [Object])]
    (messageReceived [^ChannelHandlerContext ctx message]
      (when-let [response (try (f message)
                               (catch Throwable t
                                 (locking *out*
                                   (println "Node handler caught:")
                                   (.printStackTrace t))
                                 nil))]
        (.write ctx response)))))

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
                  (addLast "codec" (edn-codec))
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
                          (addLast "codec" (edn-codec))
                          (addLast "inactive" (inactive-client-handler node))
                          (addLast event-executor "handler"
                                  (handler @(:handler node))))))))
      {:bootstrap bootstrap
       :group group-}

      (catch Throwable t
        (.shutdown group-)
        (.awaitTermination group- 30 TimeUnit/SECONDS)
        (throw t)))))

(defn node-id
  "A short identifier for a node."
  [node]
  (str (:host node) ":" (:port node)))

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
      (let [ch (.. bootstrap
                   (remoteAddress ^String (:host peer) (int (:port peer)))
                   (connect)
                   (sync)
                   (channel))]
        ; Store the peer ID in the channel's attributes for later.
        (.. ch
            (attr peer-attr)
            (set (node-id peer)))
        ch))))

(defn ^Channel conn
  "Find or create a client to the given peer."
  [node peer]
  (let [id    (node-id peer)
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
  (let [c (conn node peer)]
    (.write c msg))
  node)

(defn send-sync!
  "Sends a message to a peer, blocking for the write to complete."
  [node peer msg]
  (-> (conn node peer)
      (.write msg)
      (.sync))
  node)

(defn node
  "Creates a new network node. Options:
  
  :host
  :port
  :handler   A function which accepts messages from other nodes.

  Handlers are invoked with each message, and may return an response to be sent
  back to the client."
  [opts]
  (let [host (get opts :host "127.0.0.1")
        port (get opts :port 13000)]
    {:host host
     :port port
     :handlers (atom [])
     :handler (atom nil)
     :server (atom nil)
     :client (atom nil)
     :conns (atom {})}))

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

(defn started?
  "Is node started?"
  [node]
  (not (nil? @(:handler node))))

(defn start!
  "Starts the node, when all handlers have been registered."
  [node]
  (locking (:handler node)
    (when-not (started? node)
      (let [handler (compile-handler (deref (:handlers node)))]
        (reset! (:handler node) handler) 
        (reset! (:server node) (server node))
        (reset! (:client node) (client node)))))
  node)

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
    (reset! (:conns node) {})
    (reset! (:handler node) nil)
    node))

; High-level messaging primitives

; A Request represents a particular request made to N nodes. It includes a
; unique identifier, the time (in nanoTime) the request is valid until, some
; debugging information, the number of responses necessary to satisfy the
; request, and a mutable list of responses received.
(defrecord Request [^bytes id ^long max-time debug ^int r responses])

(defn new-request
  "Constructs a new Request. Automatically generates an ID, and computes the
  max-time by adding the timeout to the current time."
  [timeout debug r])
