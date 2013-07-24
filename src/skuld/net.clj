(ns skuld.net
  "Handles network communication between nodes. Automatically maintains TCP
  connections encapsulated in a single stateful component. Allows users to
  register callbacks to receive messages."
  (:require [clojure.edn :as edn])
  (:import (java.io ByteArrayInputStream
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
                             ChannelStateHandler
                             DefaultEventExecutorGroup)
           (io.netty.channel.socket SocketChannel)
           (io.netty.channel.socket.nio NioEventLoopGroup
                                        NioServerSocketChannel)
           (io.netty.handler.codec MessageToMessageDecoder
                                   MessageToMessageEncoder)
           (io.netty.handler.codec.protobuf ProtobufVarint32FrameDecoder
                                            ProtobufVarint32LengthFieldPrepender)
           (io.netty.util Attribute
                          AttributeKey)))

(defonce event-executor
  (DefaultEventExecutorGroup. 16))

(def protobuf-varint32-frame-decoder
  (ProtobufVarint32FrameDecoder.))

(def protobuf-varint32-frame-encoder
  (ProtobufVarint32LengthFieldPrepender.))

(def edn-decoder
  (proxy [MessageToMessageDecoder] [(into-array Class [ByteBuf])]
    (decode [^ChannelHandlerContext ctx ^ByteBuf buffer ^List out]
      (with-open [is (ByteBufInputStream. buffer)
                  i  (InputStreamReader. is)
                  r  (PushbackReader. i)]
        (binding [*read-eval* false]
          (.add out (edn/read r)))))))

(def edn-encoder
  (proxy [MessageToMessageEncoder] [(into-array Class [Object])]
    (encode [^ChannelHandlerContext ctx object ^List out]
      (->> object pr-str .getBytes Unpooled/wrappedBuffer (.add out)))))

(defn handler
  "Returns a Netty handler that calls f with its messages, and writes
  non-nil return values back."
  [f]
  (proxy [ChannelInboundMessageHandlerAdapter] [(into-array Class [Object])]
    (messageReceived [^ChannelHandlerContext ctx message]
      (when-let [response (f message)]
        (.write ctx response)))))

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
        (.localAddress ^String (:host node) ^int (:port node))
        (.option ChannelOption/SO_BACKLOG 100)
        (.childOption ChannelOption/TCP_NODELAY true)
        (.childHandler
          (proxy [ChannelInitializer] []
            (initChannel [^SocketChannel ch]
              (.. ch
                  (pipeline)
                  (addLast protobuf-varint32-frame-decoder)
                  (addLast protobuf-varint32-frame-encoder)
                  (addLast edn-decoder)
                  (addLast edn-encoder)
                  (addLast event-executor (handler (:handler node))))))))
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
  (reify ChannelStateHandler
    (^void channelInactive [i^ChannelHandlerContext ctx]
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
        group- (NioEventLoopGroup. 16)
        inactive-handler (inactive-client-handler node)
        handler (handler (:handler node))]
    (try
      (doto bootstrap
        (.group group-)
        (.option ChannelOption/TCP_NODELAY true)
        (.option ChannelOption/SO_KEEPALIVE true)
        (.handler (proxy [ChannelInitializer] []
                    (initChannel [^SocketChannel ch]
                      (.. ch
                          (pipeline)
                          (addLast protobuf-varint32-frame-decoder)
                          (addLast protobuf-varint32-frame-encoder)
                          (addLast edn-decoder)
                          (addLast edn-encoder)
                          (addLast inactive-handler)
                          (addLast event-executor handler))))))
      {:bootstrap bootstrap
       :group group-}

      (catch Throwable t
        (.shutdown group-)
        (.awaitTermination group- 30 TimeUnit/SECONDS)
        (throw t)))))

(defn connect
  "Opens a new client connection to the given peer, identified by host and
  port. Returns a netty Channel."
  [node peer]
  (let [^Bootstrap bootstrap (:bootstrap (:client node))
        ch (.. bootstrap
               (remoteAddress ^String (:host peer) ^int (:port peer))
               (connect)
               (sync)
               (channel))]
    ; Store the peer ID in the channel's attributes for later.
    (.. ch
        (attr peer-attr)
        (set peer))
    ch))

(defn ^Channel conn
  "Find or create a client to the given peer."
  [node peer]
  (let [conns (:conns node)]
    ; Standard double-locking strategy
    (or (get conns peer)
        ; Open a connection
        (let [conn (connect node peer)]
          ; Lock and see if we lost a race
          (locking conns
            (if-let [existing-conn (get @conns peer)]
              (do
                ; Abandon new conn
                (.close ^Channel conn)
                existing-conn)
              (do
                ; Use new conn
                (swap! conns assoc peer conn)
                conn)))))))

(defn send!
  "Sends a message to a peer."
  [node peer msg]
  (-> (conn node peer)
      (.write conn msg))
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
  [fns]
  (fn compiled-handler [msg]
    (loop [fns fns]
      (if-let [value ((first fns) msg)]
        value
        (recur (rest fns))))))

(defn add-handler!
  "Registers a handler for incoming messages."
  [node f]
  (locking (:handler node)
    (if (deref (:handler node))
      (throw (IllegalStateException.
               "node already running; can't add handlers now"))
      (swap! (:handlers node) conj f))))

(defn start!
  "Starts the node, when all handlers have been registered."
  [node]
  (locking (:handler node)
    (when-not (deref (:handler node))
      (let [handler (compile-handler (deref (:handlers node)))]
        (reset! (:handler node) handler) 
        (reset! (:server node) (server node))
        (reset! (:client node) (client node))))))

(defn shutdown!
  "Shuts down a node. May only be called once."
  [node]
  (shutdown-client! @(:client node))
  (shutdown-server! @(:server node))
  (reset! (:conns node) {}))
