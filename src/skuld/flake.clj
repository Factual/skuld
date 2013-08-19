(ns skuld.flake
  "ID generation. Flake IDs are 160 bits, and comprise:
  
  [64 bits | Timestamp, in milliseconds since the epoch]
  [32 bits | a per-process counter, reset each millisecond]
  [48 bits | a host identifier]
  [16 bits | the process ID]
  
  Note that the timestamp is not Posix time or UTC time. Instead we use the
  JVM's nanoTime, which is a linear time source over intervals smaller than
  ~292 years, and use it to compute an offset from the POSIX time as measured
  *once* by System/currentTimeMillis.

  Regressions in time are not allowed; Flake will periodically serialize the
  current time to disk to prevent regressions."
  (:require [primitive-math :as p])
  (:use     [potemkin :only [deftype+]])
  (:import (java.lang.management ManagementFactory)
           (java.net InetAddress
                     NetworkInterface)
           (java.nio ByteBuffer)
           (java.security MessageDigest)
           (java.util Arrays)
           (java.util.concurrent.atomic AtomicInteger)))
           
(defonce initialized (atom false))

; Cached state
(declare time-offset*)
(declare ^"[B" node-fragment*)

; Mutable state
(deftype+ Counter [^long time ^int count])
(defonce counter (atom (Counter. Long/MIN_VALUE Integer/MIN_VALUE)))

(defn pid
  "Process identifier, such as it is on the JVM. :-/"
  []
  (let [name (.. ManagementFactory getRuntimeMXBean getName)]
    (Integer. ^String (get (re-find #"^(\d+).*" name) 1))))

(defn ^"[B" node-id
  "We take all hardware addresses, sort them bytewise, concatenate, hash, and
  truncate to 48 bits."
  []
  (let [addrs (->> (NetworkInterface/getNetworkInterfaces)
                   enumeration-seq
                   (map #(.getHardwareAddress ^NetworkInterface %))
                   (remove nil?))
        md    (MessageDigest/getInstance "SHA-1")]
    (assert (< 0 (count addrs)))
    (doseq [addr addrs]
      (.update md ^bytes addr))
    ; 6 bytes is 48 bits
    (Arrays/copyOf (.digest md) 6)))

(defn time-offset-estimate
  "Tries to figure out the time offset between epochMillis and nanoTime. You
  can add this offset to nanoTime to reconstruct POSIX time, only linearized
  from a particular instant."
  []
  (- (* 1000000 (System/currentTimeMillis))
     (System/nanoTime)))

(defn mean-time-offset
  "Computes the offset, in nanoseconds, between the current nanoTime and the
  system. Takes n samples."
  [n]
  (-> (->> (repeatedly time-offset-estimate)
           (map double) 
           (take n)
           (reduce +))
      (/ n)
      long))

(defn linear-time
  "Returns a linearized time in milliseconds, roughly corresponding to time
  since the epoch."
  []
  (p/div (p/+ (unchecked-long time-offset*) (System/nanoTime))
         1000000))

(defn node-fragment
  "Constructs an eight-byte long byte array containing the node ID and process
  ID."
  []
  (let [a (byte-array 8)]
    (doto (ByteBuffer/wrap a)
      (.put (node-id))
      (.putShort (unchecked-short (pid))))
    a))

(defn init!
  "Initializes the flake generator state."
  []
  (locking initialized
    (if (false? @initialized)
      (do
        (def ^long  time-offset*   (mean-time-offset 10))
        (def ^"[B"  node-fragment* (node-fragment))

        (reset! initialized true)))))

(defn ^long count!
  "Increments and gets the count for a given time."
  [t']
  (.count ^Counter (swap! counter
                          (fn [^Counter c]
                            (cond
                              (< (.time c) t') (Counter. t' Integer/MIN_VALUE)
                              (= (.time c) t') (Counter. t' (p/inc (.count c)))
                              :else (throw (IllegalStateException.
                                             "time can't flow backwards.")))))))

(defn ^bytes id
  "Generate a new flake ID; returning a byte array."
  []
  (let [id (try
             (let [t (linear-time)
                   c (count! t)
                   b (ByteBuffer/allocate 20)]
               (.putLong b t)
               (.putInt b (count! t))
               (.put b node-fragment*)
               (.array b))
             (catch IllegalStateException e
               ; Lost the race to count for this time; retry.
               ::recur))]
    (if (= id ::recur)
      (recur)
      id)))

(defn byte-buffer
  "Wraps a byte[] in a ByteBuffer."
  [^bytes b]
  (ByteBuffer/wrap b))
