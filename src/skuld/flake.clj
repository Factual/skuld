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
  (:import (java.lang.management ManagementFactory)
           (java.net InetAddress
                     NetworkInterface)
           (java.security MessageDigest)
           (java.util Arrays)
           (java.util.concurrent.atomic AtomicInteger)))

(defonce initialized (atom false))

; Cached state
(declare time-offset*)
(declare node-id*)
(declare pid*)

; Mutable state
(declare counter)

(defn pid
  "Process identifier, such as it is on the JVM. :-/"
  []
  (let [name (.. ManagementFactory getRuntimeMXBean getName)]
    (Integer. (get (re-find #"^(\d+).*" name) 1))))

(defn node-id
  "We take all hardware addresses, sort them bytewise, concatenate, hash, and
  truncate to 48 bits."
  []
  (let [addrs (->> (NetworkInterface/getNetworkInterfaces)
                   enumeration-seq
                   (map #(.getHardwareAddress %))
                   (remove nil?))
        md    (MessageDigest/getInstance "SHA-1")]
    (assert (< 0 (count addrs)))
    (doseq [addr addrs]
      (.update md addr))
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
  (quot (+ time-offset* (System/nanoTime))
        1000000))

(defn init!
  "Initializes the flake generator state."
  []
  (locking initialized
    (if (false? @initialized)
      (do
        (def time-offset* (mean-time-offset 10))
        (def node-id*     (node-id))
        (def pid*         (pid))
        (def counter      (atom [(linear-time) 0]))

        (reset! initialized true)))))

(defn count!
  "Increments and gets the count for a given time."
  [t']
  (let [counter (swap! counter
                       (fn [[t count]]
                         (cond
                           (< t t') [t' 0]
                           (= t t') [t (inc count)]
                           :else (throw (IllegalStateException.
                                          "time can't flow backwards.")))))]
    (get counter 1)))

(defn id
  "Generate a new flake ID."
  []
  (let [t (linear-time)]
    [t (count! t) node-id* pid*]))
