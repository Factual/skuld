(ns skuld.logging
  "Configures logger"
  (:import (ch.qos.logback.classic
             Level
             Logger
             LoggerContext)
           (org.slf4j
             LoggerFactory)
           (ch.qos.logback.classic.jul
             LevelChangePropagator)
           (ch.qos.logback.core
             ConsoleAppender)))


(defn level-for
  "Get the level for a given symbol"
  [level]
    (if (instance? Level level)
      level
      (Level/toLevel (name level))))

(def ^String root-logger-name
  org.slf4j.Logger/ROOT_LOGGER_NAME)

(def ^Logger root-logger
  (LoggerFactory/getLogger root-logger-name))

(defn ^Logger get-logger
  [^String logger-name]
  (prn :get-logger logger-name)
  (LoggerFactory/getLogger
    (or logger-name root-logger-name)))

(def ^LoggerContext root-logger-context
  (.getLoggerContext root-logger))

(defn all-loggers
  []
  (.getLoggerList root-logger-context))

(defn all-logger-names
  []
  (map (fn [^Logger logger] (.getName logger)) (all-loggers)))

(defn set-level
  "Set the level for the given logger, by string name. Use:
  (set-level \"skuld.node\", :debug)"
  ([level]
    (set-level nil level))
  ([^Logger logger level]
    (prn :set-level logger level)
    (.setLevel (get-logger logger) (level-for level))))

(defmacro with-level
  "Sets logging for the evaluation of body to the desired level."
  [level-name logger-names & body]
  `(let [loggers# (map get-logger (flatten [~logger-names]))
         levels#  (doall (map #(.getLevel %) loggers#))
         level#   (level-for ~level-name)]
     (try
       (doseq [l# loggers#] (.setLevel l# level#))
       (do ~@body)
       (finally
         (dorun (map (fn [^Logger logger#
                          ^Level orig-level#]
                       (.setLevel logger# orig-level#))
                  loggers# levels#))))))

(defmacro mute
  "Turns off logging for all loggers the evaluation of body."
  [& body]
  (with-level :off (all-logger-names) ~@body))

(defmacro suppress
  "Turns off logging for the evaluation of body."
  [loggers & body]
  (with-level :off loggers ~@body))

(defn init
  []
  (let [context (.getLoggerContext root-logger)]
    (doto context
      .reset
      (prn :context)
      (.addListener
        (doto (LevelChangePropagator.)
          (.setContext context)
          (.setResetJUL true))))
    (.addAppender root-logger
      (doto (ConsoleAppender.)
        (.setName "console-appender")
        (.setContext context)
        (prn :appender)
        (.start))
      )
    (set-level :info)))

(init)