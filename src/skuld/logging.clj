(ns skuld.logging
  "Configures loggers"
  (:import (ch.qos.logback.classic
             Level
             Logger
             LoggerContext
             PatternLayout)
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

(def ^LoggerContext root-logger-context
  (.getLoggerContext root-logger))

(defn logger-name
  "The name of a logger."
  [^Logger logger]
  (.getName logger))

(defn level
  "The level of a logger."
  [^Logger logger]
  (.getLevel logger))

(defn all-loggers
  "Returns a list of all registered Loggers."
  []
  (.getLoggerList root-logger-context))

(defn all-logger-names
  "Returns the names of all loggers."
  []
  (map logger-name (all-loggers)))

(defn ^Logger get-loggers
  "Returns a singleton list containing a logger for a string or, if given a
  regular expression, a list of all loggers finding matching names."
  [logger-pattern]
  (cond
    (nil? logger-pattern)
    (list root-logger)

    (instance? Logger logger-pattern)
    (list logger-pattern)

    (string? logger-pattern)
    (list (LoggerFactory/getLogger ^String logger-pattern))

    :else (filter (comp (partial re-find logger-pattern) logger-name)
                  (all-loggers))))

(defn ^Logger get-logger
  [logger-pattern]
  (first (get-loggers logger-pattern)))

(defn set-level
  "Set the level for the given logger, by string name. Use:

  (set-level (get-logger \"skuld.node\") :debug)"
  ([level]
   (set-level root-logger level))
  ([logger-pattern level]
   (doseq [logger (get-loggers logger-pattern)]
     (.setLevel logger (level-for level)))))

(defmacro with-level
  "Sets logging for the evaluation of body to the desired level."
  [level-name logger-patterns & body]
  `(let [level#               (level-for ~level-name)
         root-logger-level#   (level root-logger)
         loggers-and-levels#  (doall
                                (->> (list ~logger-patterns)
                                     flatten
                                     (mapcat get-loggers)
                                     (map #(list % (or (level %)
                                                       root-logger-level#)))))]
     (try
       (doseq [[logger# _#] loggers-and-levels#]
         (set-level logger# level#))
       (do ~@body)
       (finally
         (dorun (map (partial apply set-level) loggers-and-levels#))))))

(defmacro mute
  "Turns off logging for all loggers the evaluation of body."
  [& body]
  `(with-level :off (all-logger-names) ~@body))

(defmacro suppress
  "Turns off logging for the evaluation of body."
  [loggers & body]
  `(with-level :off ~loggers ~@body))

(defn init
  []
  (doto root-logger-context
    .reset
    (.addListener
      (doto (LevelChangePropagator.)
        (.setContext root-logger-context)
        (.setResetJUL true))))
  (.addAppender root-logger
    (doto (ConsoleAppender.)
      (.setName "console-appender")
      (.setContext root-logger-context)
      (.setLayout
        (doto (PatternLayout.)
          (.setContext root-logger-context)
          (.setPattern "%date{YYYY-MM-dd'T'HH:mm:ss.SSS} %-5p %c: %m%n%xEx")
          (.start)))
      (.start))
    )
  (set-level :info))

(init)
