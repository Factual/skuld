(ns skuld.logging
  "Configures log4j to log to a file. It's a trap!"
  ; With thanks to arohner
  (:import (org.apache.log4j
             Logger
             BasicConfigurator
             PatternLayout
             Level
             ConsoleAppender
             FileAppender
             SimpleLayout
             RollingFileAppender)
           (org.apache.log4j.spi RootLogger))
  (:import org.apache.commons.logging.LogFactory))

(def levels {:trace org.apache.log4j.Level/TRACE
             :debug org.apache.log4j.Level/DEBUG
             :info  org.apache.log4j.Level/INFO
             :warn  org.apache.log4j.Level/WARN
             :error org.apache.log4j.Level/ERROR
             :fatal org.apache.log4j.Level/FATAL})

(defn set-level
  "Set the level for the given logger, by string name. Use:
  (set-level \"skuld.node\", :debug)"
  ([level]
    (. (Logger/getRootLogger) (setLevel (levels level))))
  ([logger level]
    (. (Logger/getLogger logger) (setLevel (levels level)))))

(defmacro with-level
  "Sets logging for the evaluation of body to the desired level."
  [level loggers & body]
  (let [[logger & more] (flatten [loggers])]
    (if logger
      `(let [l4j-logger# (Logger/getLogger ~logger)
             old-level# (.getLevel l4j-logger#)]
         (try
           (set-level ~logger ~level)
           (with-level ~level ~more ~@body)
           (finally
             (.setLevel l4j-logger# old-level#))))
      `(do ~@body))))

(defmacro suppress
  "Turns off logging for the evaluation of body."
  [loggers & body]
  (with-level :fatal loggers body))

(def skuld-layout
  "A nice format for log lines."
  (PatternLayout. "%p [%d] %t - %c - %m%n%xEx%n"))

(defn init
  "Initialize log4j. You will probably call this from the config file. You can
  call init more than once; its changes are destructive. Options:

  :file   The file to log to. If omitted, logs to console only."
  [& { :keys [file] }]
  ; Reset loggers
  (doto (Logger/getRootLogger)
    (.removeAllAppenders)
    (.addAppender (ConsoleAppender. skuld-layout)))

  (comment
  (when file
    (let [rolling-policy (doto (TimeBasedTriggeringPolicy.)
                           (.setActiveFileName file)
                           (.setFileNamePattern
                             (str file ".%d{yyyy-MM-dd}.gz"))
                           (.activateOptions))
          log-appender (doto (RollingFileAppender.)
                         (.setRollingPolicy rolling-policy)
                         (.setLayout skuld-layout)
                         (.activateOptions))]
      (.addAppender (Logger/getRootLogger) log-appender)))
    )

  ; Set levels.
  (set-level :info)

  (set-level "skuld.node" :debug)

; Not sure where he intended this to go....
(defn- add-file-appender [loggername filename]
  (.addAppender (Logger/getLogger loggername)
    (doto (FileAppender.)
      (.setLayout skuld-layout))))

  (comment
(defn nice-syntax-error
  "Rewrites clojure.lang.LispReader$ReaderException to have error messages that
  might actually help someone."
  ([e] (nice-syntax-error e "(no file)"))
  ([e file]
    ; Lord help me.
    (let [line (wall.hack/field (class e) :line e)
          msg (.getMessage (or (.getCause e) e))]
      (RuntimeException. (str "Syntax error (" file ":" line ") " msg))))))

  )
