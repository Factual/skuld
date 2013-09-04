(ns skuld.logging
  "Configures log4j to log to a file. It's a trap!"
  ; With thanks to arohner
  (:import (org.apache.log4j
             Logger
             Level)))

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