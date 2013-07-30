(defproject skuld "0.1.0-SNAPSHOT"
  :description "Task tracking service"
  :url "http://github.com/factual/skuld"
  :main skuld.bin
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [org.clojure/tools.logging "0.2.3"]
                 [clj-helix "0.1.0-SNAPSHOT"]
                 [io/netty/netty "4.0.0.Alpha8"]
                 [com.taoensso/nippy "2.0.0"]
                 [com.google.protobuf/protobuf-java "2.5.0"]
                 [org.clojure/data.codec "0.1.0"]]
  :profiles {:dev {:dependencies [[criterium "0.4.1"]]}}
  :test-selectors {:default (fn [x] (not (or (:integration x)
                                             (:time x)
                                             (:bench x))))
                   :bench :bench})
