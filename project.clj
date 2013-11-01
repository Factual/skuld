(defproject skuld "0.1.0-SNAPSHOT"
  :description "A hybrid AP/CP distributed task queue."
  :url "http://github.com/factual/skuld"
  :main skuld.bin
  :java-source-paths ["src/skuld/"]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :exclusions [[log4j]
               [org.slf4j/slf4j-log4j12]]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [org.clojure/tools.logging "0.2.3"]
                 [ch.qos.logback/logback-classic "1.0.13"]
                 [org.slf4j/jcl-over-slf4j "1.7.5"]
                 [org.slf4j/log4j-over-slf4j "1.7.5"]
                 [factual/clj-helix "0.1.0"]
                 [io.netty/netty "4.0.0.Alpha8"]
                 [com.taoensso/nippy "2.1.0"]
                 [com.google.protobuf/protobuf-java "2.5.0"]
                 [org.clojure/data.codec "0.1.0"]
                 [potemkin "0.3.0"]
                 [merkle "0.1.0"]
                 [org.apache.curator/curator-recipes "2.0.1-incubating"]
                 [factual/clj-leveldb "0.1.0"]
                 [clout "1.1.0"]
                 [ring/ring-jetty-adapter "1.2.1"]]
  :jvm-opts ^:replace ["-server"]
  :profiles {:dev {:dependencies [[criterium "0.4.1"]
                                  [com.google.guava/guava "14.0.1"]
                                  [org.apache.curator/curator-test "2.0.1-incubating"]]}}
  :test-selectors {:default (fn [x] (not (or (:integration x)
                                             (:time x)
                                             (:bench x))))
                   :focus :focus
                   :bench :bench}
  :global-vars {*warn-on-reflection* true})
