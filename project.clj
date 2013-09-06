(defproject skuld "0.1.0-SNAPSHOT"
  :description "A hybrid AP/CP distributed task queue."
  :url "http://github.com/factual/skuld"
  :main skuld.bin
  :java-source-paths ["src/skuld/"]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [org.clojure/tools.logging "0.2.3"]
                 [factual/clj-helix "0.1.0"]
                 [io.netty/netty "4.0.0.Alpha8"]
                 [com.taoensso/nippy "2.1.0"]
                 [com.google.protobuf/protobuf-java "2.5.0"]
                 [org.clojure/data.codec "0.1.0"]
                 [potemkin "0.3.0"]
                 [merkle "0.1.0"]
                 [org.apache.curator/curator-recipes "2.0.1-incubating"]
                 [factual/clj-leveldb "0.1.0"]]
  :jvm-opts ^:replace ["-server"]
  :warn-on-reflection true
  :profiles {:dev {:dependencies [[criterium "0.4.1"]
                                  [com.google.guava/guava "14.0.1"]
                                  [org.apache.curator/curator-test "2.0.1-incubating"]]}}
  :test-selectors {:default (fn [x] (not (or (:integration x)
                                             (:time x)
                                             (:bench x))))
                   :focus :focus
                   :bench :bench})
