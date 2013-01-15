(defproject nearinfinity.honeycomb/benchmark "0.1.0-SNAPSHOT"
            :description "FIXME: write description"
            :url "http://example.com/FIXME"
            :license {:name "Eclipse Public License"
                      :url "http://www.eclipse.org/legal/epl-v10.html"}
            :main nearinfinity.honeycomb.benchmark.core
            :dependencies [[org.clojure/clojure "1.5.0-RC1"]
                           [org.clojure/java.jdbc "0.2.3"]
                           [mysql/mysql-connector-java "5.1.6"]
                           [clojureql "1.0.4"]
                           [org.clojure/tools.cli "0.2.2"]
                           [nearinfinity/clj-faker "0.1.0"]])
