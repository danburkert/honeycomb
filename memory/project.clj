(defproject com.nearinfinity.honeycomb/memory "0.1.0-SNAPSHOT"
  :description "In-memory storage adaptor for the Honeycomb storage engine"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.nearinfinity.honeycomb/mysqlengine "0.1"]
                 [com.google.guava/guava "14.0.1"]
                 [com.google.inject/guice "3.0"]
                 [com.google.inject.extensions/guice-multibindings "3.0"]]
  :aot :all)
