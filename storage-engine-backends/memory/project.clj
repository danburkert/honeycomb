(defproject com.nearinfinity.honeycomb.memory/honeycomb-memory "0.1-SNAPSHOT"
  :description "In-memory backend for the Honeycomb storage engine"
  :url "http://www.github.com/nearinfinity/honeycomb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  ;; NOTE: when dependencies change, run `lein pom` to regenerate the pom.xml
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.nearinfinity.honeycomb/honeycomb "0.1-SNAPSHOT"]
                 [com.google.guava/guava "14.0.1"]
                 [com.google.inject/guice "3.0"]
                 [com.google.inject.extensions/guice-multibindings "3.0"]]
  :global-vars {*warn-on-reflection* true}
  :aot :all)
