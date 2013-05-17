(defproject com.nearinfinity.honeycomb.memory/honeycomb-memory "0.1-SNAPSHOT"
  :description "In-memory backend for the Honeycomb storage engine"
  :url "http://www.github.com/nearinfinity/honeycomb"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.nearinfinity.honeycomb/honeycomb "0.1-SNAPSHOT" :scope "provided"]
                 [com.google.guava/guava "14.0.1" :scope "provided"]
                 [com.google.inject/guice "3.0" :scope "provided"]
                 [com.google.inject.extensions/guice-multibindings "3.0" :scope "provided"]]
  :global-vars {*warn-on-reflection* true}
  :aot :all)
