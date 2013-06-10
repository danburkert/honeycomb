 ; Licensed to the Apache Software Foundation (ASF) under one
 ; or more contributor license agreements.  See the NOTICE file
 ; distributed with this work for additional information
 ; regarding copyright ownership.  The ASF licenses this file
 ; to you under the Apache License, Version 2.0 (the
 ; "License"); you may not use this file except in compliance
 ; with the License.  You may obtain a copy of the License at
 ;
 ;   http://www.apache.org/licenses/LICENSE-2.0
 ;
 ; Unless required by applicable law or agreed to in writing,
 ; software distributed under the License is distributed on an
 ; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ; KIND, either express or implied.  See the License for the
 ; specific language governing permissions and limitations
 ; under the License.
 ; 
 ; Copyright 2013 Near Infinity Corporation.


(defproject com.nearinfinity.honeycomb.memory/honeycomb-memory "0.1"
  :description "In-memory backend for the Honeycomb storage engine"
  :url "http://www.github.com/nearinfinity/honeycomb"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.nearinfinity.honeycomb/honeycomb "0.1" :scope "provided"]
                 [com.google.guava/guava "14.0.1" :scope "provided"]
                 [com.google.inject/guice "3.0" :scope "provided"]
                 [com.google.inject.extensions/guice-multibindings "3.0" :scope "provided"]]
  :global-vars {*warn-on-reflection* true}
  :aot :all)
