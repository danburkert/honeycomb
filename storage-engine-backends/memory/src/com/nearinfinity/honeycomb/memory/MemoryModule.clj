; Licensed to the Apache Software Foundation (ASF) under one
; or more contributor license agreements.  See the NOTICE file
; distributed with this work for additional information
; regarding copyright ownership.  The ASF licenses this file
; to you under the Apache License, Version 2.0 (the
; "License"); you may not use this file except in compliance
; with the License.  You may obtain a copy of the License at

;   http://www.apache.org/licenses/LICENSE-2.0

; Unless required by applicable law or agreed to in writing,
; software distributed under the License is distributed on an
; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
; KIND, either express or implied.  See the License for the
; specific language governing permissions and limitations
; under the License.

; Copyright 2013 Altamira Corporation.


(ns com.nearinfinity.honeycomb.memory.MemoryModule
  (:require [com.nearinfinity.honeycomb.memory.store :as mem-store])
  (:import [com.google.inject AbstractModule Guice]
           [com.google.inject.multibindings MapBinder]
           [com.nearinfinity.honeycomb Store]
           [com.nearinfinity.honeycomb.config AdapterType])
  (:gen-class
    :extends com.google.inject.AbstractModule
    :init init
    :constructors {[java.util.Map] []}
    :state config
    :exposes-methods {binder binderSuper
                      bind bindSuper}))

(defn -init
  [config]
  [[] config])

(defn -configure [this]
  (let [map-binder (MapBinder/newMapBinder (.binderSuper this) AdapterType Store)
        memory-store (mem-store/memory-store)]
    (.. map-binder
        (addBinding AdapterType/MEMORY)
        (to (class memory-store)))
    (.. this
        (bindSuper (class memory-store))
        (toInstance memory-store))))

(comment
(let [module (com.nearinfinity.honeycomb.memory.MemoryModule. {})]
  (Guice/createInjector [module]))
)
