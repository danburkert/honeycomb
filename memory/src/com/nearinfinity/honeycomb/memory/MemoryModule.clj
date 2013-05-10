(ns com.nearinfinity.honeycomb.memory.MemoryModule
  (:require [com.nearinfinity.honeycomb.memory.memory-store :as mem-store])
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
    (-> map-binder
        (.addBinding AdapterType/MEMORY)
        (.to com.nearinfinity.honeycomb.memory.memory_store.MemoryStore))
    (-> this
        (.bindSuper com.nearinfinity.honeycomb.memory.memory_store.MemoryStore)
        (.toInstance memory-store))))

(comment
(let [module (com.nearinfinity.honeycomb.memory.MemoryModule. {})]
  (Guice/createInjector [module]))
)
