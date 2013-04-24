(ns com.nearinfinity.honeycomb.memory.memory-scanner
  (:import [com.nearinfinity.honeycomb Scanner]))

(defrecord MemoryScanner [rows]
  java.io.Closeable
  (close [this])

  java.util.Iterator
  (hasNext [this]
    (-> @rows seq true?))

  (next [this] ;; NOT THREAD SAFE
    (let [next (first @rows)]
      (swap! rows rest)
      next))

  Scanner)
