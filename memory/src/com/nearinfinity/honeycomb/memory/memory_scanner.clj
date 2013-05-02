(ns com.nearinfinity.honeycomb.memory.memory-scanner
  (:import [com.nearinfinity.honeycomb Scanner]
           [com.nearinfinity.honeycomb.mysql Row]))

(defrecord MemoryScanner [rows]
  java.io.Closeable

  (close [this]
    (swap! rows (constantly nil)))

  java.util.Iterator

  (hasNext [this]
    (not (empty? @rows)))

  (next [this] ;; NOT THREAD SAFE
    (let [^Row next (first @rows)]
      (swap! rows rest)
      (.serialize  next)))

  (remove [this]
    (throw UnsupportedOperationException))

  Scanner)
