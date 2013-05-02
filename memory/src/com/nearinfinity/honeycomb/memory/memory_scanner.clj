(ns com.nearinfinity.honeycomb.memory.memory-scanner
  (:import [com.nearinfinity.honeycomb Scanner]))

(defrecord MemoryScanner [rows]
  java.io.Closeable

  (close [this]
    (swap! rows (constantly nil)))

  java.util.Iterator

  (hasNext [this]
    (not (empty? @rows)))

  (next [this] ;; NOT THREAD SAFE
    (let [next (first @rows)]
      (swap! rows rest)
      (.serialize  next)))

  (remove [this]
    (throw UnsupportedOperationException))

  Scanner)
