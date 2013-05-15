(ns com.nearinfinity.honeycomb.memory.test-util
  (:require [com.nearinfinity.honeycomb.memory.table :as table])
  (:import [com.nearinfinity.honeycomb.mysql Row QueryKey]
           [com.nearinfinity.honeycomb.mysql.gen QueryType]
           [com.nearinfinity.honeycomb.mysql.schema ColumnSchema ColumnSchema$Builder IndexSchema TableSchema]
           [java.nio ByteBuffer]
           [java.util UUID]))


(defn long-bb [n]
  (-> (ByteBuffer/allocate 8)
      (.putLong n)
      .rewind))

(defn double-bb [n]
  (-> (ByteBuffer/allocate 8)
      (.putDouble n)
      .rewind))

(defn string-bb [^String s]
  (-> s .getBytes ByteBuffer/wrap))

(defn create-index-schema
  "Takes a map with a :name, :columns (seq of column names), and :unique (boolean)
   and returns an IndexSchema."
  [{:keys [name columns unique] :or {unique false}}]
  (IndexSchema. name columns unique))

(defn create-schema [columns indices]
  (let [create-column (fn [{:keys [name type nullable autoincrement max-length scale precision]}]
                        (cond-> (ColumnSchema$Builder. name type)
                          (not nullable) (.setIsNullable true)
                          autoincrement (.setIsAutoIncrement true)
                          max-length (.setMaxLength max-length)
                          scale (.setScale scale)
                          precision (.setPrecision precision)
                          true .build))]
    (TableSchema. (map create-column columns)
                  (map create-index-schema indices))))

(defn create-row [& {:as fields}]
  (Row. fields (UUID/randomUUID)))

(defn create-query-key [index-name & {:as keys}]
  (QueryKey. index-name QueryType/EXACT_KEY (or keys {})))

(defn row->query-key [index-name row]
  (QueryKey. index-name QueryType/EXACT_KEY (.getRecords row)))

(defn count-results [scan]
  (count @(:rows scan)))
