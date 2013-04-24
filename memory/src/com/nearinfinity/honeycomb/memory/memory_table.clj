(ns com.nearinfinity.honeycomb.memory.memory-table
  (:require [com.nearinfinity.honeycomb.memory.memory-scanner :refer :all])
  (:import [com.nearinfinity.honeycomb Table mysql.QueryKey]
           [com.nearinfinity.honeycomb.mysql.gen ColumnType QueryType]
           [com.google.common.primitives UnsignedBytes]
           [java.nio ByteBuffer]))

(defn- row->query-key
  "Takes a row and creates a query-key suitable for inserts."
  ;; TODO: currently we just make up an index-name, because it does not matter
  ;; for the query-key comparator (we assume they are the same).  This lets us
  ;; reuse the same query-key across all indices for insert.  Check if this is OK
  [index-name row]
  (QueryKey. "" QueryType/EXACT_KEY (.getRecords row)))


;; MemoryTable must use deftype instead of defrecord because
;; the Table protocol has a get method.
;; See https://groups.google.com/forum/?fromgroups=#!topic/clojure/pdfj13ppwik
;;
;; Memory table holds a reference to a store, its table name, a ref which contains
;; a vector of its rows, and an indices map which maps an index name to a ref containg
;; a sorted set of query-key to vector of rows.
(deftype MemoryTable [store table-name rows indices]
  Table

  (insert [this row]
    (dosync
      (let [table-schema (.getSchema store table-name)
            query-key (row->query-key row)]
        (commute rows conj row)
        (doseq [[_ index] indices]
          (commute index (fn [deref-index]
                           (update-in deref-index
                                      [query-key]
                                      #(conj % row))))))))

  (insertTableIndex [this index-schema])

  (update [this oldRow newRow changed-indices])

  (delete [this uuid])

  (deleteTableIndex [this index-schema])

  (flush [this])

  (get [this uuid])

  (tableScan [this]
    (->MemoryScanner (atom @rows)))

  (ascendingIndexScanAt [this key])

  (ascendingIndexScanAfter [this key])

  (descendingIndexScanAt [this key])

  (descendingIndexScanAfter [this key])

  (indexScanExact [this key])

  (deleteAllRows [this])
  )

(defn- row-uuid-comparator [row1 row2]
  (compare (.getUUID row1)
           (.getUUID row2)))

(defn- field-comparator [column-type field1 field2]
  (condp = column-type
    ColumnType/LONG  (compare (.getLong field1)
                              (.getLong field2))
    ColumnType/TIME  (compare (.getLong field1)
                              (.getLong field2))
    ColumnType/DOUBLE (compare (.getDouble field1)
                               (.getDouble field2))
    (let [comparator (UnsignedBytes/lexicographicalComparator)
          bytes1 (.array field1)
          bytes2 (.array field2)]
      (.compare comparator bytes1 bytes2))))

(defn- schema->query-key-comparator
  "Takes an index name and a table schema, and returns a query-key comparator for the index."
  [index-name table-schema]
  (let [index-schema (.getIndexSchema table-schema index-name)
        columns (.getColumns index-schema) ]
    (fn [query-key1 query-key2]
      (let [keys1 (.getKeys query-key1)
            keys2 (.getKeys query-key2)
            compare-reducer (fn [comparison column]
                              (if (not= comparison 0)
                                comparison
                                (field-comparator (-> table-schema (.getColumnSchema column) .getType)
                                                  (get keys1 column)
                                                  (get keys2 column))))]
        (reduce compare-reducer 0 columns)))))

(defn memory-table [store table-name table-schema]
  (let [add-index (fn [indices index]
                    (let [index-name (.getIndexName index)]
                      (assoc indices
                             index-name
                             (ref (sorted-set-by
                                    (schema->query-key-comparator index-name table-schema))))))
        indices (reduce add-index {} (.getIndices table-schema))]
    (->MemoryTable store
                   table-name
                   (ref (sorted-set-by row-uuid-comparator))
                   indices)))
