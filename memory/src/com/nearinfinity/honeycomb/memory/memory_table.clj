(ns com.nearinfinity.honeycomb.memory.memory-table
  (:require [com.nearinfinity.honeycomb.memory.memory-scanner :refer :all])
  (:import [java.util UUID]
           [com.nearinfinity.honeycomb Store Table]
           [com.nearinfinity.honeycomb.mysql.schema TableSchema IndexSchema]
           [com.nearinfinity.honeycomb.exceptions RowNotFoundException]
           [com.nearinfinity.honeycomb.mysql Row QueryKey]
           [com.nearinfinity.honeycomb.mysql.gen ColumnType QueryType]
           [com.google.common.primitives UnsignedBytes]
           [java.nio ByteBuffer]
           [java.math BigInteger]))

(defn- query-key->row-after
  "Takes a query key and returns the row that would fall directly after the
   query key in sorted order."
  [^QueryKey query-key]
  (Row. (.getKeys query-key) (UUID. (Long/MAX_VALUE) (Long/MAX_VALUE))))

(defn query-key->row-before
  "Takes a query key and returns the row that would fall directly before the
   query key in sorted order."
  [^QueryKey query-key]
  (Row. (.getKeys query-key) (UUID. (Long/MIN_VALUE) (Long/MIN_VALUE))))

(defn- update-indices
  "Takes the map of indices, a function f, and arguments, and applies f to each
   index set with args, and returns the resulting map."
  [indices f & args]
  (reduce (fn [indices' index-name]
            (apply update-in indices' [index-name] f args))
          indices
          (keys indices)))

(defn- row-uuid-comparator [^Row row1 ^Row row2]
  (compare (.getUUID row1)
           (.getUUID row2)))

(defn- field-comparator [column-type ^ByteBuffer field1 ^ByteBuffer field2]
  (if (or (nil? field1) (nil? field2))
    (if (and (nil? field1) (nil? field2))
      0
      (if (nil? field1) -1 1))
    (condp = column-type
      ColumnType/LONG  (let [comparison (compare (.getLong field1)
                                                 (.getLong field2))]
                         (.rewind field1)
                         (.rewind field2)
                         comparison)
      ColumnType/TIME  (let [comparison (compare (.getLong field1)
                                                 (.getLong field2))]
                         (.rewind field1)
                         (.rewind field2)
                         comparison)
      ColumnType/DOUBLE (let [comparison (compare (.getDouble field1)
                                                  (.getDouble field2))]
                          (.rewind field1)
                          (.rewind field2)
                          comparison)
      (let [comparator (UnsignedBytes/lexicographicalComparator)
            bytes1 (.array field1)
            bytes2 (.array field2)]
        (.compare comparator bytes1 bytes2)))))

(defn- schema->row-index-comparator
  "Takes an index name and a table schema, and returns a comparator which takes
   two rows and compares them on their fields in index order, and finally, by UUID."
  [index-name ^TableSchema table-schema]
  (let [index-schema (.getIndexSchema table-schema index-name)
        column-names (.getColumns index-schema)
        column-types (map (fn [column-name]
                            (-> table-schema (.getColumnSchema column-name) .getType))
                          column-names)
        column-names-types (map vector column-names column-types)]
    (fn [^Row row1 ^Row row2]
      (let [fields1 (.getRecords row1)
            fields2 (.getRecords row2)
            compare-reducer (fn [comparison [column-name column-type]]
                              (if (zero? comparison)
                                (field-comparator column-type
                                                  (get fields1 column-name)
                                                  (get fields2 column-name))
                                comparison))
            comparison (reduce compare-reducer 0 column-names-types)]
        (if (zero? comparison)
          (row-uuid-comparator row1 row2)
          comparison)))))

(defn- query-key->row-pred
  "Takes a query-key and returns a predicate function that takes a row and returns
   true when the query-key's records match the records in the row"
  [^QueryKey query-key]
  (let [keys (.getKeys query-key)]
    (fn [^Row row]
      (let [records (.getRecords row)]
        (reduce (fn [result [column value]]
                  (and result (= value (get records column))))
                true
                keys)))))

;; MemoryTable must use deftype instead of defrecord because
;; the Table protocol has a get method.
;; See https://groups.google.com/forum/?fromgroups=#!topic/clojure/pdfj13ppwik
;;
;; Memory table holds a reference to a store, its table name, a ref which contains
;; a sorted set of its rows, and an indices ref which holds a map of index name to
;; sorted set of rows.
(deftype MemoryTable [^Store store table-name rows indices]
  java.io.Closeable

  (close [this]
    (dosync
      (ref-set rows nil)
      (ref-set indices nil)))

  Table

  (insert [this row]
    (dosync
      (commute rows conj row)
      (commute indices update-indices conj row)))

  (insertTableIndex [this index-schema]
    (dosync
      (let [rows (ensure rows)
            table-schema (.getSchema store table-name)
            index-name (.getIndexName index-schema)]
        (alter indices assoc index-name
                 (into (sorted-set-by
                         (schema->row-index-comparator index-name table-schema))
                       rows)))))

  (update [this oldRow newRow changed-indices]
    (dosync
      (.delete this oldRow)
      (.insert this newRow)))

  (delete [this row]
    (dosync
      (alter rows disj row)
      (alter indices update-indices disj row)))

  (deleteTableIndex [this index-schema]
    (dosync
      (alter indices dissoc (.getIndexName index-schema))))

  (flush [this])

  (get [this uuid]
    (if-let [row (@rows (Row. {} uuid))]
      row
      (throw (RowNotFoundException. uuid))))

  (tableScan [this]
    (->MemoryScanner (atom (seq @rows))))

  (ascendingIndexScanAt [this key]
    (let [index-name (.getIndexName key)]
      (if (= (.getQueryType key) QueryType/INDEX_FIRST)
        (->MemoryScanner (atom (seq (get @indices index-name))))
        (let [start-row (query-key->row-before key)
              rows (subseq (get @indices index-name) >= start-row)]
          (->MemoryScanner (atom rows))))))

  (ascendingIndexScanAfter [this key]
    (let [index-name (.getIndexName key)
          start-row (query-key->row-after key)
          rows (subseq (get @indices index-name) > start-row)]
      (->MemoryScanner (atom rows))))

  (descendingIndexScanAt [this key]
    (let [index-name (.getIndexName key)]
      (if (= (.getQueryType key) QueryType/INDEX_LAST)
        (->MemoryScanner (atom (rseq (get @indices index-name))))
        (let [start-row (query-key->row-after key)
              eq-rows (reverse (take-while (query-key->row-pred key)
                                              (subseq (get @indices index-name) > start-row)))
              lt-rows (rsubseq (get @indices index-name) <= start-row)]
          (->MemoryScanner (atom (concat eq-rows lt-rows)))))))

  (descendingIndexScanAfter [this key]
    (let [index-name (.getIndexName key)
          start-row (query-key->row-before key)
          rows (rsubseq (get @indices index-name) < start-row)]
      (->MemoryScanner (atom rows))))

  (indexScanExact [this key]
    (let [index-name (.getIndexName key)
          start-row (query-key->row-before key)
          end-row (query-key->row-after key)
          rows (take-while (query-key->row-pred key)
                           (subseq (get @indices index-name) >= start-row))]
      (->MemoryScanner (atom rows))))

  (deleteAllRows [this]
    (dosync
      (alter rows empty)
      (alter indices update-indices empty))))

(defn memory-table [store table-name ^TableSchema table-schema]
  (let [add-index (fn [indices ^IndexSchema index]
                    (let [index-name (.getIndexName index)]
                      (assoc indices
                             index-name
                             (sorted-set-by
                               (schema->row-index-comparator index-name table-schema)))))
        indices (reduce add-index {} (.getIndices table-schema))]
    (->MemoryTable store
                   table-name
                   (ref (sorted-set-by row-uuid-comparator))
                   (ref indices))))

(comment

  (use 'com.nearinfinity.honeycomb.memory.test-util)
  (use 'com.nearinfinity.honeycomb.memory.memory-store)

  (defn- table []
    (let [store (memory-store)
          table-name "t1"
          table-schema (create-schema [{:name "c1" :type ColumnType/LONG}]
                                      [{:name "i1" :columns ["c1"]}])]
      (.createTable store table-name table-schema)
      (.openTable store table-name)))

  (defn- row [] (create-row "c1" (long-bb (rand-int 10000000))))

  (let [table (table)
        rows [(row)]]
    (doseq [row rows] (.insert table row))
    (.ascendingIndexScanAt table (row->query-key "i1" (get rows 0))))

  (row->query-key "i1" (row))

(let [table (table)
      rows [(row)]]
  (.indexScanExact table (row->query-key "i1" (first rows))))

  )
