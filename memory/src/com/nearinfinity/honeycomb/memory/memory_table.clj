(ns com.nearinfinity.honeycomb.memory.memory-table
  (:require [com.nearinfinity.honeycomb.memory.memory-scanner :refer :all])
  (:import [java.util UUID]
           [com.nearinfinity.honeycomb Table]
           [com.nearinfinity.honeycomb.exceptions RowNotFoundException]
           [com.nearinfinity.honeycomb.mysql Row]
           [com.nearinfinity.honeycomb.mysql.gen ColumnType QueryType]
           [com.google.common.primitives UnsignedBytes]))

(defn- query-key->row-after
  "Takes a query key and returns the row that would fall directly after the
   query key in sorted order."
  [query-key]
  (Row. (.getKeys query-key) (UUID. (Long/MAX_VALUE) (Long/MAX_VALUE))))

(defn- query-key->row-before
  "Takes a query key and returns the row that would fall directly before the
   query key in sorted order."
  [query-key]
  (Row. (.getKeys query-key) (UUID. (Long/MIN_VALUE) (Long/MIN_VALUE))))

(defn- update-indices
  "Takes the map of indices, a function f, and arguments, and applies f to each
   index set with args, and returns the resulting map."
  [indices f & args]
  (reduce (fn [indices' index-name]
            (apply update-in indices' [index-name] f args))
          indices
          (keys indices)))

(defn- row-uuid-comparator [row1 row2]
  (compare (.getUUID row1)
           (.getUUID row2)))

(defn- field-comparator [column-type field1 field2]
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
      ColumnType/TIME  (compare (.getLong field1)
                                (.getLong field2))
      ColumnType/DOUBLE (compare (.getDouble field1)
                                 (.getDouble field2))
      (let [comparator (UnsignedBytes/lexicographicalComparator)
            bytes1 (.array field1)
            bytes2 (.array field2)]
        (.compare comparator bytes1 bytes2)))))

(defn- schema->row-index-comparator
  "Takes an index name and a table schema, and returns a comparator which takes
   two rows and compares them on their fields in index order, and finally, by UUID."
  [index-name table-schema]
  (let [index-schema (.getIndexSchema table-schema index-name)
        column-names (.getColumns index-schema)
        column-types (map (fn [column-name]
                            (-> table-schema (.getColumnSchema column-name) .getType))
                          column-names)
        column-names-types (map vector column-names column-types)]
    (fn [row1 row2]
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

;; MemoryTable must use deftype instead of defrecord because
;; the Table protocol has a get method.
;; See https://groups.google.com/forum/?fromgroups=#!topic/clojure/pdfj13ppwik
;;
;; Memory table holds a reference to a store, its table name, a ref which contains
;; a sorted set of its rows, and an indices ref which holds a map of index name to
;; sorted set of rows.
(deftype MemoryTable [store table-name rows indices]
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
        (commute indices assoc index-name
                 (into (sorted-set-by
                         (schema->row-index-comparator index-name table-schema))
                       rows)))))

  (update [this oldRow newRow changed-indices]
    (dosync
      (.delete this oldRow)
      (.insert this newRow)))

  (delete [this row]
    (dosync
      (commute rows disj row)
      (commute indices update-indices disj row)))

  (deleteTableIndex [this index-schema]
    (dosync
      (commute indices dissoc (.getIndexName index-schema))))

  (flush [this])

  (get [this uuid]
    (if-let [row (@rows (Row. {} uuid))]
      row
      (throw (RowNotFoundException. uuid))))

  (tableScan [this]
    (->MemoryScanner (atom @rows)))

  (ascendingIndexScanAt [this key]
    (let [index-name (.getIndexName key)
          start-row (query-key->row-before key)
          rows (subseq (get @indices index-name) >= start-row)]
      (->MemoryScanner (atom rows))))

  (ascendingIndexScanAfter [this key]
    (let [index-name (.getIndexName key)
          start-row (query-key->row-after key)
          rows (subseq (get @indices index-name) > start-row)]
      (->MemoryScanner (atom rows))))

  (descendingIndexScanAt [this key]
    (let [index-name (.getIndexName key)
          start-row (query-key->row-after key)
          rows (rsubseq (get @indices index-name) <= start-row)]
      (->MemoryScanner (atom rows))))

  (descendingIndexScanAfter [this key]
    (let [index-name (.getIndexName key)
          start-row (query-key->row-before key)
          rows (rsubseq (get @indices index-name) < start-row)]
      (->MemoryScanner (atom rows))))

  (indexScanExact [this key]
    (let [index-name (.getIndexName key)
          start-row (query-key->row-before key)
          end-row (query-key->row-after key)
          rows (subseq (get @indices index-name) >= start-row <= end-row)]
      (->MemoryScanner (atom rows))))

  (deleteAllRows [this]
    (dosync
      (commute rows empty)
      (commute indices update-indices empty))))

(defn memory-table [store table-name table-schema]
  (let [add-index (fn [indices index]
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

  (import [com.nearinfinity.honeycomb Table]
          [com.nearinfinity.honeycomb.mysql Row]
          [com.nearinfinity.honeycomb.mysql.gen ColumnType]
          [com.nearinfinity.honeycomb.mysql.schema ColumnSchema ColumnSchema$Builder IndexSchema TableSchema]
          [java.nio ByteBuffer]
          [java.util UUID])

  (defn- long-bb [n]
    (-> (ByteBuffer/allocate 8)
        (.putLong n)
        .rewind))

  (defn- double-bb [n]
    (-> (ByteBuffer/allocate 8)
        (.putDouble n)
        .rewind))

  (defn- string-bb [s]
    (-> s .getBytes ByteBuffer/wrap))

  (defn- create-schema [columns indices]
    (let [create-column (fn [{:keys [name type nullable autoincrement max-length scale precision]}]
                          (cond-> (ColumnSchema$Builder. name type)
                            (not nullable) (.setIsNullable true)
                            autoincrement (.setIsAutoIncrement true)
                            max-length (.setMaxLength max-length)
                            scale (.setScale scale)
                            precision (.setPrecision precision)
                            true .build))
          create-index (fn [{:keys [name columns unique] :or {unique false}}]
                         (IndexSchema. name columns unique))]
      (TableSchema. (map create-column columns)
                    (map create-index indices))))

  (defn- table []
    (let [store (com.nearinfinity.honeycomb.memory.memory-store/memory-store)
          table-name "t1"
          table-schema (create-schema [{:name "c1" :type ColumnType/LONG}]
                                      [{:name "i1" :columns ["c1"]}])]
      (.createTable store table-name table-schema)
      (.openTable store table-name)))

  (defn- row [& {:keys [n] :or {n (rand-int 1000)}}]
    (com.nearinfinity.honeycomb.mysql.Row. {"c1" (long-bb n)}
                                           (java.util.UUID/randomUUID)))

  (defn- row->query-key [index-name row]
    (com.nearinfinity.honeycomb.mysql.QueryKey. index-name
                                                QueryType/EXACT_KEY
                                                (.getRecords row)))

  (let [table (table)]
    (.insert table (row))
    table
    )

  (let [table (table)]
    (.insert table (row))
    (.insert table (row))
    (.insert table (row))
    (.tableScan table))

  (let [table (table)]
    (.insert table (row :n 1))
    (.insert table (row :n 2))
    (.insert table (row :n 3))
    (.insert table (row :n 4))
    (.insert table (row :n 5))
    (let [
          ;scan (.ascendingIndexScanAfter table (row->query-key "i1" (row :n 2)))
          scan (.tableScan table)
          ]
      (prn (.next scan))
      (prn (.next scan))
      (prn (.next scan))
      (prn (.next scan))
      (prn (.next scan))
      (prn (.next scan))
      (prn (.next scan))
      ))

  (let [rows (atom [1 2 3 4 5])]
    (swap! rows rest)
    @rows
    )
  )
