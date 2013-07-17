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

; Copyright 2013 Near Infinity Corporation.


(ns com.nearinfinity.honeycomb.memory.table
  (:require [com.nearinfinity.honeycomb.memory.scanner :refer :all])
  (:import [java.util UUID]
           [com.nearinfinity.honeycomb Store Table]
           [com.nearinfinity.honeycomb.mysql.schema TableSchema IndexSchema]
           [com.nearinfinity.honeycomb.exceptions RowNotFoundException]
           [com.nearinfinity.honeycomb.mysql Row QueryKey]
           [com.nearinfinity.honeycomb.mysql.gen ColumnType QueryType]
           [com.google.common.primitives UnsignedBytes]
           [java.nio ByteBuffer]))

(defn- query-key->row-after
  "Takes a query key and returns the row that would fall directly after the
   query key in sorted order."
  [^QueryKey query-key column-schemas]
  (Row. (.getKeys query-key) (UUID. (Long/MAX_VALUE) (Long/MAX_VALUE)) column-schemas))

(defn query-key->row-before
  "Takes a query key and returns the row that would fall directly before the
   query key in sorted order."
  [^QueryKey query-key column-schemas]
  (Row. (.getKeys query-key) (UUID. (Long/MIN_VALUE) (Long/MIN_VALUE)) column-schemas))

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

(defmacro compare-byte-buffer [left right field]
  `(compare (.. ~left duplicate ~field) (.. ~right duplicate ~field)))

(defn- field-comparator [column-type ^ByteBuffer field1 ^ByteBuffer field2]
  (if (or (nil? field1) (nil? field2))
    (if (and (nil? field1) (nil? field2))
      0
      (if (nil? field1) -1 1))
    (condp = column-type
      ColumnType/LONG (compare-byte-buffer field1 field2 getLong)
      ColumnType/TIME (compare-byte-buffer field1 field2 getLong)
      ColumnType/DOUBLE (compare-byte-buffer field1 field2 getDouble)
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
  "Takes an index schema and query key and returns a predicate function that
   takes a row and returns true when the query-key's records match the records
   in the row.  Ignores any extraneous keys in the query-key."
  [^IndexSchema index-schema ^QueryKey query-key]
  (let [keys (.getKeys query-key)
        column-set (set (.getColumns index-schema))]
    (fn [^Row row]
      (let [records (.getRecords row)]
        (reduce (fn [result [column value]]
                  (if (column-set column)
                    (and result (= value (get records column)))
                    result))
                true
                keys)))))

;; Memory table holds a reference to a store, its table name, a ref which contains
;; a sorted set of its rows, and an indices ref which holds a map of index name to
;; sorted set of rows.
(defrecord MemoryTable [^Store store table-name rows indices]
  java.io.Closeable

  (close [this])

  Table

  (insertRow [this row]
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

  (updateRow [this oldRow newRow changed-indices]
    (dosync
      (.deleteRow this oldRow)
      (.insertRow this newRow)))

  (deleteRow [this row]
    (dosync
      (alter rows disj row)
      (alter indices update-indices disj row)))

  (deleteTableIndex [this index-schema]
    (dosync
      (alter indices dissoc (.getIndexName index-schema))))

  (flush [this])

  (getRow [this uuid]
    (let [column-schemas (.getSchema store table-name)]
      (if-let [row (@rows (Row. {} uuid column-schemas))]
        row
        (throw (RowNotFoundException. uuid)))))

  (tableScan [this]
    (->MemoryScanner (atom (seq @rows))))

  (ascendingIndexScan [this key]
    (let [index-name (.getIndexName key)]
      (->MemoryScanner (atom (seq (get @indices index-name))))))

  (ascendingIndexScanAt [this key]
    (let [index-name (.getIndexName key)
          column-schemas (.getSchema store table-name)
          start-row (query-key->row-before key column-schemas)
          rows (subseq (get @indices index-name) >= start-row)]
      (->MemoryScanner (atom rows))))

  (ascendingIndexScanAfter [this key]
    (let [index-name (.getIndexName key)
          start-row (query-key->row-after key)
          rows (subseq (get @indices index-name) > start-row)]
      (->MemoryScanner (atom rows))))

  (descendingIndexScan [this key]
    (let [index-name (.getIndexName key)]
      (->MemoryScanner (atom (rseq (get @indices index-name))))))

  (descendingIndexScanAt [this key]
    (let [index-name (.getIndexName key)
          index-schema (.. store (getSchema table-name) (getIndexSchema index-name))
          start-row (query-key->row-after key)
          eq-rows (reverse (take-while (query-key->row-pred index-schema key)
                                       (subseq (get @indices index-name) > start-row)))
          lt-rows (rsubseq (get @indices index-name) <= start-row)]
      (->MemoryScanner (atom (concat eq-rows lt-rows)))))

  (descendingIndexScanBefore [this key]
    (let [index-name (.getIndexName key)
          column-schemas (.getSchema store table-name)
          start-row (query-key->row-before key column-schemas)
          rows (rsubseq (get @indices index-name) < start-row)]
      (->MemoryScanner (atom rows))))

  (indexScanExact [this key]
    (let [index-name (.getIndexName key)
          index-schema (.. store (getSchema table-name) (getIndexSchema index-name))
          start-row (query-key->row-before key)
          rows (take-while (query-key->row-pred index-schema key)
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
        rows (sorted-set-by row-uuid-comparator)
        indices (reduce add-index {} (.getIndices table-schema))]
    (->MemoryTable store
                   table-name
                   (ref rows)
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
    (doseq [row rows] (.insertRow table row))
    (.ascendingIndexScanAt table (row->query-key "i1" (get rows 0))))

  (row->query-key "i1" (row))

  (let [table (table)
        rows [(row)]]
    (.indexScanExact table (row->query-key "i1" (first rows))))

  )
