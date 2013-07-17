 ; Licensed to the Apache Software Foundation (ASF) under one
 ; or more contributor license agreements.  See the NOTICE file
 ; distributed with this work for additional information
 ; regarding copyright ownership.  The ASF licenses this file
 ; to you under the Apache License, Version 2.0 (the
 ; "License"); you may not use this file except in compliance
 ; with the License.  You may obtain a copy of the License at
 ;
 ;   http://www.apache.org/licenses/LICENSE-2.0
 ;
 ; Unless required by applicable law or agreed to in writing,
 ; software distributed under the License is distributed on an
 ; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ; KIND, either express or implied.  See the License for the
 ; specific language governing permissions and limitations
 ; under the License.
 ;
 ; Copyright 2013 Near Infinity Corporation.


(ns com.nearinfinity.honeycomb.memory.test-util
  (:require [com.nearinfinity.honeycomb.memory.table :as table])
  (:import [com.nearinfinity.honeycomb.mysql Row QueryKey]
           [com.nearinfinity.honeycomb.mysql.gen QueryType]
           [com.nearinfinity.honeycomb.mysql.schema ColumnSchema ColumnSchema$Builder IndexSchema TableSchema]
           [java.nio ByteBuffer]
           [java.util UUID]))

(def ^:dynamic schema)

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

(defn create-row-with-uuid [schema uuid & {:as fields}]
  (Row. fields uuid schema))
(defn create-row [schema & {:as fields}]
  (Row. fields (UUID/randomUUID) schema))

(defn create-query-key [index-name & {:as keys}]
  (QueryKey. index-name QueryType/EXACT_KEY (or keys {})))

(defn row->query-key [index-name row]
  (QueryKey. index-name QueryType/EXACT_KEY (.getRecords row)))

(defn count-results [scan]
  (count @(:rows scan)))
