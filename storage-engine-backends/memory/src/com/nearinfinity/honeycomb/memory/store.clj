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

; Copyright 2013 Altamira Corporation.


(ns com.nearinfinity.honeycomb.memory.store
  (:require [com.nearinfinity.honeycomb.memory.table :as table])
  (:import [com.nearinfinity.honeycomb Store Table]
           [com.nearinfinity.honeycomb.mysql.schema TableSchema]
           [com.nearinfinity.honeycomb.exceptions TableNotFoundException]))

(defrecord MemoryStore [metadata tables]
  Store

  (openTable [this table-name]
    (if-let [table (get @tables table-name)]
      table
      (throw (TableNotFoundException. table-name))))

  (createTable [this table-name schema]
    (dosync
      (let [table (table/memory-table this table-name schema)]
        (alter tables assoc table-name table)
        (alter metadata assoc table-name {:rows 0 :autoincrement 0 :schema schema}))))

  (deleteTable [this table-name]
    (dosync
      (.openTable this table-name) ;; check table exists
      (alter tables dissoc table-name)
      (alter metadata dissoc table-name)))

  (renameTable [this cur-table-name new-table-name]
    (dosync
      (if-let [cur-table (get (ensure tables) cur-table-name)]
        (if-let [table-metadata (get (ensure metadata) cur-table-name)]
          (let [new-table (table/->MemoryTable this new-table-name (:rows cur-table) (:indices cur-table))]
            (alter tables assoc new-table-name new-table)
            (alter metadata assoc new-table-name table-metadata)
            (alter tables dissoc cur-table-name)
            (alter metadata dissoc cur-table-name))
          (throw (TableNotFoundException. cur-table-name)))
        (throw (TableNotFoundException. cur-table-name)))))

  ;; getSchema needs the dosync because it is called during other transactions
  ;; that need the ensure in order to make sure the table's schema does not
  ;; change out from under them.
  (getSchema [this table-name]
    (dosync
      (if-let [table-metadata (get (ensure metadata) table-name)]
        (:schema table-metadata)
        (throw (TableNotFoundException. table-name)))))


  (addIndex [this table-name index-schema]
    (dosync
      (.getSchema this table-name) ;; check table exists
      (alter metadata update-in [table-name :schema] (fn [^TableSchema schema]
                                                       (doto (.schemaCopy schema)
                                                         (.addIndices [index-schema]))))))

  (dropIndex [this table-name index-name]
    (dosync
      (.getSchema this table-name) ;; check table exists
      (alter metadata update-in [table-name :schema] (fn [^TableSchema schema]
                                                       (doto (.schemaCopy schema)
                                                         (.removeIndex index-name))))))

  (getAutoInc [this table-name]
    (if-let [table-metadata (get @metadata table-name)]
      (:autoincrement table-metadata)
      (throw (TableNotFoundException. table-name))))

  (setAutoInc [this table-name value]
    (dosync
      (if (contains? (ensure metadata) table-name)
        (alter metadata update-in [table-name :autoincrement] max value)
        (throw (TableNotFoundException. table-name)))))

  (incrementAutoInc [this table-name amount]
    (dosync
      (let [metadata-val (ensure metadata)]
        (if (contains? metadata-val table-name)
          (-> (alter metadata update-in [table-name :autoincrement] #(unchecked-add (long amount) (long %)))
              (get-in [table-name :autoincrement]))
          (throw (TableNotFoundException. table-name))))))

  (truncateAutoInc [this table-name]
    (dosync
      (if (contains? (ensure metadata) table-name)
        (alter metadata assoc-in [table-name :autoincrement] 1)
        (throw (TableNotFoundException. table-name)))))

  (getRowCount [this table-name]
    (if-let [metadatum (get @metadata table-name)]
      (:rows metadatum)
      (throw (TableNotFoundException. table-name))))

  (incrementRowCount [this table-name amount]
    (dosync
      (if (contains? (ensure metadata) table-name)
        (-> (alter metadata update-in [table-name :rows] + amount)
            (get-in [table-name :rows]))
        (throw (TableNotFoundException. table-name)))))

  (truncateRowCount [this table-name]
    (dosync
      (if (contains? (ensure metadata) table-name)
        (alter metadata assoc-in [table-name :rows] 0)
        (throw (TableNotFoundException. table-name))))))

(defn memory-store []
  (->MemoryStore (ref {}) (ref {})))
