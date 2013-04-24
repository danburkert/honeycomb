(ns com.nearinfinity.honeycomb.memory.memory-store
  (:require [com.nearinfinity.honeycomb.memory.memory-table :as table])
  (:import [com.nearinfinity.honeycomb Store Table]
           [com.nearinfinity.honeycomb.exceptions TableNotFoundException]))

(defrecord MemoryStore [metadata tables]
  Store

  (openTable [this table-name]
    (let [table (get @tables table-name)]
      (if table
        table
        (throw (TableNotFoundException. table-name)))))

  (createTable [this table-name schema]
    (dosync
      (commute tables assoc table-name (table/memory-table this table-name))
      (commute metadata assoc table-name {:rows 0 :autoincrement 0 :schema schema})))

  (deleteTable [this table-name]
    (dosync
      (if (.openTable this table-name) ;; check table exists
        (do
          (commute tables dissoc table-name)
          (commute metadata assoc table-name))
        (throw (TableNotFoundException. table-name)))))

  (renameTable [this cur-table-name new-table-name]
    (sync
      (let [table (get @tables cur-table-name)
            metadatum (get @metadata cur-table-name)]
        (if (and table metadatum)
          (do
            (commute tables assoc new-table-name table)
            (commute metadata assoc new-table-name metadatum)
            (commute tables dissoc cur-table-name)
            (commute metadata dissoc cur-table-name))
          (throw (TableNotFoundException. table-name))))))

  (getSchema [this table-name]
    (let [metadatum (get @metadata table-name)]
      (if metadatum
        (:schema metadatum)
        (throw (TableNotFoundException. table-name)))))

  (addIndex [this table-name index-schema]
    (dosync
      (let [table-schema (.addIndices
                           (.schemaCopy (.getSchema this table-name))
                           [index-schema])]
        (commute metadata assoc table-name table-schema))))

  (dropIndex [this table-name index-name]
    (dosync
      (let [table-schema (.removeIndex
                           (.schemaCopy (.getSchema this table-name))
                           index-name)]
        (commute metadata assoc table-name table-schema))))

  (getAutoInc [this table-name]
    (if-let [metadatum (get @metadata table-name)]
      (:autoincrement metadatum)
      (throw (TableNotFoundException. table-name))))

  (setAutoInc [this table-name value]
    (dosync
      (if-let [metadatum (get @metadata table-name)]
        (let [value' (max (:autoincrement metadatum)
                          value)
              metadatum' (assoc metadatum :autoincrement value')]
          (commute metadata assoc table-name metadatum'))
        (throw (TableNotFoundException. table-name)))))

  (incrementAutoInc [this table-name amount]
    (dosync
      (if-let [metadatum (get @metadata table-name)]
        (let [value (+ (:autoincrement metadatum) amount)
              metadatum' (assoc metadatum :autoincrement value)]
          (commute metadata assoc table-name metadatum'))
        (throw (TableNotFoundException. table-name)))))

  (truncateAutoInc [this table-name]
    (dosync
      (if-let [metadatum (get @metadata table-name)]
        (let [metadatum' (assoc metadatum :autoincrement 0)]
          (commute metadata assoc table-name metadatum')))))

  (getRowCount [this table-name]
    (if-let [metadatum (get @metadata table-name)]
      (:rows metadatum)
      (throw (TableNotFoundException. table-name))))

  (incrementRowCount [this table-name amount]
    (dosync
      (if-let [metadatum (get @metadata table-name)]
        (let [value (+ (:rows metadatum) amount)
              metadatum' (assoc metadatum :rows value)]
          (commute metadata assoc table-name metadatum'))
        (throw (TableNotFoundException. table-name)))))

  (truncateRowCount [this table-name]
    (dosync
      (if-let [metadatum (get @metadata table-name)]
        (let [metadatum' (assoc metadatum :rows 0)]
          (commute metadata assoc table-name metadatum'))))))

(defn memory-store []
  (->MemoryStore (ref {}) (ref {})))
