(ns com.nearinfinity.honeycomb.memory.memory-store
  (:require [com.nearinfinity.honeycomb.memory.memory-table :as table])
  (:import [com.nearinfinity.honeycomb Store Table]
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
        (commute tables assoc table-name table)
        (commute metadata assoc table-name {:rows 0 :autoincrement 0 :schema schema}))))

  (deleteTable [this table-name]
    (dosync
      (when (.openTable this table-name) ;; check table exists
        (commute tables dissoc table-name)
        (commute metadata assoc table-name))))

  (renameTable [this cur-table-name new-table-name]
    (dosync
      (let [table (get (ensure tables) cur-table-name)
            table-metadata (get (ensure metadata) cur-table-name)]
        (if (and table table-metadata)
          (do
            (commute tables assoc new-table-name table)
            (commute metadata assoc new-table-name table-metadata)
            (commute tables dissoc cur-table-name)
            (commute metadata dissoc cur-table-name))
          (throw (TableNotFoundException. cur-table-name))))))

  ;; getSchema needs the dosync because it is called during other transactions
  ;; that need the ensure in order to make sure the table's schema does not
  ;; change out from under them (ex. addIndex).
  (getSchema [this table-name]
    (dosync
      (if-let [table-metadata (get (ensure metadata) table-name)]
        (:schema table-metadata)
        (throw (TableNotFoundException. table-name)))))


  (addIndex [this table-name index-schema]
    (dosync
      (let [table-schema (.. this
                             (getSchema table-name)
                             (schemaCopy)
                             (addIndices [index-schema]))]
        (commute metadata assoc table-name table-schema))))

  (dropIndex [this table-name index-name]
    (dosync
      (let [table-schema (.. this
                             (getSchema table-name)
                             (schemaCopy)
                             (removeIndex index-name))]
        (commute metadata assoc table-name table-schema))))

  (getAutoInc [this table-name]
    (if-let [table-metadata (get @metadata table-name)]
      (:autoincrement table-metadata)
      (throw (TableNotFoundException. table-name))))

  (setAutoInc [this table-name value]
    (dosync
      (if (contains? (ensure metadata) table-name)
        (commute metadata update-in [table-name :autoincrement] max value)
        (throw (TableNotFoundException. table-name)))))

  (incrementAutoInc [this table-name amount]
    (dosync
      (if (contains? (ensure metadata) table-name)
        (get-in (commute metadata update-in [table-name :autoincrement] + amount)
                [table-name :autoincrement])
        (throw (TableNotFoundException. table-name)))))

  (truncateAutoInc [this table-name]
    (dosync
      (if (contains? (ensure metadata) table-name)
        (commute metadata assoc-in [table-name :autoincrement] 0)
        (throw (TableNotFoundException. table-name)))))

  (getRowCount [this table-name]
    (if-let [metadatum (get @metadata table-name)]
      (:rows metadatum)
      (throw (TableNotFoundException. table-name))))

  (incrementRowCount [this table-name amount]
    (dosync
      (if (contains? (ensure metadata) table-name)
        (commute metadata update-in [table-name :rows] + amount)
        (throw (TableNotFoundException. table-name)))))

  (truncateRowCount [this table-name]
    (dosync
      (if (contains? (ensure metadata) table-name)
        (commute metadata assoc-in [table-name :rows] 0)
        (throw (TableNotFoundException. table-name))))))

(defn memory-store []
  (->MemoryStore (ref {}) (ref {})))

(comment
  (defn- long-bb [n]
    (-> (java.nio.ByteBuffer/allocate 8)
        (.putLong n)
        .rewind))

  (defn- double-bb [n]
    (-> (java.nio.ByteBuffer/allocate 8)
        (.putDouble n)
        .rewind))

  (defn- string-bb [s]
    (-> s .getBytes java.nio.ByteBuffer/wrap))

  (defn- table-schema []
    (let [column-schemas [(-> (com.nearinfinity.honeycomb.mysql.schema.ColumnSchema$Builder.
                                "c1"
                                com.nearinfinity.honeycomb.mysql.gen.ColumnType/LONG) .build)]
          index-schemas []]
      (com.nearinfinity.honeycomb.mysql.schema.TableSchema. column-schemas index-schemas)))

  (defn- table []
    (let [store (memory-store)
          table-name "t1"
          table-schema (table-schema)]
      (.createTable store table-name table-schema)
      (.openTable store table-name)))

  (table)

  )
