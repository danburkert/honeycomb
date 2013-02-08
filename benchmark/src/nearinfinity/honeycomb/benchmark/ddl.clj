(ns nearinfinity.honeycomb.benchmark.ddl
  (:require [clojure.java.jdbc :as sql]
            [clojure.data.generators :as gen])
  (:refer-clojure :exclude [name type]))

(defn- name []
  (let [ascii-alpha (concat (range 65 (+ 65 26))
                            (range 97 (+ 97 26)))
        ascii-char #(char (rand-nth ascii-alpha))
        sizer (+ 4 (rand-int 61))]
    (keyword (apply str (gen/reps sizer ascii-char)))))

(defn- type []
  (rand-nth [:TINYINT :SMALLINT :MEDIUMINT :INT    :BIGINT
             :DECIMAL :NUMERIC  :FLOAT     :DOUBLE :CHAR
             :BINARY  :BLOB     :TEXT]))

(defn- columns []
  (vec (gen/reps (gen/geometric 1/5) #(vector (name) (type)))))

(defn- table-spec [engine]
  (str "ENGINE="
       (clojure.core/name engine)
       " DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

;;; Create / Drop statements
(defn create-drop [engine]
  (fn []
    (let [table-name (name)]
      (try
        (do
          (apply sql/create-table
                 table-name
                 (concat (columns)
                         [:table-spec (table-spec engine)]))
          (sql/drop-table table-name))
        (catch Exception e (do (prn table-name) (prn columns)) (prn e))))))
