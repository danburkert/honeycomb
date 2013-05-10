(ns com.nearinfinity.honeycomb.memory.memory-table-test
  (:require [clojure.test :refer :all]
            [com.nearinfinity.honeycomb.memory.test-util :refer :all]
            [com.nearinfinity.honeycomb.memory.memory-table :refer :all]
            [com.nearinfinity.honeycomb.memory.memory-store :as store])
  (:import [com.nearinfinity.honeycomb.exceptions RowNotFoundException]
           [com.nearinfinity.honeycomb.mysql Row]
           [com.nearinfinity.honeycomb.mysql.gen ColumnType]
           [java.util UUID]))


(def ^:private field-comparator
  (ns-resolve 'com.nearinfinity.honeycomb.memory.memory-table
              'field-comparator))

(def ^:private row-uuid-comparator
  (ns-resolve 'com.nearinfinity.honeycomb.memory.memory-table
              'row-uuid-comparator))

(def ^:private schema->row-index-comparator
  (ns-resolve 'com.nearinfinity.honeycomb.memory.memory-table
              'schema->row-index-comparator))

(deftest field-comparator-test
  (testing "signed longs"
    (are [pred field1 field2] (pred (field-comparator ColumnType/LONG
                                                      (long-bb field1)
                                                      (long-bb field2)))
         zero? 1 1
         neg? -1 1
         neg? 1 2
         pos? 2 1
         pos? -1 -2))
  (testing "doubles"
    (are [pred field1 field2] (pred (field-comparator ColumnType/DOUBLE
                                                      (double-bb field1)
                                                      (double-bb field2)))
         zero? 1 1
         neg? -1 1
         pos? 1 -1
         zero? 1.23 1.23
         neg? 1.123 1.124
         pos? -1.23 -1.24))
  (testing "strings"
    (are [pred field1 field2] (pred (field-comparator ColumnType/STRING
                                                      (string-bb field1)
                                                      (string-bb field2)))
         zero? "foo" "foo"
         neg? "fo" "foo"
         neg? "abc" "efg"
         pos? "foo" "fo"
         pos? "bcd" "abc"
         pos? "zxy" "abc"
         zero? "" ""))
  (testing "nulls"
    (are [pred field1 field2] (pred (field-comparator ColumnType/LONG
                                                      field1
                                                      field2))
         zero? nil nil
         neg? nil (long-bb 1)
         pos? (long-bb 1) nil)))

(deftest row-uuid-comparator-test
  (testing "uuids"
    (are [pred m1 l1 m2 l2] (pred (row-uuid-comparator
                                    (Row. {} (UUID. m1 l1))
                                    (Row. {} (UUID. m2 l2))))
         zero? 1 2 1 2
         zero? -1 -2 -1 -2
         pos? (Long/MAX_VALUE) (Long/MAX_VALUE) (Long/MIN_VALUE) (Long/MIN_VALUE)
         neg? -1 -1 0 0
         neg? 0 0 1 1
         pos? 0 0 -10 -10)))

(deftest row-index-comparator-test
  (testing "single column (LONG) index"
    (let [index-name "i1"
          table-schema (create-schema [{:name "c1" :type ColumnType/LONG}]
                                      [{:name index-name :columns ["c1"]}])
          row-index-comparator (schema->row-index-comparator index-name table-schema)]
      (are [pred fields1 m1 l1 fields2 m2 l2] (pred (row-index-comparator (Row. fields1 (UUID. m1 l1))
                                                                          (Row. fields2 (UUID. m2 l2))))
           zero? {"c1" (long-bb 1)} 0 0 {"c1" (long-bb 1)} 0 0
           pos? {"c1" (long-bb 1)} 1 0 {"c1" (long-bb 1)} 0 0
           pos? {"c1" (long-bb 10)} 0 0 {"c1" (long-bb 1)} 0 0
           neg? {"c1" (long-bb 1)} 0 0 {"c1" (long-bb 152)} 0 0
           neg? {"c1" (long-bb 13)} 0 0 {"c1" (long-bb 13)} 1 0
           neg? {} 0 0 {"c1" (long-bb 199)} 0 0
           neg? {"c1" nil} 0 0 {"c1" (long-bb 199)} 0 0)))

  (testing "double column (LONG, STRING) index"
    (let [index-name "i1"
          table-schema (create-schema [{:name "c1" :type ColumnType/LONG}
                                       {:name "c2" :type ColumnType/STRING :max-length 32}]
                                      [{:name index-name :columns ["c1" "c2"]}])
          row-index-comparator (schema->row-index-comparator index-name table-schema)]
      (are [pred fields1 m1 l1 fields2 m2 l2] (pred (row-index-comparator (Row. fields1 (UUID. m1 l1))
                                                                          (Row. fields2 (UUID. m2 l2))))
           zero? {"c1" (long-bb 1) "c2" (string-bb "foo")} 0 0 {"c1" (long-bb 1) "c2" (string-bb "foo")} 0 0
           neg? {"c1" (long-bb 0) "c2" (string-bb "foo")} 0 0 {"c1" (long-bb 1) "c2" (string-bb "foo")} 0 0
           neg? {"c1" (long-bb 1) "c2" (string-bb "fo")} 0 0 {"c1" (long-bb 1) "c2" (string-bb "foo")} 0 0
           neg? {"c1" (long-bb 1) "c2" (string-bb "a")} 0 0 {"c1" (long-bb 1) "c2" (string-bb "foo")} 0 0
           neg? {"c1" (long-bb 1) "c2" (string-bb "a")} 0 0 {"c1" (long-bb 1) "c2" (string-bb "foo")} 0 1
           neg? {"c1" (long-bb 1) "c2" (string-bb "a")} 0 0 {"c1" (long-bb 1) "c2" (string-bb "foo")} 0 0
           neg? {"c1" nil} 100 100 {"c1" (long-bb 33) "c2" (string-bb "foooz")} 0 0))))

(deftest scan-tests
  (let [index-name "i1"
        table-name "t1"
        table-schema (create-schema [{:name "c1" :type ColumnType/LONG}]
                                    [{:name index-name :columns ["c1"]}])
        store (store/memory-store)
        _ (.createTable store table-name table-schema)
        table (.openTable store table-name)
        rows [(create-row "c1" (long-bb 0))
              (create-row "c1" (long-bb 1))
              (create-row "c1" (long-bb 2))
              (create-row "c1" (long-bb 3))
              (create-row "c1" (long-bb 4))
              (create-row "c1" (long-bb 5))]]
    (dorun (map #(.insert table %) rows))

    (testing "table scan"
      (is (every? (set rows) @(:rows (.tableScan table))))
      (is (= (count-results (.tableScan table)) (count rows))))

    (testing "ascending index scan at"
      (let [query-key (create-query-key "i1" "c1" (long-bb 2))]
        (is (every? (set (nthnext rows 2)) @(:rows (.ascendingIndexScanAt table query-key))))
        (is (= (count-results (.ascendingIndexScanAt table query-key)) 4))))

    (testing "ascending index scan after"
      (let [query-key (create-query-key "i1" "c1" (long-bb 2))]
        (is (every? (set (nthnext rows 3)) @(:rows (.ascendingIndexScanAfter table query-key))))
        (is (= (count-results (.ascendingIndexScanAfter table query-key)) 3))))

    (testing "descending index scan before"
      (let [query-key (create-query-key "i1" "c1" (long-bb 2))]
        (is (every? (set (take 2 rows)) @(:rows (.descendingIndexScanAfter table query-key))))
        (is (= (count-results (.descendingIndexScanAfter table query-key)) 2))))

    (testing "descending index scan at"
      (let [query-key (create-query-key "i1" "c1" (long-bb 2))]
        (is (every? (set (take 3 rows)) @(:rows (.descendingIndexScanAt table query-key))))
        (is (= (count-results (.descendingIndexScanAt table query-key)) 3))))

    (testing "index scan exact"
      (let [query-key (create-query-key "i1" "c1" (long-bb 2))]
        (is (every? (set [(nth rows 2)]) @(:rows (.indexScanExact table query-key))))
        (is (= (count-results (.indexScanExact table query-key)) 1))))

    (testing "index scan exact with unused fields in query-key"
      (let [query-key (create-query-key "i1" "c1" (long-bb 2) "foo" (long-bb 99))]
        (is (every? (set [(nth rows 2)]) @(:rows (.indexScanExact table query-key))))
        (is (= (count-results (.indexScanExact table query-key)) 1))))))

(deftest get-test
  (let [table-name "t1"
        table-schema (create-schema [{:name "c1" :type ColumnType/LONG}] nil)
        store (store/memory-store)
        _ (.createTable store table-name table-schema)
        table (.openTable store table-name)
        rows [(create-row "c1" (long-bb 0))
              (create-row "c1" (long-bb 1))
              (create-row "c1" (long-bb 2))
              (create-row "c1" (long-bb 3))
              (create-row "c1" (long-bb 4))
              (create-row "c1" (long-bb 5))]]
    (dorun (map #(.insert table %) rows))

    (testing "returns inserted rows"
      (doseq [row rows]
        (let [uuid (.getUUID row)]
          (is (= (.get table uuid) row)))))

    (testing "Throws RowNotFoundException"
      (is (thrown? RowNotFoundException
                   (.get table (UUID/randomUUID)))))))

(deftest delete-test
  (let [table-name "t1"
        table-schema (create-schema [{:name "c1" :type ColumnType/LONG}] nil)
        store (store/memory-store)
        _ (.createTable store table-name table-schema)
        table (.openTable store table-name)
        rows [(create-row "c1" (long-bb 0))
              (create-row "c1" (long-bb 0))]]
    (dorun (map #(.insert table %) rows))

    (testing "removes row"
      (is (= (count-results (.tableScan table)) (count rows)))
      (.delete table (first rows))
      (is (= (count-results (.tableScan table)) (dec (count rows)))))

    (testing "remove all rows"
      (.deleteAllRows table)
      (is (= (count-results (.tableScan table)) 0))
      (dorun (map #(.insert table %) rows))
      (is (= (count-results (.tableScan table)) (count rows))))))

(deftest update-test
  (let [table-name "t1"
        table-schema (create-schema [{:name "c1" :type ColumnType/LONG}] nil)
        store (store/memory-store)
        _ (.createTable store table-name table-schema)
        table (.openTable store table-name)
        uuid (UUID/randomUUID)
        row (Row. {"c1" (long-bb 123)} uuid)
        row' (Row. {"c1" (long-bb 456)} uuid)]
    (.insert table row)

    (testing "updates row"
      (is (= (.get table uuid) row))
      (.update table row row' nil)
      (is (= (.get table uuid) row')))))

(deftest add-index
  (let [table-name "t1"
        table-schema (create-schema [{:name "c1" :type ColumnType/LONG}] nil)
        index-schema (create-index-schema {:name "i1" :columns ["c1"] :unique false})
        store (store/memory-store)
        _ (.createTable store table-name table-schema)
        table (.openTable store table-name)
        rows [(create-row "c1" (long-bb 0))
              (create-row "c1" (long-bb 0))]]
    (dorun (map #(.insert table %) rows))

    (testing "adding index"
      (is (= (count-results (.tableScan table)) (count rows)))
      (.addIndex store "t1" index-schema)
      (.insertTableIndex table index-schema)
      (is (= (count-results (.ascendingIndexScanAt table (create-query-key "i1" "c1" (long-bb 0))))
             (count rows))))))

(deftest multi-column-index
  (let [column-names ["c1" "c2"]
        index-name "i1"
        table-name "t1"
        table-schema (create-schema [{:name (first column-names) :type ColumnType/LONG}
                                     {:name (second column-names) :type ColumnType/LONG}]
                                    [{:name index-name :columns column-names}])
        store (store/memory-store)
        _ (.createTable store table-name table-schema)
        table (.openTable store table-name)
        rows [(create-row (first column-names) (long-bb 0) (second column-names) (long-bb 0))
              (create-row (first column-names) (long-bb 1) (second column-names) (long-bb 1))
              (create-row (first column-names) (long-bb 2) (second column-names) (long-bb 2))
              (create-row (first column-names) (long-bb 3) (second column-names) (long-bb 3))
              (create-row (first column-names) (long-bb 4) (second column-names) (long-bb 4))
              (create-row (first column-names) (long-bb 5) (second column-names) (long-bb 5))]]
    (dorun (map #(.insert table %) rows))

    (testing "table scan"
      (is (every? (set rows) @(:rows (.tableScan table))))
      (is (= (count-results (.tableScan table)) (count rows))))

    (testing "ascending index scan at"
      (let [query-key (create-query-key index-name (first column-names) (long-bb 2))]
        (is (every? (set (nthnext rows 2)) @(:rows (.ascendingIndexScanAt table query-key))))
        (is (= (count-results (.ascendingIndexScanAt table query-key)) 4))))

    (testing "index scan exact"
      (let [query-key (create-query-key index-name (first column-names) (long-bb 2))]
        (is (every? (set [(nth rows 2)]) @(:rows (.indexScanExact table query-key))))
        (is (= (count-results (.indexScanExact table query-key)) 1))))

    (testing "descending scan at"
      (let [query-key (create-query-key index-name (first column-names) (long-bb 2))]
        (is (every? (set (take 3 rows)) @(:rows (.descendingIndexScanAt table query-key))))
        (is (= (count-results (.descendingIndexScanAt table query-key)) 3))))))

(run-tests)
