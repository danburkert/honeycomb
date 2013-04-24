(ns com.nearinfinity.honeycomb.memory.memory-table-test
  (:require [clojure.test :refer :all]
            [com.nearinfinity.honeycomb.memory.memory-table :refer :all])
  (:import [com.nearinfinity.honeycomb Table mysql.gen.ColumnType]
           [java.nio ByteBuffer]))

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

(def ^:private field-comparator
  (ns-resolve 'com.nearinfinity.honeycomb.memory.memory-table
              'field-comparator))

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
         zero? "" "")))

(run-tests)
