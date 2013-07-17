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


(ns com.nearinfinity.honeycomb.memory.store-test
  (:require [clojure.test :refer :all]
            [com.nearinfinity.honeycomb.memory.test-util :refer :all]
            [com.nearinfinity.honeycomb.memory.store :refer :all])
  (:import [com.nearinfinity.honeycomb.mysql.gen ColumnType]
           [com.nearinfinity.honeycomb.exceptions TableNotFoundException]))

(def ^:dynamic store)
(def table-name "t")

(defn- bind-store [f]
  (let [new-store (memory-store)]
    (binding [store new-store]
      (f))))

(defn- create-table [f]
  (let [column-name "c"
        index-name "i"
        table-schema (create-schema [{:name table-name :type ColumnType/LONG}]
                                    [{:name index-name :columns [column-name]}])]
    (.createTable store table-name table-schema)
    (f)))

(deftest open-table
  (testing "table can be opened"
    (is (not (nil? (.openTable store table-name)))))
  (testing "non-existant table throws exception"
    (is (thrown? TableNotFoundException
                 (.openTable store "fooz")))))

(deftest delete-table
  (testing "table gets deleted"
    (.deleteTable store table-name)
    (is (thrown? TableNotFoundException
                 (.openTable store table-name)))
    (is (thrown? TableNotFoundException
                 (.getSchema store table-name))))
  (testing "non-existant table throws exception"
    (is (thrown? TableNotFoundException
                 (.deleteTable store "fooz")))))

(deftest rename-table
  (testing "table gets renamed"
    (let [new-name "t2"]
      (.renameTable store table-name new-name)
      (is (thrown? TableNotFoundException
                   (.openTable store table-name)))
      (is (thrown? TableNotFoundException
                   (.getSchema store table-name)))
      (is (not (nil? (.openTable store new-name))))
      (is (not (nil? (.getSchema store new-name))))))
  (testing "non-existant table throws exception"
    (is (thrown? TableNotFoundException
                 (.renameTable store "fooz" "barz")))))

(deftest get-schema
  (testing "returns schema"
    (is (not (nil? (.getSchema store table-name)))))
  (testing "non-existant table throws exception"
    (is (thrown? TableNotFoundException
                 (.getSchema store "fooz")))))

(deftest add-index
  (testing "adds index"
    (let [index-name "i2"
          index-schema (create-index-schema {:name index-name
                                             :columns ["c"]
                                             :unique true})]
      (.addIndex store table-name index-schema)
      (is (= index-schema)
          (-> (.getSchema store table-name)
              (.getIndexSchema index-name)))))
  (testing "non-existant table throws exception"
    (is (thrown? TableNotFoundException
                 (.addIndex store "fooz" nil)))))

(deftest drop-index
  (testing "drops index"
    (let [index-name "i2"
          index-schema (create-index-schema {:name index-name
                                             :columns ["c"]
                                             :unique true})]
      (.addIndex store table-name index-schema)
      (.dropIndex store table-name index-name)
      (is (thrown? Exception
                   (-> (.getSchema store table-name)
                       (.getIndexSchema index-name)))))
    (testing "non-existant table throws exception"
      (is (thrown? TableNotFoundException
                   (.addIndex store "fooz" nil))))))

(deftest auto-increment
  (testing "defaults to 0"
    (is (= 0 (.getAutoInc store table-name))))
  (testing "set auto increment"
    (let [new-value 99]
      (.setAutoInc store table-name new-value)
      (is (= new-value (.getAutoInc store table-name)))))
  (testing "increment auto increment"
    (let [initial 111
          increment 32]
      (.setAutoInc store table-name initial)
      (is (= (+ initial increment) (.incrementAutoInc store table-name increment)))))
  (testing "truncate auto increment"
    (.setAutoInc store table-name 123)
    (.truncateAutoInc store table-name)
    (is (= 1 (.getAutoInc store table-name))))
  (testing "autoIncrement upper boundary"
    (.setAutoInc store table-name Long/MAX_VALUE)
    (is (= Long/MAX_VALUE (.getAutoInc store table-name)))
    (.incrementAutoInc store table-name 1)
    (is (= Long/MIN_VALUE (.getAutoInc store table-name)))))

(use-fixtures :each
              bind-store
              create-table)
