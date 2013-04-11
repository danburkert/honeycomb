(ns nearinfinity.honeycomb.benchmark.query
  (:require [clojureql.core :as ql]
            [clojure.java.jdbc :as sql]
            [nearinfinity.clj-faker.address :as a]
            [nearinfinity.clj-faker.phone :as p]
            [nearinfinity.clj-faker.name :as n]))

(defmacro wrap-query
  "Execute a query, and delay execution of query generator until just before used."
  [& body]
  `(fn []
     (sql/with-query-results res#
       ~@body
       (dorun res#))))

(defn combine-queries
  [table & queries]
  (fn []
    (((rand-nth queries) table))))

(defn- salary [] (rand-int 100000))
(defn- salary-range []
  (let [middle (salary)
        low (max 0 (- middle 100))
        high (min 99999 (+ middle 100))]
    [low high]))

;;; Point Queries

(defn point-name [table]
  (wrap-query
    (-> (ql/table table)
        (ql/select
          (ql/where (and (= :first_name (n/first-name))
                         (= :last_name (n/last-name)))))
        (ql/compile nil))))

(defn point-firstname [table]
  (wrap-query
    (-> (ql/table table)
        (ql/select
          (ql/where (= :first_name (n/first-name))))
        (ql/compile nil))))

(defn point-lastname [table]
  (wrap-query
    (-> (ql/table table)
        (ql/select
          (ql/where (= :last_name (n/last-name))))
        (ql/compile nil))))

(defn point-address [table]
  (wrap-query
    (-> (ql/table table)
        (ql/select
          (ql/where (and (= :address (a/street-address))
                         (= :zip (a/post-code))
                         (= :state (a/state-abbr))
                         (= :country (a/country)))))
        (ql/compile nil))))

(defn point-phone [table]
  (wrap-query
    (-> (ql/table table)
        (ql/select
          (ql/where (= :phone (p/phone-number))))
        (ql/compile nil))))

(defn point-salary [table]
  (wrap-query
    (-> (ql/table table)
        (ql/select
          (ql/where (= :salary (rand-int 100000))))
        (ql/compile nil))))

(defn point-queries [table]
  (combine-queries table point-name point-address point-salary))

;;; Range Queries

(defn range-salary [table]
  (wrap-query
    (let [[low high] (salary-range)]
      (-> (ql/table table)
          (ql/select
            (ql/where (and (> :salary low)
                           (< :salary high))))
          (ql/compile nil)))))

(defn iscan-firstname-asc [limit]
  (fn [table]
    (wrap-query
      (-> (ql/table table)
          (ql/select
            (ql/where (>= :first_name (n/first-name))))
          (ql/sort [:first_name#asc])
          (ql/take limit)
          (ql/compile nil)))))

(defn iscan-firstname-desc [limit]
  (fn [table]
    (wrap-query
      (-> (ql/table table)
          (ql/select
            (ql/where (<= :first_name (n/first-name))))
          (ql/sort [:first_name#desc])
          (ql/take limit)
          (ql/compile nil)))))

(defn iscan-firstname [table]
  (combine-queries table (iscan-firstname-asc 10) (iscan-firstname-desc 10)))

(defn iscan-address-asc [limit]
  (fn [table]
    (wrap-query
      (-> (ql/table table)
          (ql/select
            (ql/where (>= :address (a/street-address))))
          (ql/sort [:address#asc])
          (ql/take limit)
          (ql/compile nil)))))

(defn iscan-address-desc [limit]
  (fn [table]
    (wrap-query
      (-> (ql/table table)
          (ql/select
            (ql/where (<= :address (a/street-address))))
          (ql/sort [:address#desc])
          (ql/take limit)
          (ql/compile nil)))))

(defn iscan-address [table]
  (combine-queries table (iscan-address-asc 10) (iscan-address-desc 10)))

(defn iscan-salary-asc [limit]
  (fn [table]
    (wrap-query
      (-> (ql/table table)
          (ql/select
            (ql/where (>= :salary (salary))))
          (ql/sort [:salary#asc])
          (ql/take limit)
          (ql/compile nil)))))

(defn iscan-salary-desc [limit]
  (fn [table]
    (wrap-query
      (-> (ql/table table)
          (ql/select
            (ql/where (<= :salary (salary))))
          (ql/sort [:salary#desc])
          (ql/take limit)
          (ql/compile nil)))))

(defn iscan-salary [table]
  (combine-queries table (iscan-salary-asc 10) (iscan-salary-desc 10)))

(defn range-queries [table]
  (combine-queries table iscan-firstname iscan-address iscan-salary range-salary))

;;; Multi-Column Queries

(defn point-firstname-phone [table]
  (wrap-query
    (-> (ql/table table)
        (ql/select
          (ql/where (and (= :first_name (n/first-name))
                         (= :phone (p/phone-number)))))
        (ql/compile nil))))

(defn point-name-range-salary [table]
  (wrap-query
    (let [[low high] (salary-range)]
      (-> (ql/table table)
          (ql/select
            (ql/where (and (= :first_name (n/first-name))
                           (= :last_name (n/last-name))
                           (> :salary low)
                           (< :salary high))))
          (ql/compile nil)))))

(defn point-firstname-range-salary [table]
  (wrap-query
    (let [[low high] (salary-range)]
      (-> (ql/table table)
          (ql/select
            (ql/where (and (= :first_name (n/first-name))
                           (> :salary low)
                           (< :salary high))))
          (ql/compile nil)))))

(defn count-all [table]
  (wrap-query
    (-> (ql/table table)
        (ql/aggregate [:count/*])
        (ql/compile nil))))

(defn point-range-queries [table]
  (combine-queries table point-queries range-queries))
