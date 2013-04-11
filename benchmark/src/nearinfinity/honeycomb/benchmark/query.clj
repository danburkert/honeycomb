(ns nearinfinity.honeycomb.benchmark.query
  (:require [clojureql.core :as ql]
            [clojure.java.jdbc :as sql]
            [nearinfinity.clj-faker.address :as a]
            [nearinfinity.clj-faker.phone :as p]
            [nearinfinity.clj-faker.name :as n]))

(defn- salary [] (rand-int 100000))
(defn- salary-range []
  (let [middle (salary)
        low (max 0 (- middle 100))
        high (min 99999 (+ middle 100))]
    [low high]))

;;; Point Queries

(defn point-name [table]
  (-> (ql/table table)
      (ql/select
        (ql/where (and (= :first_name (n/first-name))
                       (= :last_name (n/last-name)))))
      (ql/compile nil)))

(defn point-firstname [table]
  (-> (ql/table table)
      (ql/select
        (ql/where (= :first_name (n/first-name))))
      (ql/compile nil)))

(defn point-lastname [table]
  (-> (ql/table table)
      (ql/select
        (ql/where (= :last_name (n/last-name))))
      (ql/compile nil)))

(defn point-address [table]
  (-> (ql/table table)
      (ql/select
        (ql/where (and (= :address (a/street-address))
                       (= :zip (a/post-code))
                       (= :state (a/state-abbr))
                       (= :country (a/country)))))
      (ql/compile nil)))

(defn point-phone [table]
  (-> (ql/table table)
      (ql/select
        (ql/where (= :phone (p/phone-number))))
      (ql/compile nil)))

(defn point-salary [table]
  (-> (ql/table table)
      (ql/select
        (ql/where (= :salary (rand-int 100000))))
      (ql/compile nil)))

(defn point-queries [table]
  ((rand-nth [point-name point-address point-salary ])
   table))

;;; Range Queries

(defn range-salary [table]
  (let [[low high] (salary-range)]
    (-> (ql/table table)
        (ql/select
          (ql/where (and (> :salary low)
                         (< :salary high))))
        (ql/compile nil))))

(defn iscan-firstname-asc [limit]
  (fn [table]
    (-> (ql/table table)
        (ql/select
          (ql/where (>= :first_name (n/first-name))))
        (ql/sort [:first_name#asc])
        (ql/take limit)
        (ql/compile nil))))

(defn iscan-firstname-desc [limit]
  (fn [table]
    (-> (ql/table table)
        (ql/select
          (ql/where (<= :first_name (n/first-name))))
        (ql/sort [:first_name#desc])
        (ql/take limit)
        (ql/compile nil))))

(defn iscan-firstname [table]
  ((rand-nth [(iscan-firstname-asc 10) (iscan-firstname-desc 10)])
   table))

(defn iscan-address-asc [limit]
  (fn [table]
    (-> (ql/table table)
        (ql/select
          (ql/where (>= :address (a/street-address))))
        (ql/sort [:address#asc])
        (ql/take limit)
        (ql/compile nil))))

(defn iscan-address-desc [limit]
  (fn [table]
    (-> (ql/table table)
        (ql/select
          (ql/where (<= :address (a/street-address))))
        (ql/sort [:address#desc])
        (ql/take limit)
        (ql/compile nil))))

(defn iscan-address [table]
  ((rand-nth [(iscan-address-asc 10) (iscan-address-desc 10)])
   table))

(defn iscan-salary-asc [limit]
  (fn [table]
    (-> (ql/table table)
        (ql/select
          (ql/where (>= :salary (salary))))
        (ql/sort [:salary#asc])
        (ql/take limit)
        (ql/compile nil))))

(defn iscan-salary-desc [limit]
  (fn [table]
    (-> (ql/table table)
        (ql/select
          (ql/where (<= :salary (salary))))
        (ql/sort [:salary#desc])
        (ql/take limit)
        (ql/compile nil))))

(defn iscan-salary [table]
  ((rand-nth [(iscan-salary-asc 10) (iscan-salary-desc 10)])
   table))

(defn range-queries [table]
  ((rand-nth [iscan-firstname iscan-address iscan-salary range-salary])
   table))

;;; Multi-Column Queries

(defn point-firstname-phone [table]
  (-> (ql/table table)
      (ql/select
        (ql/where (and (= :first_name (n/first-name))
                       (= :phone (p/phone-number)))))
      (ql/compile nil)))

(defn point-name-range-salary [table]
  (let [[low high] (salary-range)]
    (-> (ql/table table)
        (ql/select
          (ql/where (and (= :first_name (n/first-name))
                         (= :last_name (n/last-name))
                         (> :salary low)
                         (< :salary high))))
        (ql/compile nil))))

(defn point-firstname-range-salary [table]
  (let [[low high] (salary-range)]
    (-> (ql/table table)
        (ql/select
          (ql/where (and (= :first_name (n/first-name))
                         (> :salary low)
                         (< :salary high))))
        (ql/compile nil))))

(defn count-all [table]
  (-> (ql/table table)
      (ql/aggregate [:count/*])
      (ql/compile nil)))

(defn point-range-queries [table]
  ((rand-nth [point-queries range-queries]) table))
