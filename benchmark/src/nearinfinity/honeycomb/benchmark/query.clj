(ns nearinfinity.honeycomb.benchmark.query
  (:require [clojureql.core :as ql]
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

(defn point-fk [table]
  (-> (ql/table table)
      (ql/select
        (ql/where (= :fk (rand-int 10))))
      (ql/compile nil)))

;;; Range Queries

(defn range-salary [table]
  (let [[low high] (salary-range)]
    (-> (ql/table table)
        (ql/select
          (ql/where (and (> :salary low)
                         (< :salary high))))
        (ql/compile nil))))

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
