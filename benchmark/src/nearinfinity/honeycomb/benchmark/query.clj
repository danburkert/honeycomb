(ns nearinfinity.honeycomb.benchmark.query
  (:require [clojureql.core :as ql]
            [clojure.java.jdbc :as sql]
            [nearinfinity.clj-faker.address :as a]
            [nearinfinity.clj-faker.phone :as p]
            [nearinfinity.clj-faker.name :as n]))

(defmacro wrap-query
  "Wrap a ClojureQL query in an op function suitable for passing to the client."
  [& body]
  `(fn []
     (sql/with-query-results res#
       ~@body
       (dorun res#))))

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

(defn point-fk [table]
  (wrap-query
    (-> (ql/table table)
        (ql/select
          (ql/where (= :fk (rand-int 10))))
        (ql/compile nil))))

;;; Range Queries

(defn range-salary [table]
  (wrap-query
    (let [[low high] (salary-range)]
      (-> (ql/table table)
          (ql/select
            (ql/where (and (> :salary low)
                           (< :salary high))))
          (ql/compile nil)))))

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
