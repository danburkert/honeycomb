(ns benchmark.analyze
  (:require [incanter.core :as inc]
            [incanter.charts :as charts]
            [incanter.datasets :as data]
            [incanter.stats :as stats]))

(def empty-dataset
  (inc/dataset [:table :query :clients :time :count]))

(defn- aggregate
  [times resolution]
  (let [bin (fn [time] (int (* resolution time)))]
    (reduce (fn [acc [k v]] (assoc acc (/ k resolution) (count v)))
            {} (group-by bin times))))

(defn add-data
  "Return a function which takes a data set and adds benchmark results to it."
  [table query clients results resolution]
  (fn [dataset]
    (inc/conj-rows
      dataset
      (for [[time count] (aggregate results resolution)]
        [table query clients time count]))))

(defn plot-dataset
  [dataset]
  (do
    (inc/with-data dataset
      (doto
        (charts/xy-plot :time :count :group-by :clients :legend true)
        (inc/view)))
    (inc/view dataset)
    (inc/with-data (->> dataset
                        (inc/$rollup :sum :count [:table :query :clients]))
      (inc/view (charts/bar-chart :clients :count :legend true)))))

(defn plot-timeseries
  [tables queries clients dataset]
  (for [table tables
        query queries]
    (inc/with-data (get (inc/$group-by [:table :query] dataset)
                        {:table table :query query})
      (doto
        (charts/line-chart :time :count :group-by :clients :legend true)
        (charts/set-title (str "clients\n" query "\n" (name table)))
        (charts/set-x-label (str "time (seconds)"))
        (charts/set-y-label (str "queries/second"))
        (inc/view)))))

(plot-timeseries tables queries clients data)

(defn plot-timeseries
  [tables queries clients dataset]
  (let [data (inc/$group-by [:table :query :clients] dataset)
        get-data (fn [t q c] (get data {:table t :query q :clients c}))
        chart (doto
                (charts/xy-plot :time :count :data empty-dataset :legend true)
                (charts/set-x-label (str "time (seconds)"))
                (charts/set-y-label (str "queries/second")))]
    (dorun
      (for [t tables
            q queries
            c clients]
        (charts/add-lines chart :time :count :data (get-data t q c)
                          :series-label (str t " " q " " c))))
    (inc/view chart)))


(def data @benchmark.core/dataset)
(def tables [:inno_05])
(def queries [#'benchmark.query/range-salary #'benchmark.query/point-name])
(def clients [1 10])

(plot-timeseries tables queries clients data)

(identity data)

(inc/view data)

(->> data
     (inc/$group-by [:table :query :clients]))

;(data/get-dataset :chick-weight)
;(inc/with-data (data/get-dataset :chick-weight)
  ;(inc/view (charts/xy-plot :Time :weight :group-by :Chick)))

;(get
  ;(->>  (data/get-dataset :hair-eye-color)
       ;(inc/$group-by [:hair :eye]))
  ;{:eye "green" :hair "black"})

;(get
  ;(->>  (data/get-dataset :hair-eye-color)
       ;(inc/$group-by [:hair :eye]))
  ;{:eye "green" :hair "black"})

;(inc/with-data (->>  (data/get-dataset :hair-eye-color)
                    ;(inc/$rollup :sum :count [:hair :eye]))
  ;(inc/view (charts/bar-chart :hair :count :group-by :eye :legend true)))
