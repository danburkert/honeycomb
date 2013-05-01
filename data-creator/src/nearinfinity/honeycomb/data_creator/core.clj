(ns nearinfinity.honeycomb.data-creator.core
  (:require [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [nearinfinity.clj-faker.name :as name]
            [nearinfinity.clj-faker.address :as address]
            [nearinfinity.clj-faker.phone :as phone]
            )
  (:gen-class))
(def id-gen (atom 0))

(defn next-id []
  (let [next @id-gen]
    (swap! id-gen inc)))

(defn gen-fake [column]
  (condp = column
    :id (next-id)
    :name (name/name)
    :first-name (name/first-name)
    :last-name (name/last-name)
    :address (address/street-address)
    :state (address/state)
    :country (address/country)
    :zip (address/post-code)
    :phone (phone/phone-number)
    :salary (rand-int 100000)
    :fk (rand-int 10)))

(defn print-row [columns]
  (binding [*print-readably* false]
    (->> columns
         (map gen-fake)
         (interpose ",")
         (apply str)
         prn)))

(defn print-rows [columns rows]
  (dotimes [_ rows]
    (print-row columns)))

(defn -main [& args]
  (let [[cli-opts _ banner]
        (cli/cli args
                 ["-h" "--help" "Show this menu" :flag true :default false]
                 ["-c" "--columns" "Column types to output in CSV form.  Required. In form: {:columns [:first-name :last-name]}"]
                 ["-r" "--rows" "Number of rows to output." :default 1000 :parse-fn #(Integer. %)]
                 ["-o" "--out" "Path to file where results will be appended.  Defaults to writing output to stdout."]
                 ["-a" "--append" "Append to output file instead of overwrite." :flag true :default true]
                 ["--options" "Path to options file which contains an options map. File options override cli options."])
        opts (if (:options cli-opts)
               (merge cli-opts (read-string (slurp (:options cli-opts))))
               cli-opts)]
    (when (or (:help opts) (not (:columns opts)))
      (do (println banner) (System/exit 0)))
    (if (:out opts)
      (with-open [writer (io/writer (:out opts) :append (:append opts))]
        (binding [*out* writer
                  *flush-on-newline* false]
          (print-rows (:columns opts) (:rows opts))))
      (print-rows (:columns opts) (:rows opts)))))
