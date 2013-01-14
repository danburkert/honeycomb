(ns benchmark.core
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [benchmark.query :as q]
            [clojure.tools.cli :as cli])
  (:gen-class))

(defn client
  "Simulated client.  Opens a connection to db and executes stmts on a separate
   thread while phase is :bench or :warmup.  Only records while in :bench phase.
   Returns a future containing a list of query start times offset from the
   beggining of the warmup period."
  [db table query-fn phase]
  (future
    (sql/with-connection db
      (loop [times []]
        (condp = @phase
          :bench (recur (conj times
                              (let [t (. System (nanoTime))]
                                (sql/with-query-results res
                                  (query-fn table)
                                  (dorun res)
                                  t))))
          :warmup (recur [])
          :stop (map #(/ (- % (first times)) 1000000000.0) times))))))

(defn benchmark
  "Run individual benchmark and return results."
  [db-spec table query-fn num-clients warmup bench]
  (let [phase (atom :warmup)
        clients (doall (for [_ (range num-clients)]
                         (client db-spec table query-fn phase)))]
    (do
      (Thread/sleep (* warmup 1000))
      (reset! phase :bench)
      (Thread/sleep (* bench 1000))
      (reset! phase :stop)
      (map deref clients))))

(defn benchmark-suite
  "Run benchmarks against different configurations of tables, number of
   concurrent client connections, and query type.  The database, warmup
   period, benchmark period, and output are consistent across runs."
  [tables queries clients db-spec warmup bench]
  (dorun
    (for [table tables
          query queries
          clients clients]
      (println (name table) bench (name query) clients))))

(defn -main [& args]
  (let [[cli-opts _ banner]
        (cli/cli args
                 ["-h" "--help" "Show this menu" :flag true :default false]
                 ["-t" "--tables" "Tables to run benchmarks against.  e.g. \"[:hc_05 :inno_05]\". Required." :parse-fn read-string]
                 ["-q" "--queries" "Query types to benchmark.  eg. \"[point-name range-salary]\". Required." :parse-fn read-string]
                 ["-d" "--db" "Database connection spec." :parse-fn read-string :default {:classname "com.mysql.jdbc.Driver"
                                                                                          :subprotocol "mysql"
                                                                                          :subname "//localhost:3306/hbase"
                                                                                          :user "root"}]
                 ["-w" "--warmup" "Warmup period in seconds." :default 10]
                 ["-b" "--bench" "Benchmark period in seconds." :default 30]
                 ["-c" "--clients" "Number of concurrent client connections.  Accepts multiple values for multiple runs." :default [5] :parse-fn read-string]
                 ["-o" "--out" "Path to file where results will be appended.  Defaults to writing output to stdout."]
                 ["-a" "--append" "Append to output file instead of overwrite." :flag true :default true]
                 ["--options" "Path to options file which contains an options map. File options override cli options."])
        opts (if (:options cli-opts)
               (merge cli-opts (read-string (slurp (:options cli-opts))))
               cli-opts)]
    (when (or (:help opts) (not (and (:tables opts) (:queries opts))))
      (do (println banner) #_(System/exit 0)))
    (let [queries (map (partial get (ns-publics 'benchmark.query)) (:queries opts))
          run-benchmarks #(benchmark-suite (:tables opts)
                                           queries
                                           (:clients opts)
                                           (:db opts)
                                           (:warmup opts)
                                           (:bench opts))]
      (if (:out opts)
        (with-open [writer (io/writer (:out opts) :append (:append opts))]
          (binding [*out* writer
                    *flush-on-newline* false]
            (run-benchmarks)))
        (run-benchmarks))
      (shutdown-agents))))
