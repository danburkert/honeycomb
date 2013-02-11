(ns nearinfinity.honeycomb.benchmark.core
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [nearinfinity.honeycomb.benchmark.query :as q]
            [nearinfinity.honeycomb.benchmark.ddl :as ddl]
            [clojure.tools.cli :as cli])
  (:gen-class))

(defn- client
  "Simulated client.  Opens a connection to db and executes operations on a
   separate thread while phase is :bench or :warmup.  Only records while in
   :bench phase.  Returns a future containing a list of operation start times
   offset from the beggining of the benchmark period."
  [db-spec op phase]
  (future
    (sql/with-connection db-spec
      (loop [times []]
        (condp = @phase
          :bench (recur (conj times
                              (let [t (. System (nanoTime))]
                                (op)
                                t)))
          :warmup (do (op)
                      (recur []))
          :stop (map #(/ (- % (first times)) 1000000000.0) times))))))

(defn- benchmark
  "Run individual benchmark and return results."
  [db-spec op clients warmup bench]
  (let [phase (atom :warmup)
        clients (doall (for [_ (range clients)]
                         (client db-spec op phase)))]
    (do
      (Thread/sleep (* warmup 1000))
      (reset! phase :bench)
      (Thread/sleep (* bench 1000))
      (reset! phase :stop)
      (map deref clients))))

(defn- aggregate-timesteps
  "Bin results into timesteps.

   The number of timesteps is determined by the formula:
   # timesteps = benchmark period (in seconds) * resolution
   The QPS of a timestep is determined by the formula:
   QPS = # of queries started in timestep * resolution

   For instance, if the resolution is 2, and 4 queries are started in a given
   timestep, then the QPS of that timestep is 8.  If that was part of a 30
   second benchmark, there would be 60 individual timesteps."
  [times resolution]
  (let [timestep (fn [time] (int (* resolution time)))]
    (reduce (fn [acc [timestep times]] (assoc acc timestep
                                              (* resolution (count times))))
            {} (group-by timestep times))))

(defn- print-header []
  (binding [*print-readably* false]
    (prn "Table" "Query" "Clients" "Timestep" "OPS")))

(defn- print-results
  [results table query clients bench resolution]
  (binding [*print-readably* false]
    (dotimes [timestep (* bench resolution)]
      (prn (name table) query clients
           (float (/ timestep resolution)) (float (or (get results timestep) 0))))))

(defn- benchmark-queries
  "Run benchmarks against different configurations of tables, concurrent client
   connections, and query types.  The database, warmup period, benchmark period,
   and resolution are consistent across runs."
  [db-spec queries tables clients warmup bench resolution]
  (doseq [query queries
          table tables
          clients clients]
    (let [query-op (get (ns-publics 'nearinfinity.honeycomb.benchmark.query) query)]
      (-> (benchmark db-spec (query-op table) clients warmup bench)
          flatten
          (aggregate-timesteps resolution)
          (print-results table query clients bench resolution)))))

(defn- benchmark-ddls
  "Run benchmarks against different configurations of storage engines,
   concurrent client connections, and ddl statements.  The database, warmup
   period, benchmark period, and resolution are consistent across runs."
  [db-spec stmts engines clients warmup bench resolution]
  (doseq [stmt stmts
          engine engines
          clients clients]
    (let [stmt-op (get (ns-publics 'nearinfinity.honeycomb.benchmark.ddl) stmt)]
      (-> (benchmark db-spec (stmt-op engine) clients warmup bench)
          flatten
          (aggregate-timesteps resolution)
          (print-results engine stmt clients bench resolution)))))

(defn- xnor [a b]
  "Complement of logical XOR."
  (or (and a b)
      (not (or a b))))

(defn -main [& args]
  (let [[cli-opts _ banner]
        (cli/cli args
                 ["-h" "--help" "Show this menu" :flag true :default false]
                 ["-q" "--queries" "Query benchmarks,  eg. \"[point-name range-salary]\"." :parse-fn read-string]
                 ["-t" "--tables" "Tables to benchmark with queries, e.g. \"[:hc_05 :inno_05]\". Required." :parse-fn read-string]
                 ["-d" "--ddls" "DDL benchmarks, eg. \"[create-drop-table]\"." :parse-fn read-string]
                 ["-e" "--engines" "Storage engines to benchmark with DDLs, eg. \"[:Honeycomb :InnoDB]\"." :parse-fn read-string]
                 ["-d" "--db" "Database connection spec." :parse-fn read-string :default {:subprotocol "mysql"
                                                                                          :subname "//localhost:3306/hbase"
                                                                                          :user "root"}]
                 ["-w" "--warmup" "Warmup period in seconds." :default 10]
                 ["-b" "--bench" "Benchmark period in seconds." :default 30]
                 ["-r" "--resolution" "Number of collection periods per second." :parse-fn read-string :default 1]
                 ["-c" "--clients" "Number of concurrent client connections to benchmark." :default [5] :parse-fn read-string]
                 ["-o" "--out" "Path to file where results will be written.  Defaults to writing output to stdout."]
                 ["-a" "--append" "Append to output file instead of overwrite." :flag true :default true]
                 ["--options" "Path to options file which contains an options map. File options override cli options."])
        opts (if (:options cli-opts)
               (merge cli-opts (read-string (slurp (:options cli-opts))))
               cli-opts)]
    (when (and (:help opts)
               (xnor (:queries opts) (:tables opts))
               (xnor (:ddls opts) (:engines opts)))
      (do (println banner) (System/exit 0)))
    (with-open [writer (if (:out opts)
                         (io/writer (:out opts) :append (:append opts))
                         *out*)]
      (binding [*out* writer
                *flush-on-newline* false]
        (when-not (:append opts) (print-header))
        (if-let [queries (:queries opts)]
          (benchmark-queries (:db opts)
                             queries
                             (:tables opts)
                             (:clients opts)
                             (:warmup opts)
                             (:bench opts)
                             (:resolution opts)))
        (if-let [ddls (:ddls opts)]
          (benchmark-ddls (:db opts)
                          ddls
                          (:engines opts)
                          (:clients opts)
                          (:warmup opts)
                          (:bench opts)
                          (:resolution opts)))))
    (shutdown-agents)))
