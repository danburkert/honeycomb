(ns benchmark.core
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as s]
            [benchmark.query :as q]))

(defmacro time-call
  "Evaluates expr and returns the time it took in seconds."
  [expr]
  `(let [start# (. System (nanoTime))]
     (do
       ~expr
       (/ (double (- (. System (nanoTime)) start#)) 1000000000.0))))

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
  [db-spec table query-fn num-clients warmup-p bench-p]
  (let [phase (atom :warmup)
        clients (doall (for [_ (range num-clients)]
                         (client db-spec table query-fn phase)))]
    (do
      (Thread/sleep (* warmup-p 1000))
      (reset! phase :bench)
      (Thread/sleep (* bench-p 1000))
      (reset! phase :stop)
      (map deref clients))))

(def db-spec {:classname "com.mysql.jdbc.Driver"
         :subprotocol "mysql"
         :subname "//localhost:3306/hbase"
         :user "root"
         :password ""})
