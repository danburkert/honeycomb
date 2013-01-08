(ns benchmark.core
  (:require [clojure.java.jdbc :as sql]))

(defmacro time-call
  "Evaluates expr and returns the time it took in seconds."
  [expr]
  `(let [start# (. System (nanoTime))]
     (do
       ~expr
       (/ (double (- (. System (nanoTime)) start#)) 1000000000.0))))

(defn client
  "Simulated client.  Opens a connection to db and executes stmts on a separate
   thread while cont? atom is true.  Returns a future containing a list of
   completion times for individual statements."
  [db cont? query]
  (future
    (sql/with-connection db
      (loop [times []]
        (if @cont?
          (recur (conj times (time-call
                               (sql/with-query-results _ [query]))))
          times)))))

(defn benchmark
  [db query concurrency time]
  (let [cont (atom true)
        clients (doall (for [_ (range concurrency)]
                         (client db cont query)))]
    (do
      (Thread/sleep (* time 1000))
      (reset! cont false)
      (map deref clients))))

(def db {:classname "com.mysql.jdbc.Driver"
         :subprotocol "mysql"
         :subname "//localhost:3306/hbase"
         :user "root"
         :password ""})

(def query "SELECT COUNT(*) FROM inno_ri;")

(def b (benchmark db query 10 10))
