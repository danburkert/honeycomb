{:tables [:inno_05 :inno_06 :hc_05 :hc_06]
 :warmup 10
 :bench 30
 :clients [1 8 16]
 :queries [range-salary point-name]
 :append false
 :out "benchmark.ssv"
 :db {:classname "com.mysql.jdbc.Driver"
      :subprotocol "mysql"
      :subname "//nic-hadoop-smmc07.nearinfinity.com:3306/person"
      :user "root"}
}
