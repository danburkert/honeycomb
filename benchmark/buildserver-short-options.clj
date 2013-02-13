{:tables [:inno_05 :hc_05 :inno_06 :hc_06]
 :warmup 30
 :bench 60
 :clients [1 8 16]
 :queries [iscan-firstname-10]
 :append false
 :out "benchmark-short.ssv"
 :db {:classname "com.mysql.jdbc.Driver"
      :subprotocol "mysql"
      :subname "//nic-hadoop-smmc07.nearinfinity.com:3306/person"
      :user "root"}
}
