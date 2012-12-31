require 'rubygems'
require 'mysql2'
require 'benchmark'

def seed_table(db, table)
  db.query("INSERT INTO " + table + " VALUES (RAND()*1000000, RAND()*1000000);")
  20.times do
    db.query("INSERT INTO " + table + " SELECT RAND()*1000000, RAND()*1000000 FROM " + table + ";")
  end
end

db = Mysql2::Client.new(:username => "root",
                        :database => "hbase",
                        :encoding => "utf8")

puts Benchmark.measure { seed_table(db, "randint_hc") }
