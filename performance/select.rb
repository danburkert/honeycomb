require 'rubygems'
require 'mysql2'
require 'better-benchmark'



#config = YAML.load_file(ARGV[0])
#perf_glob = File.join(File.expand_path(ARGV[1]), "*.sql")
#begin
  #db = Mysql.new(config['host'], config['user'], config['password'], config['database'])
  #perf_dir = Dir.glob(perf_glob)
#rescue Mysql::Error => e
  #puts e
  #exit 1
#end

db = Mysql2::Client.new(:username => "root",
                        :database => "hbase",
                        :encoding => "utf8")


def seed_table(db, table)
  db.query("INSERT INTO " + table + " VALUES (RAND()*1000000, RAND()*1000000);")
  5.times do
    db.query("INSERT INTO " + table + " SELECT RAND()*1000000, RAND()*1000000 FROM " + table + ";")
  end
end

#seed_table(db, "randint_hc")
#seed_table(db, "randint_inno")

def query_table(db, table)
  db.query("SELECT * FROM " + table+ " WHERE c1 > 500000 AND c2 < 500000;")
end


result = Benchmark.compare_realtime {
  query_table(db, "randint_hc");
}.with {
  query_table(db, "randint_inno");
}

Benchmark.report_on result
