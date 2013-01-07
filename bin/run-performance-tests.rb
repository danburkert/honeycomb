#!/usr/bin/ruby

### Globals FTW
@host = "nic-hadoop-smmc07"
@db = 'person'
@tables = ['inno_05', 'hc_05']

### QUERIES

def countQuery(table)
  "SELECT COUNT(*) FROM #{table};"
end

### RUN

def printBench(query, concurrency, iterations)
    `./mysqlslap --delimiter=";" --create="" --query="#{query}" --concurrency=#{concurrency} --iterations=#{iterations} --no-create --no-drop --create-schema=#{@db} --host=#{@host} --only-print --user="root"`
end

def runBench(query, concurrency, iterations)
  `./mysqlslap --delimiter=";" --create="" --query="#{query}" --concurrency=#{concurrency} --iterations=#{iterations} --no-create --no-drop --create-schema=#{@db} --host=#{@host} --user="root"`
end

def analyzeBench(query, concurrency, iterations)
  `./mysqlslap --delimiter=";" --create="" --query="#{query}" --concurrency=#{concurrency} --iterations=#{iterations} --no-create --no-drop --create-schema=#{@db} --host=#{@host} --user="root" | ./analyze-slap.r`
end

def runBenchmarks
  @tables.each do |table|
    puts("Table " + table + ":")
    puts analyzeBench(countQuery(table), 10, 3)
  end
end

def printBenchmarks
  @tables.each do |table|
    puts("Table " + table + ":")
    puts printBench(countQuery(table), 10, 3)
  end
end

runBenchmarks
