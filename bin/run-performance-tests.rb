#!/usr/bin/ruby

### Globals FTW
@host = "nic-hadoop-smmc07"
@db = 'person'
@tables = ['inno_05', 'hc_05']

## Some other sweet hacks
`chmod +x mysqlslap`
`chmod +x analyze-slap.r`

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

def runTests
  @tables.each do |table|
    puts("Table " + table + ":")
    puts runBench(countQuery(table), 10, 3)
  end
end

def printTests
  @tables.each do |table|
    puts("Table " + table + ":")
    puts analyzeBench(countQuery(table), 10, 3)
  end
end

runTests
