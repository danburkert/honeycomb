#!/usr/bin/ruby

### Globals FTW
@host = "nic-hadoop-smmc07"
@db = 'person'
@tables = ['inno_05', 'hc_05',
          'inno_06', 'hc_06',
          'inno_07', 'hc_07']

## Some other sweet hacks
`chmod +x mysqlslap`

### QUERIES

def countQuery(table)
  "SELECT COUNT(*) FROM #{table};"
end

### RUN

def printBench(query, concurrency, iterations)
  `./mysqlslap --delimiter=";" --create="" --query="#{query}" --concurrency=#{concurrency} --iterations=#{iterations} --no-create --no-drop --create-schema=#{@db} --host=#{@host} --only-print`
end

def runBench(query, concurrency, iterations)
  `./mysqlslap --delimiter=";" --create="" --query="#{query}" --concurrency=#{concurrency} --iterations=#{iterations} --no-create --no-drop --create-schema=#{@db} --host=#{@host}`
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
    puts printBench(countQuery(table), 10, 3)
  end
end

printTests
