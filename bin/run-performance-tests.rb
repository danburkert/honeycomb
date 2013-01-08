#!/usr/bin/ruby

### Globals FTW
@host = "nic-hadoop-smmc07"
@db = 'person'
@tables = ['inno_05', 'hc_05']

### QUERIES

countQuery = lambda{|table| "SELECT COUNT(*) FROM #{table};"}
addressQuery = lambda{|table| "SELECT * FROM #{table} WHERE address < '500' AND zip > '3000' AND state='VT' AND country='Mexico';"}
firstNameQuery = lambda{|table| "SELECT * FROM #{table} WHERE first_name = 'Robert';"}

@queries = [addressQuery, firstNameQuery, countQuery ]

### RUN
def analyzeBench(query, concurrency, iterations)
  `./mysqlslap --delimiter=";" --create="" --query="#{query}" --concurrency=#{concurrency} --iterations=#{iterations} --no-create --no-drop --create-schema=#{@db} --host=#{@host} --user="root" | ./analyze-slap.r`
end

def runBenchmarks
  @queries.each do |query|
    puts("### Query: " + query.call("<table>"))
    @tables.each do |table|
      puts("## Table: " + table)
      puts analyzeBench(query.call(table), 10, 5)
    end
  end
end

runBenchmarks
