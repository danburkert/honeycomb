#!/usr/bin/env ruby

if ARGV.length != 2
  puts "Usage: #{$0} <input> <output>"
  exit 1
end
input = ARGV[0]
output = ARGV[1]
queries = []
times = []
File.open(input).each do |line|
  next if line !~ /^Query/
  _,query,*time = line.split(",")
  queries << query
  times << time
end
new_queries = queries.join(",")
new_times = times.transpose.map{|t| t.join(",") }.join("\n") 
new_file = !File.exists?(output)

File.open(output, "a") do |f|
  if new_file
    f.puts(new_queries)
  end

  f.puts(new_times)
end
