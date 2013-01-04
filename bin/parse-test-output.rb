#!/usr/bin/env ruby

input = ARGV[0]
output = ARGV[1]
lines = %x[grep " pass " #{input} | awk '{ print $1, $NF }'] 
tests = []
times = []
lines.each do |line|
  test,time = line.split(" ")
  tests << test
  times << time
end
new_tests = tests.join(",")
new_times = times.join(",")
if File.exists?(output)
  data = %x[tail +2 #{output}] + new_times
else
  data = new_times
end

File.open(output, "w") do |f|
  f.puts(new_tests)
  f.puts(data)
end
