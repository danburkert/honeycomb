#!/usr/bin/env ruby

if ARGV.length != 2
  puts "Usage: #{$0} <input> <output>"
  exit 1
end
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
new_file = !File.exists?(output)

File.open(output, "a") do |f|
  if new_file
    f.puts(new_tests)
  end

  f.puts(new_times)
end
