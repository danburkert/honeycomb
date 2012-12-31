#!/usr/bin/env ruby

begin 
  require 'mysql'
rescue LoadError
  require 'rubygems'
  require 'mysql'
end
require 'yaml'
require 'optparse'

if ARGV.length < 2
  puts "Usage: #{$0} <path to database.yml> <path to perf sql scripts> [rounds = 1]"
  exit 1
end

rounds = 1
if ARGV.length == 3
  rounds = ARGV[2].to_i
end

module Enumerable
  def sum
    self.inject(0){|accum, i| accum + i }
  end

  def mean
    self.sum/self.length.to_f
  end

  def median
    n = self.length 
    if n.odd? 
      self[(n+1)/2]
    else
      (self[n/2] + self[(n+1)/2]) / 2
    end
  end

  def percentile(p)
    n = self.length
    percent = [((p / 100.0) * n + (1/2.0)), n-1].min.floor
    percent 
  end

  def sample_variance
    m = self.mean
    sum = self.inject(0){|accum, i| accum +(i-m)**2 }
    sum/(self.length - 1).to_f
  end

  def standard_deviation
    return Math.sqrt(self.sample_variance)
  end
end 
class Stats
  def initialize
    @data_lists = {} 
  end
  def add_data(file, data)
    @data_lists[file] = data
  end
  def print_stats(longest)
    @data_lists.each do |file, data|
      median = data.median
      mean = data.mean
      stddev = data.standard_deviation
      upper = data[data.percentile(90)]
      min = data.min
      max = data.max 
      printf "%-#{longest}s %s/%s/%s/%s/%s/%s\n" % ([file] + [median, mean, stddev, upper, min, max].map{|n| n.round(2) })
    end
  end
end
def run_perf_scripts(db, perf_dir, longest, rounds)
  stats = Stats.new
  perf_dir.each do | file |
    lines = File.open(file).read.gsub(/\n/, "").each_line(';') 
    elapsed_list = []
    rounds.times do |i|
      elapsed = 0
      lines.each do |line| 
        start_time = Time.now
        db.query(line)
        end_time = Time.now
        elapsed += (end_time - start_time)
      end
      elapsed *= 1000
      elapsed_list << elapsed
      printf "%-#{longest}s %s\n", File.basename(file), elapsed.round(2)
    end
    elapsed_list.sort!
    stats.add_data(File.basename(file), elapsed_list)
  end
  puts "Med/Avg/Stddev/90%/Min/Max"
  stats.print_stats(longest)
end

config = YAML.load_file(ARGV[0])
perf_glob = File.join(File.expand_path(ARGV[1]), "*.sql")
begin
  db = Mysql.new(config['host'], config['user'], config['password'], config['database'])
  perf_dir = Dir.glob(perf_glob)
  longest = perf_dir.map {|path| File.basename(path) }.concat(["File"]).max { |a,b| a.length <=> b.length }.length
  printf "%-#{longest}s %s\n", "File", "Time (milliseconds)"
  run_perf_scripts(db, perf_dir, longest, rounds)
rescue Mysql::Error => e
  puts e
  exit 1
end
