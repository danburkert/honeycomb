#!/usr/bin/env ruby

paths = []
dirs = [ENV['HADOOP_HOME'], ENV['HBASE_HOME'], "#{ENV['M2_HOME']}/repository/com/nearinfinity"]
dirs.each do |dir|
  jar_glob = File.join(dir, "**/*.jar")
  Dir.glob(jar_glob).each do | file |
    paths << File.expand_path(file) 
  end
end

print paths.join(":")
