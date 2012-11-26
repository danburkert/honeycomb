#!/usr/bin/env ruby

paths = []
ARGV.each do |dir|
  jar_glob = File.join(dir, "**/*.jar")
  Dir.glob(jar_glob).each do | file |
    paths << File.expand_path(file)
  end
end

print paths.join(":")
