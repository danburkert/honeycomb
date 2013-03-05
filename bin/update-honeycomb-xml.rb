#!/usr/bin/env ruby

require 'rubygems'
require 'nokogiri'
load File.dirname($0) + '/constants.rb'

if ARGV.length != 2
  puts "Usage: #{$0} <project home> <mysql engine jar>"
  exit 1
end

project_home = ARGV[0]
mysql_engine_jar = ARGV[1]
[project_home, mysql_engine_jar].each do |arg|
  if arg.nil? || arg.empty?
    puts "Arguments must have value"
    exit 1
  end
end

classpath = File.read("#{project_home}/HBaseAdapter/target/classpath")
xml = nil
File.open(HONEYCOMB_XML_PATH, "r") do |honeycomb_file|
  xml = Nokogiri::XML(honeycomb_file)
end

if xml.nil?
  puts "Could not read the xml file"
  exit 1
end

options_node = xml.at_css("options").at_css("jvmoptions")

options_node.children.each do |option|
  option.remove if Regexp.new("^" + CLASSPATH_PREFIX).match(option.content)
end

classpath_node = Nokogiri::XML::Node.new("jvmoption", xml)
classpath_node.content = "#{CLASSPATH_PREFIX}#{MYSQLENGINE_JAR + mysql_engine_jar}:#{classpath}"
options_node.add_child(classpath_node)

File.open(HONEYCOMB_XML_PATH, "w") do |honeycomb_file|
  honeycomb_file.write(xml.to_xml)
end
