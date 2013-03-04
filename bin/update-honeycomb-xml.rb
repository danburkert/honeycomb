#!/usr/bin/env ruby

require 'rubygems'
require 'nokogiri'

HONEYCOMB_XML_PATH = "/etc/mysql/honeycomb.xml"
CLASSPATH_PREFIX = "-Djava.class.path="
MYSQLENGINE_JAR =  "/usr/local/lib/honeycomb/mysqlengine-0.1.jar"

if ARGV.length == 0
  puts "Please specify the 'home' location of the project"
  exit 
elsif ARGV.length > 0
  project_home = ARGV[0]
end

begin
  honeycomb_file = File.open(HONEYCOMB_XML_PATH, "r+")
  classpath_file = File.open("#{project_home}/HBaseAdapter/target/classpath")

  classpath = classpath_file.gets
  xml = Nokogiri::XML(honeycomb_file)
  options_node = xml.at_css("options").at_css("jvmoptions")

  options_node.children.each do |option|
    option.remove if Regexp.new("^" + CLASSPATH_PREFIX).match(option.content)
  end

  classpath_node = Nokogiri::XML::Node.new("jvmoption", xml)
  classpath_node.content = CLASSPATH_PREFIX + MYSQLENGINE_JAR + ":" + classpath
  options_node.add_child(classpath_node)

  honeycomb_file.reopen(HONEYCOMB_XML_PATH, "w")
  honeycomb_file.write(xml.to_xml)
ensure
  classpath_file.close
  honeycomb_file.close
end

