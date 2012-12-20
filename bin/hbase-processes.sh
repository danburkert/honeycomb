#!/bin/sh

ps ax | grep "org.apache.hadoop.hbase" | grep -v "grep" | awk '{ print $1, $(NF-1) }'
