#!/bin/sh

 ps ax | grep "org.apache.hadoop" | grep -v "grep" | grep -v "hbase" | awk '{ print $1, $NF }'
