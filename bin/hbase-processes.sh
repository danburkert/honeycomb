#!/bin/bash

HBASE_PACKAGE="org.apache.hadoop.hbase"

# Set the default process status command to use
PROCESS_STATUS_CMD="ps ax"

# If jps is available, use it to list running Java processes
command -v jps >/dev/null 2>&1
if [ $? -eq 0 ]; then
    PROCESS_STATUS_CMD="jps -l"
fi


# Find all of the process ids that correspond to HBase
$PROCESS_STATUS_CMD | grep $HBASE_PACKAGE | grep -v "grep" | awk '{ print $1, $NF }'
