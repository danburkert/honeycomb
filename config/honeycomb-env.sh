# Extra Java CLASSPATH elements.  Wildcards are not supported.
export HONEYCOMB_CLASSPATH=$HONEYCOMB_SOURCE/storage-engine-proxy/target/honeycomb-0.2-SNAPSHOT-jar-with-dependencies.jar:$HONEYCOMB_SOURCE/storage-engine-backends/hbase/target/honeycomb-hbase-0.2-SNAPSHOT-jar-with-dependencies.jar:$HONEYCOMB_SOURCE/storage-engine-backends/memory/target/honeycomb-memory-0.2-SNAPSHOT-jar-with-dependencies.jar:$CLASSPATH

# Extra Java runtime options.
export HONEYCOMB_JVM_OPTS="-XX:+UseConcMarkSweepGC -Xmx2g -ea -server -Xrs"

# Logging path for Honeycomb MySQL plugin
export HONEYCOMB_LOG_PATH=/var/log/mysql/honeycomb-c.log
