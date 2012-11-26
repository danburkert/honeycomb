Honeycomb
==================

```
      ,-.            __
      \_/         __/  \__
     {|||)<    __/  \__/  \__
      / \     /  \__/  \__/  \
      `-'     \__/  \__/  \__/
              /  \__/  \__/  \
              \__/  \__/  \__/
                 \__/  \__/
                    \__/

```


Supported Hadoop & HBase versions
---------------------------------

* Cloudera:
      * CDH 4.1.1

* Apache:
      * Hadoop 1.0.4
      * HBase 0.94.2
      * Hive 0.9.0

Building MySQL and Storage Engine Plugin
----------------------------------------

First step, add the following line to your .bashrc/.zshrc and restart your terminal:
    
    export HONEYCOMB_HOME=<path to git repository> # Very important, scripts key off this.
    export MYSQL_HOME=<path to mysql installation>

Second step, run the following:

    cd $HONEYCOMB_HOME/bin
    ./build.sh

MySQL will be installed into $MYSQL_HOME and should be running.
The MySQL plugin, MySQL and HBaseAdapter jar are built. 
A link is created from $HONEYCOMB_HOME/build/storage/cloud/ha_cloud.so to $MYSQL_HOME/lib/plugin/ha_cloud.so.
The honeycomb plugin has been installed in MySQL.
The HBaseAdapter jar will be copied into /usr/local/lib/honeycomb along with all of its dependencies. 
/etc/mysql/classpath.conf will be updated with the correct java path to /usr/local/lib/honeycomb/*.jar. 
By default, the /etc/mysql/adapter.conf will be setup to have zookeeper as the localhost.

To build and install the plugin alone:

    cd $HONEYCOMB_HOME/bin
    ./plugin-build-install.sh

To build and install HBaseAdapter alone:

    cd $HONEYCOMB_HOME/bin
    ./mvn-build-install.sh


Testing the Storage Engine Plugin
-----------------------------

Install cloud plugin tests:

    ln -s $HONEYCOMB_HOME/cloud/cloud-test $MYSQL_HOME/mysql-test/suite/

Note: The path to the test suite *must* be executable. An alternative is to place `$HONEYCOMB_HOME/cloud/cloud-test` in `/tmp`

Make sure HBase and MySQL are running and the cloud engine is installed, then:

    <mysql-cloud-engine filepath>/bin/run-tests.sh

How to prevent a certain test from running:
    
    cd $MYSQL_HOME/mysql-test/cloud-test/t
    vi disabled.def
    Add text after the ":" for the test you want disabled. (The chosen text is unimportant.)
