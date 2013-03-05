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
      * Hadoop 1.x
      * HBase 0.94.2
      * Hive 0.9.0

Building MySQL and Storage Engine Plugin
----------------------------------------

Add the following line to your .bashrc/.zshrc and restart your terminal:

    export HONEYCOMB_HOME=<path to git repository> # Very important, scripts key off this.
    export MYSQL_HOME=<path to mysql installation> # MySQL home doesn't have to exist before running this (usually $MYSQL_HOME = /usr/local/mysql)

Before building the storage engine, install Maven, libncurses and cmake. After installing Maven, set the M2_HOME or M3_HOME.


Run the following:

    cd $HONEYCOMB_HOME/bin
    ./build.sh

MySQL will be installed into $MYSQL_HOME and should be running.
The MySQL plugin, MySQL and HBaseAdapter jar are built.
A link is created from $HONEYCOMB_HOME/build/storage/honeycomb/ha_honeycomb.so to $MYSQL_HOME/lib/plugin/ha_honeycomb.so.
The honeycomb plugin has been installed in MySQL.


The HBaseAdapter jar will be copied into /usr/local/lib/honeycomb along with all of its dependencies.
/etc/mysql/honeycomb.xml will be created with the settings for the storage engine.

To build and install the plugin alone:

    cd $HONEYCOMB_HOME/bin
    ./plugin-build-install.sh

To build and install HBaseAdapter alone:

    cd $HONEYCOMB_HOME/bin
    ./mvn-build-install.sh

Note: MySQL can get into very strange states.

* Extremely large stack allocations (due to uninitialized variables) can make gdb attach to the MySQL process very slowly. To fix this restart your machine.
* On Mac OS X, if MySQL crashes, a large core dump file will appear in /cores. 


Testing the Storage Engine Plugin
-----------------------------

Install Honeycomb plugin tests:

    ln -s $HONEYCOMB_HOME/honeycomb/honeycomb-test $MYSQL_HOME/mysql-test/suite/

Note: The path to the test suite *must* be executable. An alternative is to place `$HONEYCOMB_HOME/honeycomb/honeycomb-test` in `/tmp`

Edit line 641 of `$MYSQL_HOME/mysql-test/lib/mtr_cases.pm`  to include `honeycomb` as an approved storage engine:

    my %builtin_engines = ('myisam' => 1, 'memory' => 1, 'csv' => 1, 'honeycomb' => 1);

Make sure HBase and MySQL are running and the Honeycomb engine is installed, then:

    $HONEYCOMB_HOME/bin/run-tests.sh

How to prevent a certain test from running:

    cd $MYSQL_HOME/mysql-test/honeycomb-test/t
    vi disabled.def
    Add text after the ":" for the test you want disabled. (The chosen text is unimportant.)

Go to nic-hadoop-admin:8111 to see the CI server.
