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

To build the custom storage engine, MySQL must be built as well (only
once, though).  MySQL should be made in the build/ directory so that it
will be excluded from the repository (gitignore is set up for build/).
Run the following commands from the root of the repository:


To keep CMake build files out of the git repository:

    mkdir build
    cd build
    cmake -DWITH_DEBUG=1 ../mysql-5.5.28
    make
    cd ../

The built plugin will be at `<mysql-cloud-engine filepath>/build/cloud/ha_cloud.so`.

To compile the HBaseAdapter Jar:

    cd HBaseAdapter
    mvn package assembly:single

The compiled JAR will be at `<mysql-cloud-engine filepath>/build/HBaseAdapter/target/mysqlengine-0.1-jar-with-dependencies.jar`.


Install Storage Engine Plugin
-----------------------------

Create a shell variable called MYSQL_HOME which points to your local mysql
install directory.  For example, if MySQL is installed from the repository
source (after running `make install` and symlinking /usr/local/mysql-<version>/
to /usr/local/mysql/), add the following line to your .bashrc and restart your
terminal:

    export MYSQL_HOME=/usr/local/mysql/

Create a symlink between ha_cloud.so and the compiled JAR and the plugins directory of your
MySQL install:

    ln -s <mysql-cloud-engine filepath>/build/stoarage/cloud/ha_cloud.so $MYSQL_HOME/lib/plugin/
    ln -s <mysql-cloud-engine filepath>/HBaseAdapter/target/mysqlengine-0.1-jar-with-dependencies.jar $MYSQL_HOME/lib/plugin

Symlink adapter.conf to /etc/mysql:

    mkdir /etc/mysql/
    ln -s <mysql-cloud-engine filepath>/HBaseAdapter/adapter.conf /etc/mysql/

Install cloud plugin tests:

    ln -s <mysql-cloud-engine filepath>/cloud/cloud-test $MYSQL_HOME/mysql-test/suite/

Test Storage Engine Plugin
--------------------------

Make sure HBase and MySQL are running and the cloud engine is installed, then:

    <mysql-cloud-engine filepath>/bin/run-tests.sh

How to prevent a certain test from running:
    
    cd $MYSQL_HOME/mysql-test/cloud-test/t
    vi disabled.def
    Add text after the ":" for the test you want disabled. (The chosen text is unimportant.)
