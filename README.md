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



Building MySQL and Storage Engine Plugin
----------------------------------------

To build the custom storage engine, MySQL must be built as well (only
once, though).  MySQL should be made in the build/ directory so that it
will be excluded from the repository (gitignore is set up for build/).
Run the following commands from the root of the repository:


To keep CMake build files out of the git repository:

    mkdir build
    cd build
    cmake ../mysql-5.5.25a
    make
    cd ../

The built plugin will be at `mysql-5.5.25a/storage/cloud/ha_cloud.so`.

To compile the HBaseAdapter Jar:

    cd HBaseAdapter
    mvn package assembly:single

The compiled JAR will be at `HBaseAdapter/target/mysqlengine-0.1-jar-with-dependencies.jar`.


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

    ln -s <mysql-cloud-engine filepath>/build/storage/cloud/ha_cloud.so $MYSQL_HOME/lib/plugin/
    ln -s <mysql-cloud-engine filepath>/HBaseAdapter/target/mysqlengine-0.1-jar-with-dependencies.jar $MYSQL_HOME/lib/plugin

Symlink adapter.conf to /etc/mysql:

    mkdir /etc/mysql/
    ln -s <mysql-cloud-engine filepath>/HBaseAdapter/adapter.conf /etc/mysql/

Install cloud plugin tests:

    ln -s <mysql-cloud-engine filepath>/mysql-5.5.25a/storage/cloud/cloud-test $MYSQL_HOME/mysql-test/suite/

Test Storage Engine Plugin
--------------------------

Make sure HBase and MySQL are running and the cloud engine is installed, then:

    ./bin/run-tests.sh
