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
      * CDH 4.2.1

* Apache:
      * Hadoop 1.x
      * HBase 0.94.2
      * Hive 0.9.0

Building MySQL and Storage Engine Plugin
----------------------------------------

Add the following line to your .bashrc/.zshrc and restart your terminal:

    export HONEYCOMB_HOME=<path to git repository> # Very important, scripts key off this.
    export MYSQL_HOME=<path to mysql installation> # MySQL home doesn't have to exist before running this (usually $MYSQL_HOME = /usr/local/mysql)
    export MYSQL_SOURCE_PATH=<path to MySQL source code> # Used for building the storage engine plugin

Before building the storage engine, install Maven, libncurses and cmake. After installing Maven, set the M2_HOME or M3_HOME.

Run the following:

    cd $HONEYCOMB_HOME/bin
    ./build.sh

MySQL will be installed into $MYSQL_HOME and should be running.
The MySQL plugin, MySQL and HBaseAdapter jar are built.
A link is created from $HONEYCOMB_HOME/build/storage/honeycomb/ha_honeycomb.so to $MYSQL_HOME/lib/plugin/ha_honeycomb.so.
The honeycomb plugin has been installed in MySQL.

To build and install the plugin alone:

    cd $HONEYCOMB_HOME/bin
    ./plugin-build-install.sh

To build and install HBaseAdapter alone:

    cd $HONEYCOMB_HOME/bin
    ./mvn-build-install.sh


Note: MySQL can get into very strange states.

* Extremely large stack allocations (due to uninitialized variables) can make gdb attach to the MySQL process very slowly. To fix this restart your machine.
* On Mac OS X, if MySQL crashes, a large core dump file will appear in /cores. 

## Manual Installation
### Install Dependencies
Honeycomb depends on LibAvro 1.7.4, it must be installed on the system which will run Honeycomb.  Download the Avro c 1.7.4 tarball from an [apache mirror](https://www.apache.org/dyn/closer.cgi/avro/), and install:

	wget http://mirror.sdunix.com/apache/avro/avro-1.7.4/c/avro-c-1.7.4.tar.gz
	tar zxf avro-c-1.7.4.tar.gz
	mkdir avro-c-1.7.4/build
	cd avro-c-1.7.4/build
	cmake .. -DCMAKE_INSTALL_PREFIX=/usr
	make
	sudo make install

### Install Honeycomb Storage Engine Plugin
MySQL is typically installed by the system's package manager (e.g., `aptitude` or `yum`), or in special circumstances, installed from source.  MySQL's build system requires plugins to be built from source as a part of building MySQL as a whole.  The build process produces a shared library object, `ha_honeycomb.so`, which can be loaded into any MySQL installation on the system, regardless of the installation method.  It is required that the version of MySQL that the plugin is built against be the same as the version the plugin is loaded into.  These instructions assume that Honeycomb is being loaded into a MySQL installation managed by the system's package manager.

Download the source tarball of the system's MySQL version from the [mysql website](https://www.mysql.com/downloads/mysql/), or through the package manager (shown below, the system's MySQL version must be the most recent).

On debian-based systems:

	sudo apt-get update
	apt-get source mysql-5.5

	
Link the honeycomb storage engine plugin source into MySQL's source directory so it will be built alongside MySQL, and build.  $HONEYCOMB_SOURCE is assumed to be the Honeycomb source directory, and $MYSQL_SOURCE the MySQL source directory

	ln -s $HONEYCOMB_SOURCE/honeycomb $MYSQL_SOURCE/storage/
	mkdir $MYSQL_SOURCE/build
	cd $MYSQL_SOURCE/build
	cmake ..
	make

Move the built `ha_honeycomb.so` into MySQL's plugin directory.  $MYSQL_HOME is assumed to be the MySQL installation directory (containing the `plugin` directory), typically `/usr/lib/mysql`.

	cp $MYSQL_SOURCE/build/storage/honeycomb/ha_honeycomb.so $MYSQL_HOME/plugin/


###  Setup System Directories
Honeycomb stores configuration and java libraries in `/usr/share/mysql/honeycomb`.  Create these directories and give the `mysql` user ownership (the `mysql` user should be substituted with the user who owns the MySQL process):

	sudo mkdir -p /usr/share/mysql/honeycomb
	sudo chown mysql:mysql /usr/share/mysql/honeycomb


### Install Honeycomb Java Libraries
Honeycomb relies on Java libraries to connect to HBase.  The Jar must be built and moved to the `/usr/share/mysql/honeycomb` directory:

	cd $HONEYCOMB_SOURCE/HBaseAdapater
	mvn clean package assembly:single
	cp $HONEYCOMB_SOURCE/HBaseAdapter/target/mysqlengine-0.1-jar-with-dependencies.jar /usr/share/mysql/honeycomb/

### Configure Honeycomb
Honeycomb requires its configuration file, `honeycomb.xml` to be placed in `/usr/share/mysql/honeycomb`, along with `honeycomb.xsd`.  An example `honeycomb.xml` can be found at `HONEYCOMB_SOURCE/honeycomb/honeycomb-example.xml`.

	cp $HONEYCOMB_SOURCE/honeycomb/honeycomb-example.xml /usr/share/mysql/honeycomb/honeycomb.xml
	cp $HONEYCOMB_SOURCE/honeycomb/honeycomb.xsd /usr/share/mysql/honeycomb/

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
