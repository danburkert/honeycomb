# Honeycomb

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


## Supported versions

* Cloudera:
      * CDH 4.2+

* Apache:
      * Hadoop 1.0+
      * HBase 0.94+
* MySQL 5.5.x


## Building From Source

### Dependencies

The following dependencies are necessary to build MySQL and Honeycomb from source.  Please be sure they are installed on the system before building.  All major package managers should include these packages.

* [libncurses](https://www.gnu.org/software/ncurses/)
* [Libxml2](http://www.xmlsoft.org/)
* [cmake](http://www.cmake.org/)
* C++ compiler
* [Java Development Kit](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Maven](https://maven.apache.org/)

### Build & Install

Building Honeycomb requires the [MySQL 5.5 source](https://dev.mysql.com/downloads/mysql/5.5.html#downloads) and [Honeycomb source](https://github.com/nearinfinity/honeycomb).  MySQL requires that plugins be built against the same version of MySQL that they will be run in, so ensure the MySQL source version you download matches your installed MySQL version.  If you install MySQL from source then you do not need to worry about version mismatches.

The Honeycomb build script requires a few environment variables to be set.  Run the following in the shell before running the build script, or add these lines to your `.bashrc` or `.zshrc`:

	export MYSQL_SOURCE=<path to MySQL source directory>
	export HONEYCOMB_SOURCE=<path to Honeycomb source directory>
	export MYSQL_HOME=<path to installed MySQL OR path to desired installation location>

`MYSQL_HOME` should be the path to a MySQL installation, or the path to the desired MySQL install location.  The build script will take care of the installation in the latter case.

Finally, run the build script.

	$HONEYCOMB_SOURCE/scripts/build.sh
	
### Configuration

The build script places a configuration file, `honeycomb.xml`, in `/usr/share/mysql/honeycomb`.  Edit it to reflect the location of your HBase cluster and other configuration specifics.