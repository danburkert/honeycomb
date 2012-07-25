mysql-cloud-engine
==================

MySql storage engine for the cloud


Building MySQL and Storage Engine Plugin
----------------------------------------

To build the custom storage engine, MySQL must be built as well (only
once, though).  MySQL should be made in the build/ directory so that it
will be excluded from the repository (gitignore is set up for build/).
Aditionally, thrift must generate the cpp client files for the storage
engine to use.  run the following commands from the root of the repository:

`thrift -r --gen cpp -o mysql-5.5.25a/storage/cloud/ hbase_engine.thrift`

`mkdir build`

`cd build`

`cmake ../mysql-5.5.25a`

`make`

The built plugin will be at `mysql-5.5.25a/storage/cloud/ha_cloud.so`.

Install Storage Engine Plugin
-----------------------------
Move the generated ha_cloud.so into your MySQL installations plugin
directory (or alternatively symlink it).  The following commands will
work for a Homebrew MySQL installation:

`cp build/storage/cloud/ha_cloud.so /usr/local/Cellar/mysql/5.5.25a/lib/plugin/`
or,
`ln -s build/storage/cloud/ha_cloud.so /usr/local/Cellar/mysql/5.5.25a/lib/plugin/`


Compile Controller (Thrift Server)
----------------------------------

Generate thrift files (run from repo root directory):

`thrift -r --gen java -out controller/src/main/java/`

`hbase_engine.thrift`

`cd controller`

`mvn package`

`mvn assembly:single`

Run Controller
--------------
`java -jar controller/target/controller-1.0-SNAPSHOT-jar-with-dependencies.jar`

