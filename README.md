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

`thrift -r --gen cpp -o mysql-5.5.25a/storage/cloud/ hbase_engine.thrift
mkdir build
cd build
cmake ../mysql-5.5.25a
make`

The built plugin will be at `mysql-5.5.25a/storage/cloud/ha_cloud.so`.
