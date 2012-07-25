mysql-cloud-engine
==================

MySql storage engine for the cloud


Building MySQL and Storage Engine Plugin
----------------------------------------

To build the custom storage engine, MySQL must be built as well (only
once, though).  MySQL should be made in the build/ directory so that it
will be excluded from the repository (gitignore is set up for build/).
run the following commands from the root of the repository:
`mkdir build
cd build
cmake ../mysql-5.5.25a
make`

The built plugin will be at `mysql-5.5.25a/storage/stub/ha_stub.so`.
