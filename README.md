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

Honeycomb is an open source MySQL storage engine which stores tables into an external data store, or backend.  Honeycomb currently supports storing tables to HBase and an in-memory store.  See the [project page](http://nearinfinity.github.io/honeycomb/) for more details.

## System Requirements

The following system requirements must be installed and configured for Honeycomb execution:

* Oracle Java 6
* Hadoop 
  * Apache 1.0+ or CDH 4.3+ 	
* HBase 
  * Apache 0.94.3+ or CDH 4.3+

## Getting Started
* [Build from source](https://github.com/nearinfinity/honeycomb/wiki/Building-From-Source)
* Install pre-built binaries (below)

**Install MySQL and Honeycomb from pre-built Linux binaries**
> *These binaries have been tested with CentOS 6.4 and Ubuntu 13.04*

1. Export the 'JAVA_HOME' environment variable to refer to the location of Java on your system
2. Download the Honeycomb Linux 64-bit [tarball](https://github.com/nearinfinity/honeycomb/wiki/Downloads)
3. Run `tar xzf mysql-5.5.31-honeycomb-0.1-linux-64bit.tar.gz`
4. Change directory to `mysql-5.5.31-honeycomb-0.1`

or, run the following in a shell:

```bash
curl -O https://s3.amazonaws.com/Honeycomb/releases/mysql-5.5.31-honeycomb-0.1-linux-64bit.tar.gz
tar xzf mysql-5.5.31-honeycomb-0.1-linux-64bit.tar.gz
cd mysql-5.5.31-honeycomb-0.1
```

**Configure Honeycomb**

Honeycomb reads its application configuration from the file `honeycomb.xml` that is located in the top level of the install binary.

* If using the HBase backend, add the following to each file `hbase-site.xml` on each HBase region server and restart the region servers:

```XML
  <property>
    <name>hbase.coprocessor.region.classes</name>
    <value>org.apache.hadoop.hbase.coprocessor.example.BulkDeleteEndpoint</value>
  </property>
```

* If connecting to a remote HBase cluster, change the value of the tag `hbase.zookeeper.quorum` in the HBase backend configuration section of `honeycomb.xml` to the quorum location.
* If you want to use the in-memory backend, change the value of the element `defaultAdapter` in `honeycomb.xml` to `memory`.

For more information on application configuration, refer to the [configuration](https://github.com/nearinfinity/honeycomb/wiki/Configuration-%26-Logging#configuration) page.

**Start MySQL**

5. Execute `run-mysql-with-honeycomb-installed.sh`
6. Execute `bin/mysql -u root --socket=mysql.sock` 

or,

```bash
./run-mysql-with-honeycomb-installed.sh
bin/mysql -u root --socket=mysql.sock --port=5630
```

Once Honeycomb is up and running, test it with:

```SQL
create table foo (x int, y varchar(20)) character set utf8 collate utf8_bin engine=honeycomb;
insert into foo values (1, 'Testing Honeycomb');
select * from foo;
```

## Documentation

* [User documentation](https://github.com/nearinfinity/honeycomb/wiki)
* [Developer documentation](https://github.com/nearinfinity/honeycomb/wiki/Developer-Resources)

## License

The Honeycomb storage engine plugin (the C++ library) is released under [GPL v2.0](https://www.gnu.org/licenses/gpl-2.0.html).

The Honeycomb storage proxy and backends (the JVM libraries) are released under [Apache v2.0](https://www.apache.org/licenses/LICENSE-2.0.html).

## Issues & Contributing

Check out the [contributor guidelines](https://github.com/nearinfinity/honeycomb/blob/develop/CONTRIBUTING.md)
