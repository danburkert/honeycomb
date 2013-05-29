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

Honeycomb is an open-source storage engine for MySQL that allows MySQL to store and retrieve data from HBase.

## System Requirements

The following system requirements must be installed and configured for Honeycomb execution:

* Oracle Java 6
* Hadoop 
  * Apache 1.0+ or CDH 4.2+ 	
* HBase 
  * Apache 0.94+ or CDH 4.2+

## Getting Started


1. Download the Linux 64-bit [tarball](https://s3.amazonaws.com/Honeycomb/releases/mysql-5.5.31-honeycomb-0.1-SNAPSHOT-linux-64bit.tar.gz)
2. Run `tar xzf mysql-5.5.31-honeycomb-0.1-SNAPSHOT-linux-64bit.tar.gz`
3. Change directory to `mysql-5.5.31-honeycomb-0.1-SNAPSHOT`
4. Execute `run-mysql-with-honeycomb-installed.sh`


The complete commands are:

```
curl -O https://s3.amazonaws.com/Honeycomb/releases/mysql-5.5.31-honeycomb-0.1-SNAPSHOT-linux-64bit.tar.gz
tar xzf mysql-5.5.31-honeycomb-0.1-SNAPSHOT-linux-64bit.tar.gz
cd mysql-5.5.31-honeycomb-0.1-SNAPSHOT
./run-mysql-with-honeycomb-installed.sh
```

> *If desired, the project may be built from source using these [build instructions](https://github.com/nearinfinity/honeycomb/wiki/Building-From-Source)*

Once Honeycomb is up and running, test it out with:

```
create table foo (x int, y varchar(20)) character set utf8 collate utf8_bin engine=honeycomb;
insert into foo values (1, 'Testing Honeycomb');
select * from foo;
```

## What if HBase is not local?
To configure Honeycomb to use a remote HBase replace the following in `mysql-5.5.31-honeycomb-0.1-SNAPSHOT/honeycomb.xml` with your Zookeeper Quorum address

```
<hbase.zookeeper.quorum>{ZOOKEEPER QUORUM}</hbase.zookeeper.quorum>
```

## What if I don't have HBase?
To configure Honeycomb to run in-memory replace the following in `mysql-5.5.31-honeycomb-0.1-SNAPSHOT/honeycomb.xml`

```
<defaultAdapter>hbase</defaultAdapter>
```
with

```
<defaultAdapter>memory</defaultAdapter>
```






