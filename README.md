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
 
* Apache:
  * Hadoop 1.0+
  * HBase 0.94+
* Cloudera:
  * CDH 4.2+
* MySQL 5.5.x
* Oracle Java 6

## Getting Started




1. Get [HBase](http://hbase.apache.org/) running locally
2. Download the Linux 64-bit [tarball](https://s3.amazonaws.com/Honeycomb/releases/mysql-5.5.31-honeycomb-0.1-SNAPSHOT-linux-64bit.tar.gz)
3. Run `tar xzf mysql-5.5.31-honeycomb-0.1-SNAPSHOT-linux-64bit.tar.gz`
4. Change directory to mysql-5.5.31-honeycomb-0.1-SNAPSHOT
5. Run `mysql-with-honeycomb.sh`


Configure `Honeycomb.xml`.  See [the wiki](https://github.com/nearinfinity/honeycomb/wiki/Configuration-%26-Logging) for details
The complete commands are:

```
curl -O https://s3.amazonaws.com/Honeycomb/releases/mysql-5.5.31-honeycomb-0.1-SNAPSHOT-linux-64bit.tar.gz
tar xzf mysql-5.5.31-honeycomb-0.1-SNAPSHOT-linux-64bit.tar.gz
cd mysql-5.5.31-honeycomb-0.1-SNAPSHOT
./mysql-with-honeycomb.sh
```

