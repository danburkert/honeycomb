# Honeycomb

Honeycomb is a MySQL storage engine which stores MySQL tables into an external data store, or backend.  Currently, Honeycomb includes backends for HBase, as well as an in-memory backend.  Honeycomb provides a high level Java interface so that developing additional backends is straight-forward, with no need to understand MySQL internals.

With Honeycomb, MySQL is able to take advantage of the scalability, fault-tolerance, and capacity of the underlying backend, while preserving the familiar relational data model and SQL interface.  The HBase backend provides access to the underlying data in offline map/reduce jobs without having to extract or duplicate the data.

Feature comparison between Honeycomb with the HBase backend, and InnoDB (the default MySQL storage engine):

||Honeycomb|InnoDB|
||---------|------|
|[cross table joins](https://dev.mysql.com/doc/refman/5.5/en/join.html)|&#x2713;|&#x2713;|
|[compound indices](https://dev.mysql.com/doc/refman/5.5/en/multiple-column-indexes.html)|&#x2713;|&#x2713;|
|[unique index constraints](https://dev.mysql.com/doc/refman/5.0/en/constraint-primary-key.html)|&#x2713;|&#x2713;|
|[auto increment columns](https://dev.mysql.com/doc/refman/5.5/en/example-auto-increment.html)|&#x2713;|&#x2713;|
|[stored programs & views](https://dev.mysql.com/doc/refman/5.5/en/stored-programs-views.html)|&#x2713;|&#x2713;|
|[transactions](https://dev.mysql.com/doc/refman/5.5/en/sql-syntax-transactions.html)| |&#x2713;|
|[foreign key constraints](https://dev.mysql.com/doc/refman/5.5/en/innodb-foreign-key-constraints.html)| |&#x2713;|
|automatic sharding|&#x2713;| |
|automatic replication & failover|&#x2713;| |
|Hadoop map/reduce integration|&#x2713;| |
|offline map/reduce bulkload|&#x2713;| |

## Performance

… he's so fast right now.

## Get Honeycomb

Honeycomb is hosted on [GitHub](https://www.github.com/nearinfinity/honeycomb).  Check out the [README](https://github.com/nearinfinity/honeycomb/blob/develop/README.md) for getting-started instructions, and the [Honeycomb wiki](https://github.com/nearinfinity/honeycomb/wiki) for documentation.

## License

The Honeycomb storage engine plugin (the C++ library) is released under [GPL v2.0](https://www.gnu.org/licenses/gpl-2.0.html).  The Honeycomb proxy and backends (the JVM libraries) are released under [Apache v2.0](https://www.apache.org/licenses/LICENSE-2.0.html).

## Contact

For issues and contributions, see the [contribution guidelines](https://github.com/nearinfinity/honeycomb/blob/develop/CONTRIBUTING.md).