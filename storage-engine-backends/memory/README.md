# Memory Backend

An in-memory backend for the [Honeycomb Storage Engine](https://www.github.com/nearinfinity/honeycomb).

This backend allows you to use and test Honeycomb without having access to an HBase cluster.  When the MySQL server instance that is running Honeycomb shuts down, all data stored in the memory backend will be lost.  The table definitions will remain in MySQL, but any attempt to access them will result in an error (dropping the table will result in an error, but the table will be dropped).

## Building

The backend can be build with either maven or [leiningen](https://github.com/technomancy/leiningen):

```Bash
mvn clean compile package
# OR
lein do clean, compile
```

## Configuration

Add the following to your `honeycomb.xml`:

```XML
<adapter name="memory">
  <configuration>
  </configuration>
<adapter name="memory">
```

At this point the memory backend does not take any configuration parameters.

Any tables created in the `memory` database will automatically use the memory backend.  If you configure the `defaultAdapter` tag in `honeycomb.xml` to `memory`, then tables in other databases will use it as well.  For a more detailed explanation see the main Honeycomb documentation.  If you intend to run only the memory backend it is recommended that you configure the `defaultAdapter` as `memory`.

## Examples

```sql
use memory;
create table in-mem (x int, index(x)) engine=Honeycomb;
insert into in-mem values (123);
```

## Contributing

The memory backend is written in Clojure.  We maintain a `project.clj` for building with Leiningen which is conveinent for Clojure developers, as well as a `pom.xml` suitable for building directly from maven.  Please ensure that any changes to the version or dependencies take place in both files.

### Testing

The memory backend contains its own unit tests which can be run with `lein test`.  Additionally, the integration tests will be run against the memory backend iff the `defaultAdapter` is configured as `memory` in `honeycomb.xml`.  Please ensure these tests pass before contributing changes.

## License

Copyright © 2013 Altamira Corporation.

Distributed under the [Apache License](https://www.apache.org/licenses/LICENSE-2.0.html).
