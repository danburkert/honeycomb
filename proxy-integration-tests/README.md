# Proxy Integration Tests

This project contains the integration test suite that is run by the storage engine proxy 
on a storage engine backend to ensure expected behavior from supported backend implementations.

## Running Tests

The integration tests are run by the [Maven Failsafe Plugin](https://maven.apache.org/surefire/maven-failsafe-plugin) and may be executed with:

```Bash
mvn verify
```

## Configuration

To run the test suite against a specific backend implementation, the `defaultAdapter` property in the
application configuration file must be configured to the desired backend.

For example, to run the integration tests against the HBase backend ensure that the `honeycomb.xml` file contains:

```XML
<defaultAdapter>hbase</defaultAdapter>
```

>*This will require that HBase is accessible to execute the test suite.*

## License

Copyright © 2013 [Near Infinity Corporation](https://www.nearinfinity.com).

Distributed under the [Apache License](https://www.apache.org/licenses/LICENSE-2.0.html).