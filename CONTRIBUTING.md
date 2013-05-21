# Contributing to Honeycomb

Want to contribute to Honeycomb?  **Here's how you can help.**

## Reporting Issues

Bug reports and feature requests can be submitted to our [GitHub issues tracker](https://github.com/nearinfinity/honeycomb/issues).  In the case of a bug report, the following information is necessary for us to track down bugs:

* system information - including operating system and version, MySQL version, Honeycomb version, and Java version
* a reproducible test case - where possible this should include the SQL commands which recreate the issue (including commands to create any involved tables).
* log output from `honeycomb-c.log`, `honeycomb-java.log`, and your [MySQL error log](https://dev.mysql.com/doc/refman/5.5/en/error-log.html).

## Pull requests

We accept pull requests to the [Honeycomb GitHub repository](https://github.com/nearinfinity/honeycomb).  Please keep in mind the following when submitting pull requests:

* include a clear description of why the changes are necessary and what has been changed
* where possible, include unit tests covering the changes, preferably that failed pre-change
* commits should be as self-contained as possible, with useful messages
* pull requests should rebase cleanly against the `develop` branch

### Branches

* the `master` branch holds the latest release version of Honeycomb.
* the `develop` branch stages all changes to be included in the next release.

## Coding standards

### C++

Public functions/methods and non-obvious private methods should have [Doxygen](https://en.wikipedia.org/wiki/Doxygen) style documentation.  C/C++ code should conform to the following the style guidelines (if not specified below defer to the [Google C++ Style Guide](https://google-styleguide.googlecode.com/svn/trunk/cppguide.xml)):

* opening braces on own line
* two space indent
* UpperCamelCase for class names
* snake_case for variable names
* SCREAMING_SNAKE_CASE for macros

### Java

Public methods and non-obvious private methods should have [JavaDoc](https://en.wikipedia.org/wiki/Javadoc)) style documentation.  Java code should conform to the following style guidelines:

* opening braces on same line
* four space indent
* UpperCamelCase for class names
* lowerCamelCase for variable names
* SCREAMING_SNAKE_CASE for constants

### Exceptions

Honeycomb spawns a JVM inside a native process without exception handling, so catching and dealing with Java exceptions can be complex.  The guidelines below describe how and where Honeycomb throws and handles exceptions.

* The barrier for checked exceptions is between the interfaces Store, Table and Scanner. All exceptions coming out of those interfaces must be RuntimeExceptions.
* Exception logging should happen as close to the source as possible, with logging containing maximum detail, including source, reason, and stack trace. Log the context of the operation that caused the exception to occur.
* Typed runtime exceptions should only be added if they enable better error handling / recognition on the C++ side. I.E. a typed runtime exception should only be added if it translates to something other than a generic HA_ERROR code.
* IOExceptions should be wrapped in RuntimeIOException, which indicates to the C++ side an IOException occurred.
* Uncaught exceptions will bubble up to the C++ side and be caught and reported there.  Runtime exceptions of certain types will be handled specially (`TableNotFoundException`, `RowNotFoundException`, etc.)
* Every JNI call must check the JVM for exceptions upon return.

![cluster](https://a248.e.akamai.net/camo.github.com/a2bbdf6d93237b2748da59d52959d3c2b748d779/687474703a2f2f696d67732e786b63642e636f6d2f636f6d6963732f626f6e64696e672e706e67)
