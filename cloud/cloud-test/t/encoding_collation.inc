### Setup
--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings

### Create table
eval CREATE TABLE t1(c1 $type)
  DEFAULT CHARACTER SET $encoding COLLATE $collation
  ENGINE=cloud;

DROP TABLE t1;
