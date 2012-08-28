### Setup
--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings

### Create table
eval CREATE TABLE t1(c1 $string_type $null_flag $index_flag)
  ENGINE=$engine_type;

### Insert Values

# Values within range
eval INSERT INTO t1 VALUES
  ($val1),
  ($val2),
  ($val3),
  ($val4),
  ($val5),
  ($val6),
  ($val7),
  ($val8),
  ($val9),
  ($val10),
  ($val11),
  ($val12),
  ($val13),
  ($comp_val),
  (NULL),
  (NULL),
  (NULL),
  (NULL);

### Read table
SELECT * FROM t1 WHERE c1 IS     NULL ORDER BY c1 ASC;
SELECT * FROM t1 WHERE c1 IS NOT NULL ORDER BY c1 DESC;

SELECT * FROM t1 WHERE c1 IS     NULL ORDER BY c1 ASC;
SELECT * FROM t1 WHERE c1 IS NOT NULL ORDER BY c1 DESC;

### Update Values
eval UPDATE t1
  SET c1 = $comp_val
  WHERE c1 IS NULL
  LIMIT 2;

# Check updates
SELECT * FROM t1 ORDER BY c1;

### Delete values

eval DELETE FROM t1
  WHERE c1 IS NULL
  ORDER BY c1;

# Check deletes
SELECT * FROM t1 ORDER BY c1;

### Cleanup
#DROP TABLE t1;
