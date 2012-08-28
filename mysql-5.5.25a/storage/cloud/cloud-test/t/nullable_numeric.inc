### Setup
--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings

### Create table
eval CREATE TABLE t1(c1 $numeric_type $sign_flag $null_flag)
  ENGINE=$engine_type;

### Insert Values

# Values within range
eval INSERT INTO t1 VALUES
  ($min_val),
  (0),
  ($mid_val),
  ($max_val),
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

UPDATE t1
  SET c1 = 0
  WHERE c1 IS NULL;

# Check update
SELECT * FROM t1 ORDER BY c1;

UPDATE t1
  SET c1 = 0
  WHERE c1 IS NULL
  LIMIT 2;

# Check update
SELECT * FROM t1 ORDER BY c1;

### Delete values

DELETE FROM t1
  WHERE c1 IS NULL
  ORDER BY c1;

# Check deletes
SELECT * FROM t1 ORDER BY c1;

### Cleanup
DROP TABLE t1;
