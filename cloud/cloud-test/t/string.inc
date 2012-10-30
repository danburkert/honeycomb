### Setup
--disable_warnings
DROP TABLE IF EXISTS t1;
--enable_warnings

### Create table
eval CREATE TABLE t1(c1 $string_type $null_flag $index_flag)
  DEFAULT CHARACTER SET utf8 COLLATE utf8_bin
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
  ($comp_val),
  ($comp_val),
  ($comp_val);

### Read table
SELECT * FROM t1 ORDER BY c1 ASC;
SELECT * FROM t1 ORDER BY c1 DESC;

eval SELECT * FROM t1 WHERE c1 >  $comp_val ORDER BY c1 ASC;
eval SELECT * FROM t1 WHERE c1 >= $comp_val ORDER BY c1 ASC;
eval SELECT * FROM t1 WHERE c1 <  $comp_val ORDER BY c1 ASC;
eval SELECT * FROM t1 WHERE c1 <= $comp_val ORDER BY c1 ASC;
eval SELECT * FROM t1 WHERE c1 =  $comp_val ORDER BY c1 ASC;

eval SELECT * FROM t1 WHERE c1 >  $comp_val ORDER BY c1 DESC;
eval SELECT * FROM t1 WHERE c1 >= $comp_val ORDER BY c1 DESC;
eval SELECT * FROM t1 WHERE c1 <  $comp_val ORDER BY c1 DESC;
eval SELECT * FROM t1 WHERE c1 <= $comp_val ORDER BY c1 DESC;
eval SELECT * FROM t1 WHERE c1 =  $comp_val ORDER BY c1 DESC;

### Update Values
eval UPDATE t1
  SET c1 = $val7
  WHERE c1 = $comp_val
  ORDER BY c1
  LIMIT 2;

# Check updates
SELECT * FROM t1 ORDER BY c1;

### Delete values

eval DELETE FROM t1
  WHERE c1 = $comp_val
  ORDER BY c1;

# Check deletes
SELECT * FROM t1 ORDER BY c1;

### Cleanup
DROP TABLE t1;

eval CREATE TABLE t1(c1 $string_type $null_flag, c2 $string_type $null_flag $multi_index_flag)
  DEFAULT CHARACTER SET utf8 COLLATE utf8_bin
  ENGINE=$engine_type;
eval INSERT INTO t1 VALUES
  ($comp_val, $val1),
  ($comp_val, $val2),
  ($comp_val, $val3),
  ($comp_val, $val4),
  ($comp_val, $val5),
  ($comp_val, $val6),
  ($comp_val, $val7),
  ($comp_val, $val8),
  ($comp_val, $val9),
  ($comp_val, $val10),
  ($comp_val, $val11),
  ($comp_val, $val12),
  ($comp_val, $val13),
  ($comp_val, $comp_val),
  ($comp_val, $comp_val),
  ($comp_val, $comp_val),
  ($comp_val, $comp_val);

### Read table
eval SELECT * FROM t1 WHERE c1 = $comp_val AND c2 > $comp_val ORDER BY c2 ASC;
eval SELECT * FROM t1 WHERE c1 = $comp_val AND c2 >= $comp_val ORDER BY c2 ASC;
eval SELECT * FROM t1 WHERE c1 = $comp_val AND c2 < $comp_val ORDER BY c2 ASC;
eval SELECT * FROM t1 WHERE c1 = $comp_val AND c2 <= $comp_val ORDER BY c2 ASC;

eval SELECT * FROM t1 WHERE c1 = $comp_val AND c2 > $comp_val ORDER BY c2 DESC;
eval SELECT * FROM t1 WHERE c1 = $comp_val AND c2 >= $comp_val ORDER BY c2 DESC;
eval SELECT * FROM t1 WHERE c1 = $comp_val AND c2 < $comp_val ORDER BY c2 DESC;
eval SELECT * FROM t1 WHERE c1 = $comp_val AND c2 <= $comp_val ORDER BY c2 DESC;

### Cleanup
DROP TABLE t1;
