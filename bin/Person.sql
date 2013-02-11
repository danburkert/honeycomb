USE hbase;

CREATE TABLE hc_05 (
  first_name VARCHAR(32),
  last_name VARCHAR(32),
  address VARCHAR(32),
  zip VARCHAR(16),
  state CHAR(2),
  country VARCHAR(64),
  phone VARCHAR(32),
  salary INT,
  fk TINYINT,
  INDEX(first_name, last_name),
  INDEX(address, zip, state, country),
  INDEX(salary),
  INDEX(fk)) ENGINE = Honeycomb;
