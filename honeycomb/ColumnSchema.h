#ifndef COLUMN_SCHEMA_H
#define COLUMN_SCHEMA_H

#include <avro.h>

class ColumnSchema
{
  private:
    avro_schema_t column_schema_schema;
    avro_value_t column_schema;

    bool get_bool_field(const char name[]);

    int set_bool_field(const char name[], bool bool_val);

    /**
     * Gets the value of the integer fields associated with the ColumnSchema.
     * All of these fields are unions of null or the integer.  This function
     * returns a negative integer to indicate a null value (the range of the
     * valid integers is non-negative).  This function is only intended to be
     * used for testing, if we end up needing to check the value on the c++
     * side we should probably come up with something more robust, such as
     * bool is_auto_increment_null().
     */
    int get_int_field(const char name[]);

    int set_int_field(const char name[], int val);

    int set_null_field(const char name[]);

    int set_defaults();

  public:

  enum ColumnType
  {
    STRING,
    BINARY,
    ULONG,
    LONG,
    DOUBLE,
    DECIMAL,
    TIME,
    DATE,
    DATETIME
  };

  ColumnSchema();
  ~ColumnSchema();

  /**
   * @brief Resets the ColumnSchema to a fresh state.  Reseting an existing Row
   * is much faster than creating a new one.
   * @return Error code
   */
  int reset();

  bool equal(const ColumnSchema& other);

  int serialize(const char** buf, size_t* len);

  int deserialize(const char* buf, int64_t len);

  ColumnType get_type();

  int set_type(ColumnType type);

  bool get_is_nullable();

  int set_is_nullable(bool is_nullable);

  bool get_is_auto_increment();

  int set_is_auto_increment(bool is_nullable);

  int get_max_length();

  int set_max_length(int length);

  int get_scale();

  int set_scale(int scale);

  int get_precision();

  int set_precision(int precision);

};
#endif
