#include "ColumnSchema.h"
#include "AvroUtil.h"

const char TYPE[] = "type";
const char IS_NULLABLE[] = "isNullable";
const char IS_AUTO_INCREMENT[] = "isAutoIncrement";
const char MAX_LENGTH[] = "maxLength";
const char SCALE[] = "scale";
const char PRECISION[] = "precision";

bool ColumnSchema::get_bool_field(const char name[]) {
  int bool_val;
  avro_value_t avro_bool;
  avro_value_get_by_name(&column_schema, name, &avro_bool, NULL);
  avro_value_get_boolean(&avro_bool, &bool_val);
  return static_cast<bool> (bool_val);
}

int ColumnSchema::set_bool_field(const char name[], bool bool_val) {
  avro_value_t avro_bool;
  int ret = avro_value_get_by_name(&column_schema, name, &avro_bool, NULL);
  ret |= avro_value_set_boolean(&avro_bool, bool_val);
  return ret;
}

int ColumnSchema::get_int_field(const char name[]) {
  int val;
  int null_disc;
  int disc;

  avro_schema_t union_schema;
  avro_value_t avro_union;
  avro_value_t avro_val;


  int ret = avro_value_get_by_name(&column_schema, name, &avro_union, NULL);

  union_schema = avro_value_get_schema(&avro_union);

  ret |= avro_value_get_discriminant(&avro_union, &disc);
  avro_schema_union_branch_by_name(union_schema, &null_disc, "null");

  if (disc == null_disc) { return -1; }
  ret |= avro_value_get_current_branch(&avro_union, &avro_val);
  ret |= avro_value_get_int(&avro_val, &val);
  return val;
}

int ColumnSchema::set_int_field(const char name[], int val) {
  int disc;
  avro_schema_t union_schema;
  avro_value_t avro_union;
  avro_value_t branch;

  int ret = avro_value_get_by_name(&column_schema, name, &avro_union, NULL);

  union_schema = avro_value_get_schema(&avro_union);

  avro_schema_union_branch_by_name(union_schema, &disc, "int");
  ret |= avro_value_set_branch(&avro_union, disc, &branch);
  ret |= avro_value_set_int(&branch, val);

  return ret;
}

int ColumnSchema::set_null_field(const char name[]) 
{
  int disc;
  avro_schema_t union_schema;
  avro_value_t avro_union;
  avro_value_t branch;

  int ret = avro_value_get_by_name(&column_schema, name, &avro_union, NULL);

  union_schema = avro_value_get_schema(&avro_union);

  avro_schema_union_branch_by_name(union_schema, &disc, "null");
  ret |= avro_value_set_branch(&avro_union, disc, &branch);
  ret |= avro_value_set_null(&branch);
  return ret;
}

int ColumnSchema::set_defaults() {
  return set_is_nullable(true) | set_null_field(MAX_LENGTH) | set_null_field(SCALE) | set_null_field(PRECISION);
}

ColumnSchema::ColumnSchema()
{
  if (avro_schema_from_json_literal(COLUMN_SCHEMA, &column_schema_schema))
  {
    printf("Unable to create ColumnSchema schema.  Exiting.\n");
    abort();
  }
  avro_value_iface_t* rc_class = avro_generic_class_from_schema(column_schema_schema);
  if (avro_generic_value_new(rc_class, &column_schema))
  {
    printf("Unable to create ColumnSchema.  Exiting.\n");
    abort();
  }
  set_defaults();
  avro_value_iface_decref(rc_class);
}

ColumnSchema::~ColumnSchema()
{
  avro_value_decref(&column_schema);
  avro_schema_decref(column_schema_schema);
}

int ColumnSchema::reset()
{
  return avro_value_reset(&column_schema) || set_defaults();
}

bool ColumnSchema::equals( const ColumnSchema& other)
{
  avro_value_t other_column_schema = other.column_schema;
  return avro_value_equal(&column_schema, &other_column_schema);
}

int ColumnSchema::serialize(const char** buf, size_t* len)
{
  return serialize_object(&column_schema, buf, len);
}

int ColumnSchema::deserialize(const char* buf, int64_t len)
{
  return deserialize_object(&column_schema, buf, len);
}

ColumnSchema::ColumnType ColumnSchema::get_type()
{
  int val;
  avro_value_t avro_enum;
  avro_value_get_by_name(&column_schema, TYPE, &avro_enum, NULL);
  avro_value_get_enum(&avro_enum, &val);
  return (ColumnSchema::ColumnType) val;
}

int ColumnSchema::set_type(ColumnType type)
{
  avro_value_t avro_enum;
  return avro_value_get_by_name(&column_schema, TYPE, &avro_enum, NULL) |
         avro_value_set_enum(&avro_enum, type);
}

bool ColumnSchema::get_is_nullable() {
  return get_bool_field(IS_NULLABLE);
}

int ColumnSchema::set_is_nullable(bool is_nullable)
{
  return set_bool_field(IS_NULLABLE, is_nullable);
}

bool ColumnSchema::get_is_auto_increment()
{
  return get_bool_field(IS_AUTO_INCREMENT);
}

int ColumnSchema::set_is_auto_increment(bool is_auto_increment)
{
  return set_bool_field(IS_AUTO_INCREMENT, is_auto_increment);
}

int ColumnSchema::get_max_length()
{
  return get_int_field(MAX_LENGTH);
}

int ColumnSchema::set_max_length(int length)
{
  return set_int_field(MAX_LENGTH, length);
}

int ColumnSchema::get_scale()
{
  return get_int_field(SCALE);
}

int ColumnSchema::set_scale(int scale)
{
  return set_int_field(SCALE, scale);
}

int ColumnSchema::get_precision()
{
  return get_int_field(PRECISION);
}

int ColumnSchema::set_precision(int precision)
{
  return set_int_field(PRECISION, precision);
}

avro_value_t* ColumnSchema::get_avro_value()
{
  return &column_schema;
}

int ColumnSchema::set_avro_value(avro_value_t* value)
{
  return avro_value_copy(&column_schema, value);
}
