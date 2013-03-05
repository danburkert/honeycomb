#ifndef TABLE_SCHEMA_H
#define TABLE_SCHEMA_H

#include <avro.h>

const char TABLE_SCHEMA[] = "{\"type\":\"record\",\"name\":\"TableSchema\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"columns\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"ColumnSchema\",\"fields\":[{\"name\":\"type\",\"type\":{\"type\":\"enum\",\"name\":\"ColumnType\",\"symbols\":[\"STRING\",\"BINARY\",\"ULONG\",\"LONG\",\"DOUBLE\",\"TIME\",\"DATE\",\"DATETIME\",\"DECIMAL\"]}},{\"name\":\"isNullable\",\"type\":\"boolean\",\"default\":true},{\"name\":\"isAutoincrement\",\"type\":\"boolean\",\"default\":false},{\"name\":\"maxLength\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"scale\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"precision\",\"type\":[\"null\",\"int\"],\"default\":null}]},\"avro.java.string\":\"String\"}},{\"name\":\"indices\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"IndexSchema\",\"fields\":[{\"name\":\"columns\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"isUnique\",\"type\":\"boolean\",\"default\":false}]},\"avro.java.string\":\"String\"}}]}";

class TableSchema
{
  avro_schema_t table_schema_schema; // uh huh
  avro_value_t table_schema;

}


#endif
