#ifndef TABLE_SCHEMA_H
#define TABLE_SCHEMA_H

#include <avro.h>

const char TABLE_SCHEMA[] = "{\"type\":\"record\",\"name\":\"TableSchema\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"columns\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"ColumnSchema\",\"fields\":[{\"name\":\"type\",\"type\":{\"type\":\"enum\",\"name\":\"ColumnType\",\"symbols\":[\"STRING\",\"BINARY\",\"ULONG\",\"LONG\",\"DOUBLE\",\"DECIMAL\",\"TIME\",\"DATE\",\"DATETIME\"]}},{\"name\":\"isNullable\",\"type\":\"boolean\",\"default\":true},{\"name\":\"isAutoIncrement\",\"type\":\"boolean\",\"default\":false},{\"name\":\"maxLength\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"scale\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"precision\",\"type\":[\"null\",\"int\"],\"default\":null}]},\"avro.java.string\":\"String\"}},{\"name\":\"indices\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"IndexSchema\",\"fields\":[{\"name\":\"columns\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"isUnique\",\"type\":\"boolean\",\"default\":false}]},\"avro.java.string\":\"String\"}}]}";

class TableSchema
{
  private:
    avro_schema_t table_schema_schema;
    avro_value_t table_schema;

  public:
    int add_column(const char* name, ColumnSchema schema);

    int add_index(const char* name, IndexSchema schema);

}


#endif
