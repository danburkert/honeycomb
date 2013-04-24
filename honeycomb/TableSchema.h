#ifndef TABLE_SCHEMA_H
#define TABLE_SCHEMA_H

#include <avro.h>
#include "Serializable.h"

class ColumnSchema;
class IndexSchema;

#define TABLE_SCHEMA "{\"type\":\"record\",\"name\":\"AvroTableSchema\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"version\",\"type\":\"int\",\"doc\":\"Schema version number\",\"default\":0},{\"name\":\"columns\",\"type\":{\"type\":\"map\",\"values\":" COLUMN_SCHEMA ",\"avro.java.string\":\"String\"}},{\"name\":\"indices\",\"type\":{\"type\":\"map\",\"values\":" INDEX_SCHEMA ",\"avro.java.string\":\"String\"}}]}"

class TableSchema : public Serializable
{
  private:
    avro_schema_t table_schema_schema;
    avro_value_t table_schema;
    static const int CURRENT_VERSION;
    static const char* VERSION_FIELD;

    int add_to_map_field(const char* field_name, const char* key, avro_value_t* value);
    int get_from_map_field(const char* field_name, const char* key, avro_value_t* value);

    /**
     * @brief Set the schema version of the table schema
     * @param version  The version used during serialization.
     * @return Error code
     */
    int set_schema_version(const int& version);

  public:
    TableSchema();

    ~TableSchema();

    int reset();

    bool equals(const TableSchema& other);

    int serialize(const char** buf, size_t* len);

    int deserialize(const char* buf, int64_t len);

    int add_column(const char* name, ColumnSchema* schema);

    int get_column(const char* name, ColumnSchema* schema);

    int add_index(const char* name, IndexSchema* schema);

    int get_index(const char* name, IndexSchema* schema);

    size_t column_count();

    size_t index_count();
};
#endif
