#ifndef INDEXCONTAINER_H
#define INDEXCONTAINER_H

#include <avro.h>
#include <stdlib.h>
#include "Serializable.h"

class IndexContainer : public Serializable
{
  private:
    avro_schema_t container_schema_schema;
    avro_value_t container_schema;
    int get_record(const char* column_name, const char* type, avro_value_t** entry_value);
    int set_record(const char* column_name, const char* type, avro_value_t* record);
  public:
    enum QueryType
    {
      EXACT_KEY,
      AFTER_KEY,
      KEY_OR_NEXT,
      KEY_OR_PREVIOUS,
      BEFORE_KEY,
      INDEX_FIRST,
      INDEX_LAST
    };
    IndexContainer();
    ~IndexContainer();
    int reset();
    bool equals(const IndexContainer& other);

    int serialize(const char** buf, size_t* len);

    int deserialize(const char* buf, int64_t len);

    int set_bytes_record(const char* column_name, char* value, size_t size);

    int get_bytes_record(const char* column_name, const char** value, size_t* size);

    int set_type(QueryType type);

    QueryType get_type();

    int record_count(size_t* count);

    int set_name(const char* index_name);

    const char* get_name();
};

#endif 
