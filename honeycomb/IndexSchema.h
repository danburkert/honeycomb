#ifndef INDEX_SCHEMA_H
#define INDEX_SCHEMA_H

#include <avro.h>
#include <stdlib.h>

class IndexSchema
{
  private:
    avro_schema_t index_schema_schema;
    avro_value_t index_schema;

  public:
    IndexSchema();
    ~IndexSchema();

    /**
     * @brief Resets the IndexSchema to a fresh state. Reseting an existing
     * IndexSchema is much faster than creating a new one.
     * @return Error code
     */
    int reset();

    bool get_is_unique();

    int set_is_unique(bool is_unique);

    /**
     * Return the number of columns in the index schema.
     */
    size_t size();

    /**
     * Return the nth column of the index.
     */
    const char* get_column(size_t n);

    /**
     * Add a column to the index.
     */
    int add_column(const char* column_name);
};
#endif
