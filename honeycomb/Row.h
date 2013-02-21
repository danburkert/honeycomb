#ifndef ROW_H
#define ROW_H

#include <avro.h>
#include <stdio.h>
#include <stdlib.h>

const char ROW_CONTAINER_SCHEMA[] = "{\"type\": \"record\", \"name\": \"RowContainer\", \"namespace\": \"com.nearinfinity.honeycomb.mysql.gen\", \"fields\": [ {\"name\": \"uuid\", \"type\": {\"type\":\"fixed\", \"name\": \"UUIDContainer\", \"size\": 16}}, {\"name\":\"records\",\"type\":{\"type\":\"map\",\"values\":[\"null\",\"bytes\"],\"avro.java.string\":\"String\"}}]}";

class Row
{
  private:
    avro_schema_t row_container_schema;
    avro_value_t row_container;

    /**
     * @brief Gets the record of the column in the Row, and checks the type.
     * Sets the passed in value pointer to the value of the record, or NULL if
     * not found.
     * @param column_name   The column of the requested record
     * @param type  The expected type of the record.
     * @param record Pointer to the contained record, or NULL.
     *
     * @return Error code
     */
    int get_record(const char* column_name, const char* type, avro_value_t** record)
    {
      int ret = 0;
      int type_disc;
      int null_disc;
      int disc;
      avro_value_t records_map;
      avro_value_t record_union;
      avro_schema_t union_schema;

      // Get the records map and find the record belonging to the column
      ret |= avro_value_get_by_name(&row_container, "records", &records_map, NULL);
      ret |= avro_value_get_by_name(&records_map, column_name, &record_union, NULL);
      if (record_union.self == NULL) {*record = NULL; return 0;} // Not found

      union_schema = avro_value_get_schema(&record_union);

      // Get the discriminant (union offset) of the actual record, as well as
      // the expected type and null type discriminants
      ret |= avro_value_get_discriminant(&record_union, &disc);
      avro_schema_union_branch_by_name(union_schema, &type_disc, type);
      avro_schema_union_branch_by_name(union_schema, &null_disc, "null");

      if (!ret)
      {
        if (disc == type_disc)
        {
          ret |= avro_value_get_current_branch(&record_union, *record);
        } else if (disc == null_disc) {
          *record = NULL;
        } else {
          ret = -1;
        }
      }
      return ret;
    }


    /**
     * @brief Adds record to the records map.  The value of record must not be
     * set until after this function returns.  If the row already contains a
     * record in the column the new value will replace the old.
     *
     * @param column_name Column name of the record
     * @param type Type of the record
     * @param record Record to be added to the Row
     *
     * @return Error code
     */
    int set_record(const char* column_name, const char* type, avro_value_t* record)
    {
      int ret = 0;
      int type_disc;
      int is_new;
      avro_value_t records_map;
      avro_value_t record_union;
      avro_schema_t union_schema;

      ret |= avro_value_get_by_name(&row_container, "records", &records_map, NULL);
      ret |= avro_value_add(&records_map, column_name, &record_union, NULL, NULL);

      union_schema = avro_value_get_schema(&record_union);
      avro_schema_union_branch_by_name(union_schema, &type_disc, type);

      // The return value from avro_value_get_current_branch is not an error
      // code as far as I can tell.  See test_avro_value.c.  It returns 22 when
      // there is no current branch, which would be most of the time during a set
      avro_value_get_current_branch(&record_union, record);
      ret |= avro_value_set_branch(&record_union, type_disc, record);

      return ret;
    }

  public:

  Row()
  {
    if (avro_schema_from_json_literal(ROW_CONTAINER_SCHEMA, &row_container_schema))
    {
      printf("Unable to create RowContainer schema.  Exiting.\n");
      abort();
    };
    avro_value_iface_t* rc_class = avro_generic_class_from_schema(row_container_schema);
    if (avro_generic_value_new(rc_class, &row_container))
    {
      printf("Unable to create RowContainer.  Exiting.\n");
      abort();
    }
    avro_value_iface_decref(rc_class);
  }

  ~Row()
  {
    avro_value_decref(&row_container);
    avro_schema_decref(row_container_schema);
  }

  /**
   * @brief Resets the Row to a fresh state with a no UUID and an empty row map.
   * Reseting an existing Row is much faster than creating a new one.
   * @return Error code
   */
  int reset()
  {
    return avro_value_reset(&row_container);
  }

  bool equal(const Row& other)
  {
    avro_value_t other_row_container = other.row_container;
    return avro_value_equal(&row_container, &other_row_container);
  }

  /**
   * @brief Get the UUID of the Row
   * @param buf A pointer to a char buffer.  Upon return the pointer will point
   *            to a byte buffer containing the UUID's bytes.
   * @return Error code
   */
  int get_UUID(const char** buf)
  {
    int ret = 0;
    size_t size = 16;
    avro_value_t uuid;
    ret |= avro_value_get_by_name(&row_container, "uuid", &uuid, NULL);
    ret |= avro_value_get_fixed(&uuid, (const void**)buf, &size);
    return ret;
  }

  /**
   * @brief Set the UUID of the Row
   * @param uuid_buf  byte buffer holding new UUID value. Must be 16 bytes long.
   * @return Error code
   */
  int set_UUID(char* uuid_buf)
  {
    int ret = 0;
    avro_value_t uuid;
    ret |= avro_value_get_by_name(&row_container, "uuid", &uuid, NULL);
    ret |= avro_value_set_fixed(&uuid, uuid_buf, 16);
    return ret;
  }

  /**
   * @brief Get the bytes of a record in the Row.  The value byte buffer will
   * be set to NULL if the record is not in the Row, or if the value of the
   * record is NULL.
   * @param column_name   The column of the requested record
   * @param value   A pointer to the result byte buffer
   * @param size  A pointer to the size of the result byte buffer
   * @return  Error code
   */
  int get_bytes_record(const char* column_name, const char** value, size_t* size)
  {
    int ret;
    avro_value_t record;
    avro_value_t* rec_ptr = &record;

    ret = get_record(column_name, "bytes", &rec_ptr);
    if (!ret && (rec_ptr == NULL))
    {
      *value = NULL;
    } else {
      ret |= avro_value_get_bytes(rec_ptr, (const void**) value, size);
    }
    return ret;
  }

  /**
   * @brief set record to null in Row.
   *
   * @param column_name   Record to set to null
   * @return Error code
   */
  int set_null_record(const char* column_name)
  {
    int ret = 0;
    avro_value_t record;
    ret |= set_record(column_name, "null", &record);
    ret |= avro_value_set_null(&record);
    return ret;
  }

  /**
   * @brief set count to the number of records in the row
   * @param the count
   * @return Error code
   */
  int record_count(size_t* count)
  {
    int ret = 0;
    avro_value_t records_map;
    ret |= avro_value_get_by_name(&row_container, "records", &records_map, NULL);
    ret |= avro_value_get_size(&records_map, count);
    return ret;
  }

  /**
   * @brief Set record in Row to given value and size.
   *
   * @param column_name Column of record
   * @param value Byte buffer value of record
   * @param size Size of value
   *
   * @return Error code
   */
  int set_bytes_record(const char* column_name, char* value, size_t size)
  {
    int ret = 0;
    avro_value_t record;
    ret |= set_record(column_name, "bytes", &record);
    ret |= avro_value_set_bytes(&record, value, size);
    return ret;
  }


  /**
   * @brief Serialize Row to buf and set serialized length in len
   * @param buf Pointer to a byte buffer holding the serialized Row.  The caller
   * is responsible for freeing the buffer after finishing with it.
   * @return Error code
   */
  int serialize(const char** buf, size_t* len)
  {
    int ret = 0;
    ret |= avro_value_sizeof(&row_container, len);
    *buf = (const char*) malloc(sizeof(const char) * (*len));
    if(*buf)
    {
      avro_writer_t writer = avro_writer_memory(*buf, *len);
      ret |= avro_value_write(writer, &row_container);
    } else {
      ret = -1;
    }

    return ret;
  }

  int deserialize(const char* buf, int64_t len)
  {
    avro_reader_t reader = avro_reader_memory(buf, len);
    return avro_value_read(reader, &row_container);
  }
};
#endif
