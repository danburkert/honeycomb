#ifndef ROW_H
#define ROW_H

#include <avro.h>
#include <stdlib.h>

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
    int get_record(const char* column_name, const char* type, avro_value_t** record);

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
    int set_record(const char* column_name, const char* type, avro_value_t* record);

  public:

  Row(); 
  ~Row();

  /**
   * @brief Resets the Row to a fresh state with a no UUID and an empty row map.
   * Reseting an existing Row is much faster than creating a new one.
   * @return Error code
   */
  int reset();

  bool equal(const Row& other);

  /**
   * @brief set count to the number of records in the row
   * @param the count
   * @return Error code
   */
  int record_count(size_t* count);

  /**
   * @brief Get the UUID of the Row
   * @param buf A pointer to a char buffer.  Upon return the pointer will point
   *            to a byte buffer containing the UUID's bytes.
   * @return Error code
   */
  int get_UUID(const char** buf);

  /**
   * @brief Set the UUID of the Row
   * @param uuid_buf  byte buffer holding new UUID value. Must be 16 bytes long.
   * @return Error code
   */
  int set_UUID(char* uuid_buf);

  /**
   * @brief Get the bytes of a record in the Row.  The value byte buffer will
   * be set to NULL if the record is not in the Row, or if the value of the
   * record is NULL.
   * @param column_name   The column of the requested record
   * @param value   A pointer to the result byte buffer
   * @param size  A pointer to the size of the result byte buffer
   * @return  Error code
   */
  int get_bytes_record(const char* column_name, const char** value, size_t* size);

  /**
   * @brief Set record in Row to given value and size.
   *
   * @param column_name Column of record
   * @param value Byte buffer value of record
   * @param size Size of value
   *
   * @return Error code
   */
  int set_bytes_record(const char* column_name, char* value, size_t size);

  /**
   * @brief Serialize Row to buf and set serialized length in len
   * @param buf Pointer to a byte buffer holding the serialized Row.  The caller
   * is responsible for freeing the buffer after finishing with it.
   * @return Error code
   */
  int serialize(const char** buf, size_t* len);

  int deserialize(const char* buf, int64_t len);
};
#endif
