#ifndef FIELDENCODER_H_
#define FIELDENCODER_H_

#include <time.h>

typedef unsigned char uchar;
class Field;
class FieldEncoder
{
protected:
	Field& field;
public:
	FieldEncoder(Field& field);
	virtual ~FieldEncoder();

    /**
     * @brief Encode a key for use with a MySQL query
     *
     * @param key MySQL query key
     * @param buffer Encoded query key
     * @param field_size Length of the query key
     */
	virtual void encode_field_for_reading(uchar* key, uchar** buffer,
			size_t* field_size);

    /**
     * @brief Encode a key for saving on the Java side
     *
     * @param buffer Encoded value 
     * @param field_size Length of the value
     */
	virtual void encode_field_for_writing(uchar** buffer, size_t* field_size);

    /**
     * @brief Store a value back into a field based on the field type.
     *
     * @param buffer Buffer containing value
     * @param buffer_length Length of the value
     */
	virtual void store_field_value(uchar* buffer, size_t buffer_length);

    /**
     * @brief Create a encoder for a field on a table.
     *
     * @param field Table field
     *
     * @return Field encoder
     */
	static FieldEncoder* create_encoder(Field& field);
};

#endif
