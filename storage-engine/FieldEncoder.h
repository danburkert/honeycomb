#ifndef FIELDENCODER_H_
#define FIELDENCODER_H_

#include <time.h>

typedef unsigned char uchar;
class Field;
class THD;
class FieldEncoder
{
protected:
	Field& field;
public:
	FieldEncoder(Field& field);
	virtual ~FieldEncoder();

	virtual void encode_field_for_reading(uchar* key, uchar** buffer,
			size_t* field_size);
	virtual void encode_field_for_writing(uchar** buffer, size_t* field_size);
	virtual void store_field_value(uchar* buffer, size_t buffer_length);

	static FieldEncoder* create_encoder(Field& field, THD* thd);
};

#endif
