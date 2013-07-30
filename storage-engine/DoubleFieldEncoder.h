#ifndef DOUBLEFIELDENCODER_H_
#define DOUBLEFIELDENCODER_H_
#include "FieldEncoder.h"

class DoubleFieldEncoder: public FieldEncoder
{
private:
	float floatGet(const uchar *ptr);
public:
	DoubleFieldEncoder(Field& field);
	void encode_field_for_reading(uchar* key, uchar** buffer,
			size_t* field_size);

	void encode_field_for_writing(uchar** buffer, size_t* field_size);
	void store_field_value(uchar* buffer, size_t buffer_length);
};
#endif
