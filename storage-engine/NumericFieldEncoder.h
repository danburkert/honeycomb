#ifndef NUMERICFIELDENCODER_H_
#define NUMERICFIELDENCODER_H_
#include "FieldEncoder.h"

class NumericFieldEncoder : public FieldEncoder
{
private:
	void encode_year(uchar* key, uchar* buffer, size_t field_size);
	bool is_time_field();

public:
	NumericFieldEncoder(Field& field);
	~NumericFieldEncoder();
	void encode_field_for_reading(uchar* key, uchar** buffer, size_t* field_size);
	void encode_field_for_writing(uchar** buffer, size_t* field_size);
	void store_field_value(uchar* buffer, size_t buffer_length);
};
#endif
