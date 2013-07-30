#ifndef DECIMALFIELDENCODER_H_
#define DECIMALFIELDENCODER_H_
#include "FieldEncoder.h"

class DecimalFieldEncoder : public FieldEncoder
{
public:
	DecimalFieldEncoder(Field& field);
	void encode_field_for_writing(uchar** buffer, size_t* field_size);
	void store_field_value(uchar* buffer, size_t buffer_length);
};


#endif /* DECIMALFIELDENCODER_H_ */
