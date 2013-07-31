#ifndef STRINGFIELDENCODER_H_
#define STRINGFIELDENCODER_H_

#include "FieldEncoder.h"

class String;
class StringFieldEncoder: public FieldEncoder
{
private:
	bool is_blob();

	void set_field(uchar** buffer, size_t* field_size,
			const String& string_value);

public:
	StringFieldEncoder(Field& field);
	~StringFieldEncoder();
	void encode_field_for_reading(uchar* key, uchar** buffer,
			size_t* field_size);

	void encode_field_for_writing(uchar** buffer, size_t* field_size);
};
#endif /* STRINGFIELDENCODER_H_ */
