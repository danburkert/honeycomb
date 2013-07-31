#ifndef DATETIMEFIELDENCODER_H_
#define DATETIMEFIELDENCODER_H_

#include "FieldEncoder.h"

struct st_mysql_time;
typedef st_mysql_time MYSQL_TIME;
class THD;

class DateTimeFieldEncoder: public FieldEncoder
{
public:
	DateTimeFieldEncoder(Field& field);
	void encode_field_for_reading(uchar* key, uchar** buffer, size_t* field_size);
	void encode_field_for_writing(uchar** buffer, size_t* field_size);
	void store_field_value(uchar* buffer, size_t buffer_length);
};

#endif
