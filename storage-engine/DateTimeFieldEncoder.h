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
	~DateTimeFieldEncoder();
	void encode_field_for_reading(uchar* key, uchar** buffer, size_t* field_size);
	void encode_field_for_writing(uchar** buffer, size_t* field_size);
	void store_field_value(uchar* buffer, size_t buffer_length);

private:
	void getTime(const MYSQL_TIME& mysql_time,
		char* timeString);
	void create_datetime(size_t buffer_length, uchar* buffer,
			MYSQL_TIME& mysql_time);
	bool is_time();
};

#endif
