#include "DateTimeFieldEncoder.h"
#include <string.h>
#include "Util.h"
#include "my_time.h"
#include "sql_class.h"
#include <tztime.h>

DateTimeFieldEncoder::DateTimeFieldEncoder(Field& field) :
		FieldEncoder(field)
{
}

void DateTimeFieldEncoder::getTime(const MYSQL_TIME& mysql_time,
		char* timeString)
{
	my_TIME_to_str(&mysql_time, timeString);
}

void DateTimeFieldEncoder::encode_field_for_reading(uchar* key, uchar** buffer,
		size_t* field_size)
{
	MYSQL_TIME mysql_time;
	char timeString[MAX_DATE_STRING_REP_LENGTH];

	field.set_key_image(key, *field_size);
	field.get_time(&mysql_time);

	getTime(mysql_time, timeString);
	*field_size = strlen(timeString);
	*buffer = new uchar[*field_size];
	memcpy(*buffer, timeString, *field_size);
}

void DateTimeFieldEncoder::encode_field_for_writing(uchar** buffer,
		size_t* field_size)
{
	MYSQL_TIME mysql_time;
	char temporal_value[MAX_DATE_STRING_REP_LENGTH];
	field.get_time(&mysql_time);
	if (mysql_time.time_type == MYSQL_TIMESTAMP_DATE
			&& field.real_type() == MYSQL_TYPE_TIMESTAMP)
		mysql_time.time_type = MYSQL_TIMESTAMP_DATETIME;

	getTime(mysql_time, temporal_value);
	*field_size = strlen(temporal_value);
	*buffer = (uchar*) my_malloc(*field_size, MYF(MY_WME));
	memcpy(*buffer, temporal_value, *field_size);
}

void DateTimeFieldEncoder::create_datetime(size_t buffer_length, uchar* buffer,
		MYSQL_TIME& mysql_time)
{
	int was_cut;
	str_to_datetime((char*) buffer, buffer_length, &mysql_time,
	TIME_FUZZY_DATE, &was_cut);
}

bool DateTimeFieldEncoder::is_time()
{
	return field.real_type() == MYSQL_TYPE_TIME;
}

void DateTimeFieldEncoder::store_field_value(uchar* buffer,
		size_t buffer_length)
{
	if (is_time())
	{
		long long long_value = *(long long*) buffer;
		if (is_little_endian())
		{
			long_value = bswap64(long_value);
		}
		field.store(long_value, false);
	}
	else
	{
		MYSQL_TIME mysql_time;
		create_datetime(buffer_length, buffer, mysql_time);
		field.store_time(&mysql_time, mysql_time.time_type);
	}
}

DateTimeFieldEncoder::~DateTimeFieldEncoder()
{
}
