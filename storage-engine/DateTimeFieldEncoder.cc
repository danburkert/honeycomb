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

void DateTimeFieldEncoder::encode_field_for_reading(uchar* key, uchar** buffer,
		size_t* field_size)
{
	MYSQL_TIME mysql_time;
	char timeString[MAX_DATE_STRING_REP_LENGTH];

	field.set_key_image(key, *field_size);
	field.get_time(&mysql_time);

	my_TIME_to_str(&mysql_time, timeString, DATETIME_MAX_DECIMALS);
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

	my_TIME_to_str(&mysql_time, temporal_value, DATETIME_MAX_DECIMALS);
	*field_size = strlen(temporal_value);
	*buffer = (uchar*) my_malloc(*field_size, MYF(MY_WME));
	memcpy(*buffer, temporal_value, *field_size);
}

void DateTimeFieldEncoder::store_field_value(uchar* buffer,
		size_t buffer_length)
{
	enum_field_types type = field.real_type();
	if (type == MYSQL_TYPE_TIME || type == MYSQL_TYPE_TIME2)
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
		MYSQL_TIME_STATUS was_cut;
		str_to_datetime((char*) buffer, buffer_length, &mysql_time,
		TIME_FUZZY_DATE, &was_cut);
		field.store_time(&mysql_time, mysql_time.time_type);
	}
}
