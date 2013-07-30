#include "DateTimeFieldEncoder.h"
#include <string.h>
#include "Util.h"
#include "my_time.h"
#include "sql_class.h"
#include <tztime.h>

static void zero_time(MYSQL_TIME* time)
{
	memset((void*) time, 0, sizeof(*time));
}

static void extract_mysql_newdate(long tmp, MYSQL_TIME *time)
{
	zero_time(time);
	time->month = tmp >> 5 & 15;
	time->day = tmp & 31;
	time->year = tmp >> 9;
	time->time_type = MYSQL_TIMESTAMP_DATE;
}

static void extract_mysql_old_date(int32 tmp, MYSQL_TIME *time)
{
	zero_time(time);
	time->year = (int) ((uint32) tmp / 10000L % 10000);
	time->month = (int) ((uint32) tmp / 100 % 100);
	time->day = (int) ((uint32) tmp % 100);
	time->time_type = MYSQL_TIMESTAMP_DATE;
}

static void extract_mysql_datetime(longlong tmp,
		MYSQL_TIME *time)
{
	zero_time(time);
	uint32 part1, part2;
	part1 = (uint32) (tmp / LL(1000000));
	part2 = (uint32) (tmp - (ulonglong) part1 * LL(1000000));

	time->neg = 0;
	time->second_part = 0;
	time->second = (int) (part2 % 100);
	time->minute = (int) (part2 / 100 % 100);
	time->hour = (int) (part2 / 10000);
	time->day = (int) (part1 % 100);
	time->month = (int) (part1 / 100 % 100);
	time->year = (int) (part1 / 10000);
	time->time_type = MYSQL_TIMESTAMP_DATETIME;
}

static void extract_mysql_timestamp(long tmp, MYSQL_TIME *time,
		THD* thd)
{
	zero_time(time);
	thd->variables.time_zone->gmt_sec_to_TIME(time, (my_time_t) tmp);
}

DateTimeFieldEncoder::DateTimeFieldEncoder(Field& field, THD* thd) :
		FieldEncoder(field), thd(thd)
{
}

void DateTimeFieldEncoder::encode_field_for_reading(uchar* key, uchar** buffer,
		size_t* field_size)
{
	MYSQL_TIME mysql_time;

	switch (field.real_type())
	{
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
			if (*field_size == 3)
			{
				extract_mysql_newdate((long) uint3korr(key), &mysql_time);
			}
			else
			{
				extract_mysql_old_date((int32) uint4korr(key), &mysql_time);
			}
			break;
		case MYSQL_TYPE_TIMESTAMP:
			extract_mysql_timestamp((long) uint4korr(key), &mysql_time, thd);
			break;
		case MYSQL_TYPE_DATETIME:
			extract_mysql_datetime((ulonglong) uint8korr(key), &mysql_time);
			break;
	}

	char timeString[MAX_DATE_STRING_REP_LENGTH];
	my_TIME_to_str(&mysql_time, timeString, MAX_DATE_STRING_REP_LENGTH);
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

	my_TIME_to_str(&mysql_time, temporal_value, MAX_DATE_STRING_REP_LENGTH);
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
