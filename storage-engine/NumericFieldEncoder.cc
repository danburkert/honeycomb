#include "NumericFieldEncoder.h"
#include "my_sys.h"
#include "Util.h"
#include <stdint.h>
#include "handler.h"
#include "sql_class.h"

/**
 * Convert an integral type of count bytes to a little endian long
 * Convert a buffer of length buff_length into an equivalent long long in long_buff
 */
static void bytes_to_long(const uchar* buff, unsigned int buff_length,
		const bool is_signed, uchar* long_buff)
{
	if (is_signed && buff[buff_length - 1] >= (uchar) 0x80)
	{
		memset(long_buff, 0xFF, sizeof(long));
	}
	else
	{
		memset(long_buff, 0x00, sizeof(long));
	}

	memcpy(long_buff, buff, buff_length);
}

NumericFieldEncoder::NumericFieldEncoder(Field& field) :
		FieldEncoder(field)
{
}
void NumericFieldEncoder::encode_year(uchar* key, uchar* buffer,
		size_t field_size)
{
	uint32_t int_val;
	if (key[0] == 0)
	{
		int_val = 0;
	}
	else
	{
		// It comes to us as one byte, need to cast it to int and add 1900
		int_val = (uint32_t) key[0] + 1900;
	}

	bytes_to_long((uchar *) &int_val, sizeof(uint32_t), false, buffer);
}
void NumericFieldEncoder::encode_field_for_reading(uchar* key, uchar** buffer,
		size_t* field_size)
{
	size_t key_len = *field_size;
	*field_size = sizeof(long long);
	*buffer = new uchar[*field_size];
	if (field.real_type() == MYSQL_TYPE_YEAR)
	{
		encode_year(key, *buffer, *field_size);
	}
	else if (field.real_type() == MYSQL_TYPE_TIME2 || field.real_type() == MYSQL_TYPE_TIME)
	{
		field.set_key_image(key, key_len);
		long long integral_value = field.val_int();
		memcpy(*buffer, &integral_value, *field_size);
	}
	else
	{
		const bool is_signed = !is_unsigned_field(field);
		bytes_to_long(key, field.pack_length(), is_signed, *buffer);
	}

	make_big_endian(*buffer, *field_size);
}

void NumericFieldEncoder::encode_field_for_writing(uchar** buffer,
		size_t* field_size)
{
	*field_size = sizeof(long long);
	*buffer = (uchar*) my_malloc(*field_size, MYF(MY_WME));

	long long integral_value = field.val_int();
	if (is_little_endian())
	{
		integral_value = bswap64(integral_value);
	}

	memcpy(*buffer, &integral_value, *field_size);
}

void NumericFieldEncoder::store_field_value(uchar* buffer, size_t buffer_length)
{
	enum_field_types type = field.real_type();
	if (type == MYSQL_TYPE_LONGLONG)
	{
		memcpy(field.ptr, buffer, sizeof(ulonglong));
		if (is_little_endian())
		{
			reverse_bytes(field.ptr, buffer_length);
		}
	}
	else
	{
		long long long_value = *(long long*) buffer;
		if (is_little_endian())
		{
			long_value = bswap64(long_value);
		}

		field.store(long_value, false);
	}
}

NumericFieldEncoder::~NumericFieldEncoder()
{
}
