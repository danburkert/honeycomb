#include "StringFieldEncoder.h"
#include "my_sys.h"
#include <string.h>
#include <stdint.h>
#include "sql_class.h"

bool StringFieldEncoder::is_blob()
{
	switch (field.real_type())
	{
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
			return true;
		default:
			return false;
	}
}

void StringFieldEncoder::set_field(uchar** buffer, size_t* field_size,
		const String& string_value)
{
	*field_size = string_value.length();
	*buffer = (uchar*) my_malloc(*field_size, MYF(MY_WME));
	memcpy(*buffer, string_value.ptr(), *field_size);
}

StringFieldEncoder::StringFieldEncoder(Field& field) :
		FieldEncoder(field)
{
}

void StringFieldEncoder::encode_field_for_reading(uchar* key, uchar** buffer,
		size_t* field_size)
{
	/**
	 * VARCHARs are prefixed with two bytes that represent the actual length of the value.
	 * So we need to read the length into actual_length, then copy those bits to key_copy.
	 * Thank you, MySQL...
	 */
	uint16_t *short_len_ptr = (uint16_t *) key;
	*field_size = (uint) (*short_len_ptr);
	key += 2;
	*buffer = new uchar[*field_size];
	memcpy(*buffer, key, *field_size);
}

void StringFieldEncoder::encode_field_for_writing(uchar** buffer,
		size_t* field_size)
{
	if (is_blob())
	{
		String string_value;
		field.val_str(&string_value);
		set_field(buffer, field_size, string_value);
	}
	else
	{
		char string_value_buff[field.field_length];
		String string_value(string_value_buff, sizeof(string_value_buff),
				&my_charset_bin);
		field.val_str(&string_value);
		set_field(buffer, field_size, string_value);
	}
}

StringFieldEncoder::~StringFieldEncoder()
{
}
