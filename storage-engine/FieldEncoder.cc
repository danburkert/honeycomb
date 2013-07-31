#include "FieldEncoder.h"
#include "NumericFieldEncoder.h"
#include "DateTimeFieldEncoder.h"
#include "StringFieldEncoder.h"
#include "DoubleFieldEncoder.h"
#include "DecimalFieldEncoder.h"
#include "sql_class.h"
#include "my_global.h"

FieldEncoder::FieldEncoder(Field& field) :
		field(field)
{
}

FieldEncoder::~FieldEncoder()
{
}

FieldEncoder* FieldEncoder::create_encoder(Field& field)
{
	enum_field_types type = field.real_type();
	switch (type)
	{
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_TIME: // Time is a special case for sorting
		case MYSQL_TYPE_TIME2:
			return new NumericFieldEncoder(field);
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
			return new DoubleFieldEncoder(field);
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL:
			return new DecimalFieldEncoder(field);
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATETIME2:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIMESTAMP2:
		case MYSQL_TYPE_NEWDATE:
			return new DateTimeFieldEncoder(field);
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
			return new StringFieldEncoder(field);
		default:
			return new FieldEncoder(field);
	}
}

void FieldEncoder::store_field_value(uchar* buffer, size_t buffer_length)
{
	field.store((char*) buffer, buffer_length, &my_charset_bin);
}

void FieldEncoder::encode_field_for_reading(uchar* key, uchar** buffer,
		size_t* field_size)
{
	*buffer = new uchar[*field_size];
	memcpy(*buffer, key, *field_size);
}

void FieldEncoder::encode_field_for_writing(uchar** buffer, size_t* field_size)
{
	*field_size = field.key_length();
	*buffer = (uchar*) my_malloc(*field_size, MYF(MY_WME));
	memcpy(*buffer, field.ptr, *field_size);
}
