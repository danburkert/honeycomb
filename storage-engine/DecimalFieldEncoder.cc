/*
 * DecimalFieldEncoder.cc
 *
 *  Created on: Jul 30, 2013
 *      Author: showell
 */

#include "DecimalFieldEncoder.h"
#include "my_sys.h"
#include "sql_class.h"

DecimalFieldEncoder::DecimalFieldEncoder(Field& field) :
		FieldEncoder(field)
{
}

void DecimalFieldEncoder::encode_field_for_writing(uchar** buffer,
		size_t* field_size)
{
	*field_size = field.key_length();
	*buffer = (uchar*) my_malloc(*field_size, MYF(MY_WME));
	memcpy(*buffer, field.ptr, *field_size);
}

void DecimalFieldEncoder::store_field_value(uchar* buffer, size_t buffer_length)
{
	// TODO: Is this reliable? Field_decimal doesn't seem to have these members.
	// Potential crash for old decimal types. - ABC
	Field_new_decimal& decimal_field = static_cast<Field_new_decimal&>(field);
	uint precision = decimal_field.precision;
	uint scale = decimal_field.dec;
	my_decimal decimal_val;
	binary2my_decimal(0, (const uchar *) buffer, &decimal_val, precision,
			scale);
	const my_decimal* decimalVal = const_cast<my_decimal*>(&decimal_val);
	decimal_field.store_value(decimalVal);
}
