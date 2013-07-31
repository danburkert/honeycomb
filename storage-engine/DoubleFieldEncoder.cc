/*
 * DoubleFieldEncoder.cc
 *
 *  Created on: Jul 30, 2013
 *      Author: showell
 */

#include "DoubleFieldEncoder.h"
#include <string.h>
#include "Util.h"
#include "sql_class.h"

float DoubleFieldEncoder::floatGet(const uchar *ptr)
{
	float j;
#ifdef WORDS_BIGENDIAN
	float4get(j,ptr);
#endif
	memcpy(&j, ptr, sizeof(j));

	return j;
}
DoubleFieldEncoder::DoubleFieldEncoder(Field& field) :
		FieldEncoder(field)
{
}
void DoubleFieldEncoder::encode_field_for_reading(uchar* key, uchar** buffer,
		size_t* field_size)
{
	double j = (double) floatGet(key);
	if (field.real_type() == MYSQL_TYPE_DOUBLE)
	{
		doubleget(j, key);
	}

	*buffer = new uchar[sizeof(double)];
	*field_size = sizeof(double);

	doublestore(*buffer, j);
	make_big_endian(*buffer, *field_size);
}

void DoubleFieldEncoder::encode_field_for_writing(uchar** buffer,
		size_t* field_size)
{
	double fp_value = field.val_real();
	long long* fp_ptr = (long long*) &fp_value;
	if (is_little_endian())
	{
		*fp_ptr = bswap64(*fp_ptr);
	}
	*field_size = sizeof fp_value;
	*buffer = (uchar*) my_malloc(*field_size, MYF(MY_WME));
	memcpy(*buffer, fp_ptr, *field_size);
}

void DoubleFieldEncoder::store_field_value(uchar* buffer, size_t buffer_length)
{
	double double_value;
	if (is_little_endian())
	{
		long long* long_ptr = (long long*) buffer;
		longlong swapped_long = bswap64(*long_ptr);
		double_value = *(double*) &swapped_long;
	}
	else
	{
		double_value = *(double*) buffer;
	}

	field.store(double_value);
}
