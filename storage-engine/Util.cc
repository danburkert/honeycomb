/*
 * Copyright (C) 2013 Near Infinity Corporation
 *
 * This file is part of Honeycomb Storage Engine.
 *
 * Honeycomb Storage Engine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Honeycomb Storage Engine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Honeycomb Storage Engine.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "Util.h"
#include <tztime.h>
#include <pwd.h>
#include <grp.h>

void get_user_group(uid_t user_id, gid_t group_id, char* buffer,
		size_t buf_size)
{
	struct passwd passwd;
	struct passwd* tmp_user;
	struct group group;
	struct group* tmp_group;
	char u_temp_buf[256];
	char g_temp_buf[256];

	getgrgid_r(group_id, &group, g_temp_buf, sizeof(g_temp_buf), &tmp_group);
	getpwuid_r(user_id, &passwd, u_temp_buf, sizeof(u_temp_buf), &tmp_user);
	snprintf(buffer, buf_size, "%s:%s", passwd.pw_name, group.gr_name);
}

void get_file_user_group(const char* file, char* buffer, size_t buf_size)
{
	struct stat fstat;
	stat(file, &fstat);
	get_user_group(fstat.st_uid, fstat.st_gid, buffer, buf_size);
}

uint64_t bswap64(uint64_t x)
{
	return __builtin_bswap64(x);
}

bool is_unsigned_field(Field& field)
{
	ha_base_keytype keyType = field.key_type();
	return (keyType == HA_KEYTYPE_BINARY || keyType == HA_KEYTYPE_USHORT_INT
			|| keyType == HA_KEYTYPE_UINT24 || keyType == HA_KEYTYPE_ULONG_INT
			|| keyType == HA_KEYTYPE_ULONGLONG);
}

void reverse_bytes(uchar *begin, uint length)
{
	for (int x = 0, y = length - 1; x < y; x++, y--)
	{
		uchar tmp = begin[x];
		begin[x] = begin[y];
		begin[y] = tmp;
	}
}

bool is_little_endian()
{
#ifdef WORDS_BIG_ENDIAN
	return false;
#else
	return true;
#endif
}

float floatGet(const uchar *ptr)
{
	float j;
#ifdef WORDS_BIGENDIAN
	if (table->s->db_low_byte_first)
	{
		float4get(j,ptr);
	}
	else
#endif
	memcpy(&j, ptr, sizeof(j));

	return j;
}

void make_big_endian(uchar *begin, uint length)
{
	if (is_little_endian())
	{
		reverse_bytes(begin, length);
	}
}

const char* extract_table_name_from_path(const char *path)
{
	return path + 2;
}

