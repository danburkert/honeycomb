#ifndef HBASE_FIELD
#define HBASE_FIELD

//#include "mysqld.h"
#include "sql_class.h"

class HBaseField
{
public:
  uchar* byte_val;
  long val_len;
  bool is_null;

  HBaseField(Field* field)
  :val_len(0),
   is_null(false)
  {
    if (field->is_null())
    {
      byte_val = NULL;
      val_len = NULL;
      is_null = true;
    } else {

      unsigned int field_length = field->field_length;

      switch (field->real_type())
      {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_ENUM:
        {
          long long integral_value = field->val_int();
          if(is_little_endian())
          {
            integral_value = __builtin_bswap64(integral_value);
          }
          val_len = sizeof integral_value;
          byte_val = (uchar*) my_malloc(val_len, MYF(MY_WME));
          memcpy(byte_val, &integral_value, val_len);
          break;
        }
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_DOUBLE:
        {
          double fp_value = field->val_real();
          long long* fp_ptr = (long long*) &fp_value;
          if(is_little_endian())
          {
            *fp_ptr = __builtin_bswap64(*fp_ptr);
          }
          val_len = sizeof fp_value;
          byte_val = (uchar*) my_malloc(val_len, MYF(MY_WME));
          memcpy(byte_val, fp_ptr, val_len);
          break;
        }
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
          val_len = field->key_length();
          byte_val = (uchar*) my_malloc(val_len, MYF(MY_WME));
          memcpy(byte_val, field->ptr, val_len);
          break;
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
        {
          MYSQL_TIME mysql_time;
          char temporal_value[MAX_DATE_STRING_REP_LENGTH];
          field->get_time(&mysql_time);
          my_TIME_to_str(&mysql_time, temporal_value);
          val_len = strlen(temporal_value);
          byte_val = (uchar*) my_malloc(val_len, MYF(MY_WME));
          memcpy(byte_val, temporal_value, val_len);
          break;
        }
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        {
          char string_value_buff[field->field_length];
          String string_value(string_value_buff, sizeof(string_value_buff), field->charset());
          field->val_str(&string_value);
          val_len = string_value.length();
          byte_val = (uchar*) my_malloc(val_len, MYF(MY_WME));
          memcpy(byte_val, string_value.ptr(), val_len);
          break;
        }
        case MYSQL_TYPE_NULL:
        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_GEOMETRY:
        default:
          val_len = field->key_length();
          byte_val = (uchar*) my_malloc(val_len, MYF(MY_WME));
          memcpy(byte_val, field->ptr, val_len);
          break;
      }
    }
  }
};
#endif
