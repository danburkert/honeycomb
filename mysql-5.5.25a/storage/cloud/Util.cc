#include "Util.h"

hbase_data_type extract_field_type(Field *field)
{
  int fieldType = field->type();
  hbase_data_type essentialType;

  if (fieldType == MYSQL_TYPE_LONG
          || fieldType == MYSQL_TYPE_SHORT
          || fieldType == MYSQL_TYPE_TINY
          || fieldType == MYSQL_TYPE_LONGLONG
          || fieldType == MYSQL_TYPE_INT24
          || fieldType == MYSQL_TYPE_ENUM
          || fieldType == MYSQL_TYPE_YEAR)
  {
    essentialType = JAVA_LONG;
  }
  else if (fieldType == MYSQL_TYPE_DOUBLE
             || fieldType == MYSQL_TYPE_FLOAT
             || fieldType == MYSQL_TYPE_DECIMAL
             || fieldType == MYSQL_TYPE_NEWDECIMAL)
	{
		essentialType = JAVA_DOUBLE;
	}
	else if (fieldType == MYSQL_TYPE_DATE
      || fieldType == MYSQL_TYPE_NEWDATE)
	{
	  essentialType = JAVA_DATE;
	}
	else if (fieldType == MYSQL_TYPE_TIME)
	{
	  essentialType = JAVA_TIME;
	}
	else if (fieldType == MYSQL_TYPE_DATETIME
      || fieldType == MYSQL_TYPE_TIMESTAMP)
	{
	  essentialType = JAVA_DATETIME;
	}
	else if (fieldType == MYSQL_TYPE_VARCHAR
            || fieldType == MYSQL_TYPE_STRING
            || fieldType == MYSQL_TYPE_VAR_STRING
            || fieldType == MYSQL_TYPE_BLOB
            || fieldType == MYSQL_TYPE_TINY_BLOB
            || fieldType == MYSQL_TYPE_MEDIUM_BLOB
            || fieldType == MYSQL_TYPE_LONG_BLOB)
  {
    essentialType = JAVA_STRING;
  }
  else
  {
    essentialType = UNKNOWN_TYPE;
  }

  return essentialType;
}
