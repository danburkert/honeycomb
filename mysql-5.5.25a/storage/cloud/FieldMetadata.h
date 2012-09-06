#ifndef FIELD_METADATA_H
#define FIELD_METADATA_H

#include <jni.h>
#include "Util.h"
#include "Java.h"

class FieldMetadata
{
private:
  JNIEnv* env;

  jobject create_metadata_enum_object(const char *name)
  {
    jclass metadata_class = this->env->FindClass("com/nearinfinity/hbaseclient/ColumnMetadata");
    jfieldID enum_field = this->env->GetStaticFieldID(metadata_class, name, "Lcom/nearinfinity/hbaseclient/ColumnMetadata;");
    jobject enum_object = this->env->GetStaticObjectField(metadata_class, enum_field);

    return enum_object;
  }

  jbyteArray type_enum_to_byte_array(const char *name)
  {
    jclass metadata_class = this->env->FindClass("com/nearinfinity/hbaseclient/ColumnType");
    jfieldID enum_field = this->env->GetStaticFieldID(metadata_class, name, "Lcom/nearinfinity/hbaseclient/ColumnType;");
    jobject enum_object = this->env->GetStaticObjectField(metadata_class, enum_field);
    jmethodID get_value_method = this->env->GetMethodID(metadata_class, "getValue", "()[B");

    return (jbyteArray)this->env->CallObjectMethod(enum_object, get_value_method);
  }

  jbyteArray string_to_java_byte_array(const char *string)
  {
    //TODO: Need to free this? - jre
    jstring java_string = this->env->NewStringUTF(string);
    jclass string_class = this->env->FindClass("java/lang/String");
    jmethodID get_bytes_method = this->env->GetMethodID(string_class, "getBytes", "()[B");
    
    return (jbyteArray)this->env->CallObjectMethod(java_string, get_bytes_method);
  }

  jbyteArray long_to_java_byte_array(longlong val)
  {
    val = __builtin_bswap64(val);
    uint array_len = sizeof(longlong);
    //TODO: Need to free this? - jre
    jbyteArray array = this->env->NewByteArray(array_len);
    this->env->SetByteArrayRegion(array, 0, array_len, (jbyte*)&val);

    return array;
  }

public:
  FieldMetadata(JNIEnv* env) : env(env)
  {
  }

  jobject get_field_metadata(Field *field, TABLE *table_arg)
  {
    jobject map = create_java_map(this->env);
    switch (field->real_type())
    {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_YEAR:
        if (is_unsigned_field(field))
        {
          java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("ULONG"), this->env);
        } else {
          java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("LONG"), this->env);
        }
        break;
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
        java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("DOUBLE"), this->env);
          break;
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("DECIMAL"), this->env);
        break;
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
          java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("DATE"), this->env);
          break;
      case MYSQL_TYPE_TIME:
          java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("TIME"), this->env);
          break;
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP:
          java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("DATETIME"), this->env);
          break;
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
        {
          longlong max_data_length = (longlong)field->max_data_length();
          if (field->maybe_null())
          {
            //If the field might be null, MySQL builds an extra byte into max_data_length. We want to ignore that.
            max_data_length--;
          }
          java_map_insert(map, create_metadata_enum_object("MAX_LENGTH"), long_to_java_byte_array(max_data_length), this->env);
          if (field->binary())
          {
            java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("BINARY"), this->env);
          } else {
            java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("STRING"), this->env);
          }
        }
        break;
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
        java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("BINARY"), this->env);
        break;
      case MYSQL_TYPE_ENUM:
        java_map_insert(map, create_metadata_enum_object("COLUMN_TYPE"), type_enum_to_byte_array("ULONG"), this->env);
        break;
      case MYSQL_TYPE_NULL:
      case MYSQL_TYPE_BIT:
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_GEOMETRY:
      case MYSQL_TYPE_VAR_STRING:
      default:
        break;
    }

    if (field->real_maybe_null())
    {
      java_map_insert(map, create_metadata_enum_object("IS_NULLABLE"), string_to_java_byte_array("True"), this->env);
    }

    // 64 is obviously some key flag indicating no primary key, but I have no idea where it's defined. Will fix later. - ABC
    if (table_arg->s->primary_key != 64)
    {
      if (strcmp(table_arg->s->key_info[table_arg->s->primary_key].key_part->field->field_name, field->field_name) == 0)
      {
        java_map_insert(map, create_metadata_enum_object("PRIMARY_KEY"), string_to_java_byte_array("True"), this->env);
      }
    }

    return map;
  }
};

#endif
