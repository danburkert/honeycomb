#ifndef FIELD_METADATA_H
#define FIELD_METADATA_H

#include <jni.h>
#include "Util.h"

class FieldMetadata
{
private:
  JNIEnv* env;

  jobject create_java_list()
  {
    jclass list_class = this->env->FindClass("java/util/LinkedList");
    jmethodID list_constructor = this->env->GetMethodID(list_class, "<init>", "()V");
    return this->env->NewObject(list_class, list_constructor);
  }

  void java_list_add(jobject list, jobject obj)
  {
    jclass list_class = this->env->FindClass("java/util/LinkedList");
    jmethodID add_method = this->env->GetMethodID(list_class, "add", "(Ljava/lang/Object;)Z");

    this->env->CallBooleanMethod(list, add_method, obj);
  }

  jobject create_metadata_enum_object(const char *name)
  {
    jclass metadata_class = this->env->FindClass("com/nearinfinity/hbaseclient/ColumnMetadata");
    jfieldID enum_field = this->env->GetStaticFieldID(metadata_class, name, "Lcom/nearinfinity/hbaseclient/ColumnMetadata;");
    jobject enum_object = this->env->GetStaticObjectField(metadata_class, enum_field);

    return enum_object;
  }

public:
  FieldMetadata(JNIEnv* env) : env(env)
  {
  }

  jobject get_field_metadata(Field *field, TABLE *table_arg)
  {
    jobject list = this->create_java_list();
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
          this->java_list_add(list, create_metadata_enum_object("ULONG"));
        } else {
          this->java_list_add(list, create_metadata_enum_object("LONG"));
        }
        break;
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
        this->java_list_add(list, create_metadata_enum_object("DOUBLE"));
          break;
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        this->java_list_add(list, create_metadata_enum_object("DECIMAL"));
        break;
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
          this->java_list_add(list, create_metadata_enum_object("DATE"));
          break;
      case MYSQL_TYPE_TIME:
          this->java_list_add(list, create_metadata_enum_object("TIME"));
          break;
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP:
          this->java_list_add(list, create_metadata_enum_object("DATETIME"));
          break;
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
        if (field->binary())
        {
          this->java_list_add(list, create_metadata_enum_object("BINARY"));
        } else {
          this->java_list_add(list, create_metadata_enum_object("STRING"));
        }
        break;
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
        this->java_list_add(list, create_metadata_enum_object("BINARY"));
        break;
      case MYSQL_TYPE_ENUM:
        this->java_list_add(list, create_metadata_enum_object("ULONG"));
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
      this->java_list_add(list, create_metadata_enum_object("IS_NULLABLE"));
    }

    // 64 is obviously some key flag indicating no primary key, but I have no idea where it's defined. Will fix later. - ABC
    if (table_arg->s->primary_key != 64)
    {
      if (strcmp(table_arg->s->key_info[table_arg->s->primary_key].key_part->field->field_name, field->field_name) == 0)
      {
        this->java_list_add(list, create_metadata_enum_object("PRIMARY_KEY"));
      }
    }

    return list;
  }
};

#endif
