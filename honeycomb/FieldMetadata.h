#ifndef FIELD_METADATA_H
#define FIELD_METADATA_H

#include <jni.h>
#include "my_global.h"

class Field;
struct TABLE;
class JNICache;
class FieldMetadata
{
private:
  JNIEnv* env;
  JNICache* cache;

public:
  FieldMetadata(JNIEnv* env, JNICache* cache); 
  jobject get_field_metadata(Field *field, TABLE *table_arg, ulonglong auto_increment_value);
};

#endif
