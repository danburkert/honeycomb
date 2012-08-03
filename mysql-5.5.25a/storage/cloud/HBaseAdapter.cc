#include "HBaseAdapter.h"
#include "JVMThreadAttach.h"

jboolean HBaseAdapter::create_table(jstring table_name, jobject columns)
{
  JVMThreadAttach attached_thread(this->env, this->jvm);
  jclass adapter_class = this->adapter_class();
  if (adapter_class == NULL)
  {
    DBUG_PRINT("Error", ("Could not find adapter class HBaseAdapter"));
    return false;
  }

  jmethodID create_table_method = this->env->GetStaticMethodID(adapter_class, "createTable", "(Ljava/lang/String;Ljava/util/List;)Z");
  jboolean result = this->env->CallStaticBooleanMethod(adapter_class, create_table_method, table_name, columns);
  DBUG_PRINT("INFO", ("Result of createTable: %d", result));
  return result;
}

jlong HBaseAdapter::start_scan(jstring table_name)
{
  /*
  JVMThreadAttach attached_thread(this->env, this->jvm);
  jclass adapter_class = this->env->FindClass("HBaseAdapter");
  jmethodID start_scan_method = this->env->GetStaticMethodID(adapter_class, "startScan", "(Ljava/lang/String;)J");
  jstring java_table_name = this->string_to_java_string(table_name);
  return this->env->CallStaticLongMethod(adapter_class, start_scan_method, java_table_name);*/
  return 0;
}

void HBaseAdapter::end_scan(jlong scan_id)
{
  /*
  JVMThreadAttach attached_thread(this->env, this->jvm);
  jclass adapter_class = this->env->FindClass("HBaseAdapter");
  jmethodID end_scan_method = this->env->GetStaticMethodID(adapter_class, "end_scan", "(J)V");
  jlong java_scan_id = scan_id;
  this->env->CallStaticVoidMethod(adapter_class, end_scan_method, java_scan_id);
  */
}

jboolean HBaseAdapter::write_row(jstring table_name, jobject *row)
{
  /*
  JVMThreadAttach attached_thread(this->env, this->jvm);
  return true;*/
  return 0;
}

jobject* HBaseAdapter::next_row(jlong scan_id)
{
  /*
  JVMThreadAttach attached_thread(this->env, this->jvm);
  jclass adapter_class = this->env->FindClass("HBaseAdapter");
  jclass row_class = this->env->FindClass("Row");
  jmethodID next_row_method = this->env->GetStaticMethodID(adapter_class, "next_row", "(J)Lcom/nearinfinity/mysqlengine/jni/Row;");
  jlong java_scan_id = scan_id;
  jobject row = this->env->CallStaticObjectMethod(adapter_class, next_row_method, java_scan_id);

  jmethodID get_keys_method = this->env->GetMethodID(row_class, "getKeys", "()[Ljava/lang/String;");
  jmethodID get_vals_method = this->env->GetMethodID(row_class, "getValues", "()[[B");

  jarray keys = (jarray) this->env->CallObjectMethod(row, get_keys_method);
  jarray vals = (jarray) this->env->CallObjectMethod(row, get_vals_method);

  std::map<std::string, char*>* row_map = new std::map<std::string, char*>();
  std::string key;
  char* val;

  jboolean is_copy = JNI_FALSE;

  jsize size = this->env->GetArrayLength(keys);
  for(jsize i = 0; i < size; i++) {
    key = java_to_string((jstring) this->env->GetObjectArrayElement((jobjectArray) keys, (jsize) i));
    val = (char*) this->env->GetByteArrayElements((jbyteArray) this->env->GetObjectArrayElement((jobjectArray) vals, i), &is_copy);
    (*row_map)[key] = val;
  }
  return row_map;*/
  return NULL;
}

jclass HBaseAdapter::adapter_class()
{
  return this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
}
