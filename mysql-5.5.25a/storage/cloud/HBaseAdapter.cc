#include "HBaseAdapter.h"

bool HBaseAdapter::create_table(std::string table_name, std::vector<std::string> columns)
{
  this->attach_current_thread();
  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  if (adapter_class == NULL)
  {
    DBUG_PRINT("Error", ("Could not find adapter class HBaseAdapter"));
    return false;
  }

  jmethodID create_table_method = this->env->GetStaticMethodID(adapter_class, "createTable", "(Ljava/lang/String;Ljava/util/List;)Z");
  jstring java_table_name = this->string_to_java_string(table_name);
  jobject java_list = this->vector_to_java_list(columns);
  jboolean result = this->env->CallStaticBooleanMethod(adapter_class, create_table_method, java_table_name, java_list);
  DBUG_PRINT("INFO", ("Result of the java call %d", result));
  this->jvm->DetachCurrentThread();
  return result;
}

long long HBaseAdapter::start_scan(std::string table_name)
{
  this->attach_current_thread();
  jclass adapter_class = this->env->FindClass("HBaseAdapter");
  jmethodID start_scan_method = this->env->GetStaticMethodID(adapter_class, "startScan", "(Ljava/lang/String;)J");
  jstring java_table_name = this->string_to_java_string(table_name);
  this->jvm->DetachCurrentThread();
  return this->env->CallStaticLongMethod(adapter_class, start_scan_method, java_table_name);
}

void HBaseAdapter::end_scan(long long scan_id)
{
  this->attach_current_thread();
  jclass adapter_class = this->env->FindClass("HBaseAdapter");
  jmethodID end_scan_method = this->env->GetStaticMethodID(adapter_class, "end_scan", "(J)V");
  jlong java_scan_id = scan_id;
  this->env->CallStaticVoidMethod(adapter_class, end_scan_method, java_scan_id);
  this->jvm->DetachCurrentThread();
}

bool HBaseAdapter::write_row(std::map<std::string, unsigned char*> values)
{
  this->attach_current_thread();
  this->jvm->DetachCurrentThread();
  return true;
}

std::map<std::string, char*>* HBaseAdapter::next_row(long long scan_id)
{
  this->attach_current_thread();
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
  this->jvm->DetachCurrentThread();
  return row_map;
}

void HBaseAdapter::attach_current_thread()
{
  JavaVMAttachArgs attachArgs;
  attachArgs.version = JNI_VERSION_1_4;
  attachArgs.name = NULL;
  attachArgs.group = NULL;
  this->jvm->AttachCurrentThread((void**)&this->env, &attachArgs);
}

std::string HBaseAdapter::java_to_string(jstring str)
{
  const char* chars = this->env->GetStringUTFChars(str, NULL);
  std::string results = chars;
  this->env->ReleaseStringUTFChars(str, chars);
  return results;
}

jstring HBaseAdapter::string_to_java_string(std::string string)
{
  return this->env->NewStringUTF(string.c_str());
}

jobject HBaseAdapter::vector_to_java_list(std::vector<std::string> columns)
{
  jclass linked_list = this->env->FindClass("java/util/LinkedList");
  jmethodID constructor = this->env->GetMethodID(linked_list, "<init>", "()V");
  jobject list_object = this->env->NewObject(linked_list, constructor);
  jmethodID add_method = this->env->GetMethodID(linked_list, "add", "(Ljava/lang/Object;)Z");
  std::vector<std::string>::iterator iterator;
  for(iterator = columns.begin(); iterator != columns.end(); iterator++)
  {
    std::string column = *iterator;
    jstring java_column = this->string_to_java_string(column); 
    this->env->CallBooleanMethod(list_object, add_method, java_column);
  }

  return list_object;
}
