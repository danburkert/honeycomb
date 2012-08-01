#include "HBaseAdapter.h"

bool HBaseAdapter::create_table(std::string table_name, std::vector<std::string> columns)
{
    jclass adapter_class = this->env->FindClass("HBaseAdapter");
    jmethodID create_table_method = this->env->GetStaticMethodID(adapter_class, "createTable", "(Ljava/lang/String;Ljava/util/List;)Z");
    jstring java_table_name = this->string_to_java_string(table_name);
    jobject java_list = this->vector_to_java_list(columns);
    return this->env->CallStaticBooleanMethod(adapter_class, create_table_method, java_table_name, java_list);
}

long long HBaseAdapter::start_scan(std::string table_name)
{
    jclass adapter_class = this->env->FindClass("HBaseAdapter");
    jmethodID start_scan_method = this->env->GetStaticMethodID(adapter_class, "startScan", "(Ljava/lang/String;)J");
    jstring java_table_name = this->string_to_java_string(table_name);
    return this->env->CallStaticLongMethod(adapter_class, start_scan_method, java_table_name);
    
}

void HBaseAdapter::end_scan(long long scan_id)
{
  jclass adapter_class = this->env->FindClass("HBaseAdapter");
  jmethodID end_scan_method = this->env->GetStaticMethodID(adapter_class, "end_scan", "(J)V");
  jlong java_scan_id = scan_id;
  this->env->CallStaticVoidMethod(adapter_class, end_scan_method, java_scan_id);
}

bool HBaseAdapter::write_row(std::map<std::string, unsigned char*> values)
{
    return true;
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
