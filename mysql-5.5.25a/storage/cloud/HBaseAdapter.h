#ifndef HBASE_ADAPTER_H
#define HBASE_ADAPTER_H
#include "my_global.h"
// MySQL stupidly defines macros for min/max
#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

#include <string>
#include <vector>
#include <map>
#include <jni.h>

class HBaseAdapter
{
private:
    JNIEnv* env;
    JavaVM* jvm;

    std::string java_to_string(jstring str);
    jstring string_to_java_string(std::string string);
    jobject vector_to_java_list(std::vector<std::string> columns);
    void attach_current_thread();

public:
    HBaseAdapter (JavaVM* vm) : jvm(vm)
    { 
    }

    virtual ~HBaseAdapter ()
    {
    }

    bool create_table(std::string table_name, std::vector<std::string> columns);
    long long start_scan(std::string table_name);
    void end_scan(long long scan_id);
    bool write_row(std::map<std::string, unsigned char*> values);
};

#endif
