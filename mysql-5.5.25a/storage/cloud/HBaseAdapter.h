#ifndef HBASE_ADAPTER_H
#define HBASE_ADAPTER_H
#include <string>
#include <vector>
#include <map>
#include <jni.h>

class HBaseAdapter
{
private:
    JNIEnv* env;

    std::string java_to_string(jstring str);
    jstring string_to_java_string(std::string string);
    jobject vector_to_java_list(std::vector<std::string> columns);

public:
    HBaseAdapter (JNIEnv* jni_env) : env(jni_env)
    { 
    }

    virtual ~HBaseAdapter ()
    {
    }

    bool create_table(std::string table_name, std::vector<std::string> columns);
    bool write_row(std::map<std::string, unsigned char*> values);
};

#endif
