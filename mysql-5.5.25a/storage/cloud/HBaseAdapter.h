#ifndef HBASE_ADAPTER_H
#define HBASE_ADAPTER_H
#include "my_global.h"
#include "sql_string.h"
#include <jni.h>

class HBaseAdapter
{
private:
    JNIEnv* env;
    JavaVM* jvm;

    jclass adapter_class();

public:
    HBaseAdapter (JavaVM* vm) : jvm(vm)
    { 
    }

    virtual ~HBaseAdapter ()
    {
    }

    jboolean create_table(jstring table_name, jobject columns);
    jlong start_scan(jstring table_name);
    void end_scan(jlong scan_id);
    jboolean write_row(jstring table_name, jobject* row);
    jobject* next_row(jlong scan_id);

};

#endif
