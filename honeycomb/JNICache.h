#ifndef JNICACHE_H
#define JNICACHE_H

#include "JavaFrame.h"
#include "JNISetup.h"

#define TOFIX "com/nearinfinity/honeycomb/mysqlengine/"

/* JNICache holds jmethodID's and jclass global refs to be used in later JNI
 * invocations by Honeycomb.  Upon creation, JNICache asks the JVM for
 * the jmethodID's and jclass refs it needs, and stores them in its fields.
 */
class JNICache
{
  private:
    JavaVM* jvm;

  public:
  const jclass hbase_adapter;

  JNICache(JavaVM* jvm) : jvm(jvm)
  {
    JNIEnv* env;
    attach_thread(jvm, env);
    JavaFrame frame(env, 1); // Number of local references created
    jclass hbase_adapter_local = env->FindClass(TOFIX "HBaseAdapter");
    if(hbase_adapter_local == NULL)
    {
      // handle class not found exception
    }
    hbase_adapter = env->NewGlobalRef(hbase_adapter_local);
    if(hbase_adapter == NULL)
    {
      // handle out_of_memory exception
    }
    detach_thread(jvm);
  }

  ~JNICache()
  {
    // Setup env
    JNIEnv* env;
    attach_thread(jvm, env);

    // Delete global references
    env->DeleteGlobalRef(hbase_adapter);

    detach_thread(jvm);
  }
};


#endif
