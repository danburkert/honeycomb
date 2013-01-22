#ifndef JNICACHE_H
#define JNICACHE_H

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
    jclass hbase_adapter_;

  public:
    inline jclass JNICache::hbase_adapter() const {return hbase_adapter_;};

    JNICache(JavaVM* jvm) : jvm(jvm)
    {
      JNIEnv* env;
      jint attach_result = attach_thread(jvm, env);
      if(attach_result != JNI_OK)
      {
        // Handle thread attach issue
      }
      jint frame_result = env->PushLocalFrame(1); // JavaFrame will not work, because we detach before end of scope
      if(frame_result != JNI_OK)
      {
        // Handle out of memory exception
      }

      jclass hbase_adapter_local = env->FindClass(TOFIX "HBaseAdapter");
      if(hbase_adapter_local == NULL)
      {
        // handle class not found exception
      }
      hbase_adapter_ = (jclass) env->NewGlobalRef(hbase_adapter_local);
      if(hbase_adapter_ == NULL)
      {
        // handle out_of_memory exception
      }
      env->PopLocalFrame(NULL);
      detach_thread(jvm);
    }

    ~JNICache()
    {
      // Setup env
      JNIEnv* env;
      attach_thread(jvm, env);

      // Delete global references
      env->DeleteGlobalRef(hbase_adapter_);

      detach_thread(jvm);
    }
};

#endif
