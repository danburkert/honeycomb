#ifndef JNICACHE_H
#define JNICACHE_H

#include "JNISetup.h"
#include "JavaFrame.h"

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
    inline jclass hbase_adapter() const {return hbase_adapter_;};

    JNICache(JavaVM* jvm) : jvm(jvm)
    {
      JNIEnv* env;
      jint attach_result = attach_thread(jvm, env);
      if (attach_result != JNI_OK)
      {
        Logging::fatal("Thread could not be attached in JNICache::hbase_adapter");
        perror("Failed to create JNICache. Check honeycomb.log for details.");
        abort();
      }
      { // A new scope is needed so frame will be destructed before detaching
        JavaFrame frame(env, 1);

        jclass hbase_adapter_local = env->FindClass(TOFIX "HBaseAdapter");
        if (hbase_adapter_local == NULL)
        {
          Logging::fatal("HBaseAdapter class not found.");
          perror("Failed to create JNICache. Check honeycomb.log for details.");
          abort();
        }
        hbase_adapter_ = (jclass) env->NewGlobalRef(hbase_adapter_local);
        if (hbase_adapter_ == NULL)
        {
          Logging::fatal("Not enough memory to create global ref in JNICache constructor.");
          perror("Failed to create global ref. Check honeycomb.log for details.");
          abort();
          // handle out_of_memory exception
        }
      }
      detach_thread(jvm);
    }

    ~JNICache()
    {
      // Setup env
      JNIEnv* env;
      jint attach_result = attach_thread(jvm, env);
      if (attach_result != JNI_OK)
      {
        // Handle thread attach issue
      }

      // Delete global references
      env->DeleteGlobalRef(hbase_adapter_);

      detach_thread(jvm);
    }
};

#endif
