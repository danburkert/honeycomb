#ifndef JAVAFRAME_H
#define JAVAFRAME_H

#include "Logging.h"
#include "Macros.h"

class JavaFrame
{
  private:
    JNIEnv* env;
  public:
    JavaFrame(JNIEnv* env, int capacity = 10)
    {
      this->env = env;
      int result = env->PushLocalFrame(capacity);
      CHECK_JNI_ABORT(result, "JavaFrame: Out of memory exception thrown in PushLocalFrame");
    }

    ~JavaFrame()
    {
      this->env->PopLocalFrame(NULL);
    }
};

#endif
