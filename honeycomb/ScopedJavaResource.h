#ifndef SCOPEDJAVARESOURCE_H
#define SCOPEDJAVARESOURCE_H

#include <jni.h>
#include "Macros.h"
class ScopedJavaResource
{
  private:
    jobject object;
    JNIEnv* env;
  public:
    ScopedJavaResource(JNIEnv* env, jobject object)
    {
      this->env = env;
      this->object = object;
    }

    ~ScopedJavaResource()
    {
      DELETE_REF(env, object);
    }
};

#endif
