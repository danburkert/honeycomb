#ifndef JAVAFRAME_H
#define JAVAFRAME_H
#include <jni.h>

class JavaFrame
{
  private:
    JNIEnv* env;
  public:
    JavaFrame(JNIEnv* env, int capacity = 10);
    ~JavaFrame();
};

#endif
