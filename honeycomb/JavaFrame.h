#ifndef JAVAFRAME_H
#define JAVAFRAME_H
#include <jni.h>

/**
  * @brief Creates a frame for tracking local references for JNI objects. The destructors frees JNI objects on scope exit.
 *
 * Pretty much any call into JNIEnv that return a non-primitive (i.e. inherits from jobject) is a local reference.
 * Java provides a set of functions for explicitly managing the lifetime
 * of JNI local references instead of waiting for the JVM to cleanup the objects. PushLocalFrame and PopLocalFrame are 
 * used to save a frame of the allocated JNI objects. 
 */
class JavaFrame
{
  private:
    JNIEnv* env;
  public:
    JavaFrame(JNIEnv* env, int capacity = 10);
    ~JavaFrame();
};

#endif
