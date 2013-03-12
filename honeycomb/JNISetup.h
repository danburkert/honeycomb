#ifndef JNISETUP_H
#define JNISETUP_H
#include <jni.h>


jobject initialize_jvm(JavaVM* &jvm);
jint attach_thread(JavaVM *jvm, JNIEnv* &env);
jint detach_thread(JavaVM *jvm);

#endif
