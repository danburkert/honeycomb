#ifndef JNISETUP_H
#define JNISETUP_H
#include <jni.h>
#include "Settings.h"

jobject initialize_jvm(JavaVM** jvm, Settings* options);
void attach_thread(JavaVM *jvm, JNIEnv** env, const char* location = "unknown");
jint detach_thread(JavaVM *jvm);

#endif
