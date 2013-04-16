#ifndef JNISETUP_H
#define JNISETUP_H
#include <jni.h>
#include "Settings.h"

bool try_initialize_jvm(JavaVM** jvm, const Settings& options, jobject* factory);
void attach_thread(JavaVM *jvm, JNIEnv** env, const char* location = "unknown");
jint detach_thread(JavaVM *jvm);

#endif
