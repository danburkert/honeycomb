#ifndef JNISETUP_H
#define JNISETUP_H
#include <jni.h>

class JNICache;

static __thread int thread_attach_count=0;
static JavaVMAttachArgs attach_args = {JNI_VERSION_1_6, NULL, NULL};

void initialize_jvm(JavaVM* &jvm);
jint attach_thread(JavaVM *jvm, JNIEnv* &env);
jint detach_thread(JavaVM *jvm);

#endif
