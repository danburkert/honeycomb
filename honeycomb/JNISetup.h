#ifndef JNISETUP_H
#define JNISETUP_H

struct JNIEnv_;
typedef JNIEnv_ JNIEnv;

class _jobject;
typedef _jobject *jobject;

struct JavaVM_;
typedef JavaVM_ JavaVM;

typedef int jint;

class Settings;


bool try_initialize_jvm(JavaVM** jvm, const Settings& options, jobject* factory);
void attach_thread(JavaVM *jvm, JNIEnv** env, const char* location = "unknown");
jint detach_thread(JavaVM *jvm);

#endif
