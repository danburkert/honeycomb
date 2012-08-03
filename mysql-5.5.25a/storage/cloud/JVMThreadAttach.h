#ifndef JVM_THREAD_RESOURCE_H
#define JVM_THREAD_RESOURCE_H

class JVMThreadAttach
{
private:
    JNIEnv* env;
    JavaVM* jvm;

public:
  JVMThreadAttach (JNIEnv* jni_env, JavaVM* vm) : env(jni_env), jvm(vm)
  {
    JavaVMAttachArgs attachArgs;
    attachArgs.version = JNI_VERSION_1_6;
    attachArgs.name = NULL;
    attachArgs.group = NULL;
    this->jvm->AttachCurrentThread((void**)&this->env, &attachArgs);
  }

  virtual ~JVMThreadAttach ()
  {
    this->jvm->DetachCurrentThread();
  }
};

#endif
