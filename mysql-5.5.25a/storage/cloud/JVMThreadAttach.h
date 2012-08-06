#ifndef JVM_THREAD_RESOURCE_H
#define JVM_THREAD_RESOURCE_H

class JVMThreadAttach
{
private:
    JavaVM* jvm;

public:
  JVMThreadAttach (JNIEnv** jni_env, JavaVM* vm)
  {
    jvm = vm;
    JavaVMAttachArgs attachArgs;
    attachArgs.version = JNI_VERSION_1_6;
    attachArgs.name = NULL;
    attachArgs.group = NULL;
    this->jvm->AttachCurrentThread((void**)jni_env, &attachArgs);
    
  }

  virtual ~JVMThreadAttach ()
  {
    if (this->jvm != NULL)
      this->jvm->DetachCurrentThread();
  }
};

#endif
