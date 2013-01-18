#ifndef JAVAFRAME_H
#define JAVAFRAME_H

class JavaFrame
{
  private:
    JNIEnv* env;
  public:
    JavaFrame(JNIEnv* env, int capacity = 10)
    {
      this->env = env;
      this->env->PushLocalFrame(capacity);
    }

    ~JavaFrame()
    {
      this->env->PopLocalFrame(NULL);
    }
};

#endif
