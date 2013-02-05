#include "JavaFrame.h"
#include "Logging.h"
#include "Macros.h"

JavaFrame::JavaFrame(JNIEnv* env, int capacity)
{
  this->env = env;
  int result = env->PushLocalFrame(capacity);
  CHECK_JNI_ABORT(result, "JavaFrame: Out of memory exception thrown in PushLocalFrame");
}

JavaFrame::~JavaFrame()
{
  this->env->PopLocalFrame(NULL);
}
