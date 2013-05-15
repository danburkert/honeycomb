#include "JavaFrame.h"
#include "Logging.h"
#include "Macros.h"
#include <jni.h>

/**
 * Create JNI Stack Frame.  If enough memory is not able to be allocated, log
 * errors and abort.
 */
JavaFrame::JavaFrame(JNIEnv* env, int capacity) : env(env)
{
  if (env->PushLocalFrame(capacity))
  {
    const char* msg = "Unable to push local frame.  Out of memory. Aborting.";
    perror(msg);
    Logging::fatal(msg);
    abort();
  }
}

JavaFrame::~JavaFrame()
{
  env->PopLocalFrame(NULL);
}
