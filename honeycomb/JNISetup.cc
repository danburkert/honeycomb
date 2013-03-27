#include "JNISetup.h"
#include <signal.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "Logging.h"
#include "my_global.h"

#include "Macros.h"
#include "Util.h"
#include "Java.h"
#include "JavaFrame.h"
#include "JNICache.h"
#include "OptionParser.h"

static const char* config_file = "/etc/mysql/honeycomb.xml";
static __thread int thread_attach_count=0;
static JavaVMAttachArgs attach_args = {JNI_VERSION_1_6, NULL, NULL};
class JNICache;

__attribute__((noreturn))
static void abort_with_fatal_error(const char* message, ...)
{
    va_list args;
    va_start(args,message);
    int size = strlen(message) + 512;
    char* buffer = new char[size];

    vsnprintf(buffer, size, message, args);
    Logging::fatal(buffer);
    perror(buffer);
    delete[] buffer;
    abort();

    va_end(args);
}

/**
 * Initialize Bootstrap class. This should only be called once per MySQL Server
 * instance during Handlerton initialization.
 */
jobject bootstrap(JavaVM* jvm)
{
  Logging::info("Starting bootstrap()");
  JNIEnv* env;
  jint attach_result = attach_thread(jvm, &env);

  if(attach_result != JNI_OK)
  {
    Logging::fatal("Thread could not be attached in bootstrap()");
    perror("Failed to startup Bootstrap. Check honeycomb.log for details.");
    abort();
  }
  
  // TODO: check the result of these JNI calls with macro
  jclass bootstrap_class = env->FindClass("com/nearinfinity/honeycomb/mysql/Bootstrap");
  jmethodID startup = env->GetStaticMethodID(bootstrap_class, "startup", "()Lcom/nearinfinity/honeycomb/mysql/HandlerProxyFactory;");
  jobject handler_proxy_factory_local = env->CallStaticObjectMethod(bootstrap_class, startup);

  if (print_java_exception(env))
  {
    abort_with_fatal_error("Startup failed with an error. Check"
        "HBaseAdapter.log and honeycomb.log for more information.");
  }

  jobject handler_proxy_factory = env->NewGlobalRef(handler_proxy_factory_local);
  env->DeleteLocalRef(bootstrap_class);
  env->DeleteLocalRef(handler_proxy_factory_local);
  detach_thread(jvm);

  return handler_proxy_factory;
}


static void log_java_classpath(JNIEnv* env)
{
  Logging::info("Java classpath:");
  jclass classloader_class = env->FindClass("java/lang/ClassLoader");
  jclass url_loader_class = env->FindClass("java/net/URLClassLoader");
  jclass url_class = env->FindClass("java/net/URL");
  jmethodID get_class_method = env->GetStaticMethodID(classloader_class, "getSystemClassLoader", "()Ljava/lang/ClassLoader;");
  jobject class_loader = env->CallStaticObjectMethod(classloader_class, get_class_method);
  jmethodID get_urls_method = env->GetMethodID(url_loader_class, "getURLs", "()[Ljava/net/URL;");
  jmethodID get_file_method = env->GetMethodID(url_class, "getFile", "()Ljava/lang/String;");
  jobjectArray urls = (jobjectArray)env->CallObjectMethod(class_loader, get_urls_method);
  jsize length = env->GetArrayLength(urls);
  for(jsize i = 0; i < length; i++)
  {
    jobject url = env->GetObjectArrayElement(urls, i);
    jstring file = (jstring)env->CallObjectMethod(url, get_file_method);
    const char* string = env->GetStringUTFChars(file, NULL);
    Logging::info("%s", string);
    env->ReleaseStringUTFChars(file, string);
    DELETE_REF(env, file);
    DELETE_REF(env, url);
  }
  DELETE_REF(env, urls);
  DELETE_REF(env, class_loader);
}

/**
 *  Allows MySQL to stop during normal shutdown. Restores signal handlers to
 *  MySQL process that were hijacked by the JVM.
 */
extern bool volatile abort_loop;
#if defined(__APPLE__)
extern "C" pthread_handler_t kill_server_thread(void *arg __attribute__((unused)));
static void handler(int sig)
{
  abort_loop = true;
  pthread_t tmp;
  if (mysql_thread_create(0, &tmp, &connection_attrib, kill_server_thread, (void*) &sig))
    sql_print_error("Can't create thread to kill server");
}
#elif defined(__linux__)
extern void kill_mysql(void);
static void handler(int sig)
{
  abort_loop = true;
  kill_mysql();
}
#endif

/**
 * Creates an embedded JVM through the JNI Invocation API and calls
 * bootstrap(). This should only be called once per MySQL Server
 * instance during Handlerton initialization. Aborts process if a JVM already
 * exists. After return the current thread is NOT attached.
 */
jobject initialize_jvm(JavaVM** jvm)
{
  JavaVM* created_vms;
  jsize vm_count;
  jint result = JNI_GetCreatedJavaVMs(&created_vms, sizeof(created_vms), &vm_count);
  if (result == 0 && vm_count > 0) // There is an existing VM
  {
    abort_with_fatal_error("JVM already created. Aborting.");
  }
  else
  {
    JNIEnv* env;
    JavaVMInitArgs vm_args;
    OptionParser* parser = new_parser(config_file);
    if (has_error(parser))
    {
      char* error_message = get_errormessage(parser);
      abort_with_fatal_error(error_message);
    }

    vm_args.options = get_options(parser);
    vm_args.nOptions = get_optioncount(parser);
    vm_args.version = JNI_VERSION_1_6;
    thread_attach_count++; // roundabout to attach_thread
    jint result = JNI_CreateJavaVM(jvm, (void**) &env, &vm_args);
    if (result != JNI_OK)
    {
      abort_with_fatal_error("Failed to create JVM. Check the Java classpath.");
    }
    if (env == NULL)
    {
      abort_with_fatal_error("Environment not created correctly during JVM creation.");
    }

    free_parser(parser);
    jobject handler_proxy_factory = bootstrap(*jvm);
    log_java_classpath(env);
    detach_thread(*jvm);
#if defined(__APPLE__) || defined(__linux__)
    signal(SIGTERM, handler);
#endif
    return handler_proxy_factory;
  }
}

/**
 * Attach current thread to the JVM. Assign current environment to env. Keeps
 * track of how often the current thread has attached, and will not detach
 * until the number of calls to detach is the same as the number of calls to
 * attach.
 *
 * Returns JNI_OK if successful, or a negative number on failure.
 */
jint attach_thread(JavaVM *jvm, JNIEnv** env)
{
  jint result = jvm->AttachCurrentThread((void**) env, &attach_args);

  if ( result == JNI_OK )
  {
    thread_attach_count++;
  }

  return result;
}

/**
 * Detach thread from JVM. Will not detach unless the number of calls to
 * detach is the same as the number of calls to attach.
 *
 * Returns JNI_OK if successful, or a negative number on failure.
 */
jint detach_thread(JavaVM *jvm)
{
  thread_attach_count--;

  if(thread_attach_count <= 0)
  {
    thread_attach_count = 0;
    return jvm->DetachCurrentThread();
  }
  else
  {
    return JNI_OK;
  }
}
