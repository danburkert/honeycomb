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

#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>

static const char* config_file = "/etc/mysql/honeycomb.xml";
static char* rtrim(char* string)
{
  char* original = string + strlen(string);
  while(isspace(*--original));
  *(original + 1) = '\0';
  return string;
}

static char* ltrim(char *string)
{
  char* original = string;
  char *p = original;
  int trimmed = 0;
  do
  {
    if (!isspace(*original) || trimmed)
    {
      trimmed = 1;
      *p++ = *original;
    }
  }
  while (*original++ != '\0');
  return string;
}


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

static void initialize_adapter(bool attach_thread, JavaVM* jvm, JNIEnv* env)
{
  Logging::info("Initializing HBaseAdapter");
  if(attach_thread)
  {
    JavaVMAttachArgs attachArgs;
    attachArgs.version = JNI_VERSION_1_6;
    attachArgs.name = NULL;
    attachArgs.group = NULL;
    jint ok = jvm->AttachCurrentThread((void**)&env, &attachArgs);
    if (ok != 0)
    {
      abort_with_fatal_error("Attach to current thread failed while trying to initialize the HBaseAdapter.");
    }
  }

  jclass adapter_class = find_jni_class("HBaseAdapter", env);
  if (adapter_class == NULL)
  {
    abort_with_fatal_error("The HBaseAdapter class could not be found. Make sure %s has the correct jar path.", config_file);
  }

  jmethodID initialize_method = env->GetStaticMethodID(adapter_class, "initialize", "()V");
  env->CallStaticVoidMethod(adapter_class, initialize_method);
  if (print_java_exception(env))
  {
    abort_with_fatal_error("Initialize failed with an error. Check HBaseAdapter.log and honeycomb.log for more information.");
  }
}

static void test_config_file(const char* config_file)
{
  FILE* config = fopen(config_file, "r");
  if(config != NULL)
  {
    fclose(config);
  }
  else
  {
    abort_with_fatal_error("Could not open \"%s\". %s must be readable.", config_file, config_file);
  }
}

static void print_java_classpath(JNIEnv* env)
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

extern bool volatile abort_loop;
#if defined(__APPLE__)
extern pthread_handler_t kill_server_thread(void *arg __attribute__((unused)));
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

JavaVMOption* read_options(const char* filename, int* count) 
{
  const xmlChar* xpath = (const xmlChar*)"/options/jvmoptions/jvmoption";
  xmlInitParser();
  xmlDocPtr doc = xmlParseFile(filename);
  xmlXPathContextPtr xpath_ctx = xmlXPathNewContext(doc);
  xmlXPathObjectPtr jvm_options = xmlXPathEvalExpression(xpath, xpath_ctx);
  xmlNodeSetPtr option_nodes = jvm_options->nodesetval;
  int option_count = option_nodes->nodeNr;
  *count = option_count;
  JavaVMOption* options = (JavaVMOption*)malloc(option_count * sizeof(JavaVMOption));
  for(int i = 0; i < option_count; i++)
  {
    xmlNodePtr current_option = option_nodes->nodeTab[i];
    char* opt = (char*)xmlNodeListGetString(doc, current_option->xmlChildrenNode, 1);
    options[i].optionString = ltrim(rtrim(opt));
  }

  xmlXPathFreeNodeSet(option_nodes);
  xmlXPathFreeContext(xpath_ctx);
  xmlFreeDoc(doc);
  xmlCleanupParser();
  return options;
}

void create_or_find_jvm(JavaVM** jvm)
{
  JNIEnv* env;
  int option_count;

  test_config_file(config_file);
  JavaVMOption* options = read_options(config_file, &option_count);
  JavaVMInitArgs vm_args;
  vm_args.options = options;
  vm_args.nOptions = option_count;
  vm_args.version = JNI_VERSION_1_6;
  jint result = JNI_CreateJavaVM(jvm, (void**)&env, &vm_args);
  if (result != 0)
  {
    abort_with_fatal_error("*** Failed to create JVM. Check the Java classpath. ***");
  }

  if (env == NULL)
  {
    abort_with_fatal_error("Environment was not created correctly.");
  }

  print_java_classpath(env);
  initialize_adapter(false, *jvm, env);
  (*jvm)->DetachCurrentThread();
#if defined(__APPLE__) || defined(__linux__)
  signal(SIGTERM, handler);
#endif

  for(int i = 0; i < option_count; i++)
  {
    xmlFree(options[i].optionString);
  }

  free(options);
}
