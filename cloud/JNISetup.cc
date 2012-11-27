#include "JNISetup.h"
#include <signal.h>
#include <pthread.h>

static const char* prefix = "-Djava.class.path=";
static const int prefix_length = strlen(prefix);

static void abort_with_fatal_error(const char* message)
{
    Logging::fatal(message);
    perror(message);
    abort();
}

static int line_length(FILE* option_config)
{
  int ch;
  fpos_t start;
  long int start_pos = ftell(option_config), end_pos = 0;
  if(start_pos < 0) { goto error; }
  if(fgetpos(option_config, &start) != 0) { goto error; }
  do
  {
    ch = fgetc(option_config);
  }
  while(ch != '\n' && ch != EOF);
  end_pos = ftell(option_config);
  if(end_pos < 0) { goto error; }
  if(fsetpos(option_config, &start) != 0) { goto error; }

  return end_pos - start_pos;
error:
  return -1;
}

static int option_count(FILE* option_config)
{
  int ch; 
  int count = 0;
  do
  {
    ch = fgetc(option_config);
    if (ch == '\n')
    {
      count++;
    }

  }
  while(ch != EOF);
  if (ferror(option_config)) { goto error; }
  if (fseek(option_config, 0, SEEK_SET) != 0) { goto error; }

  return count;
error:
  return -1;
}

static JavaVMOption* initialize_options(char* class_path, uint* opt_count)
{
  JavaVMOption* options, *option;
  FILE* option_config = fopen("/etc/mysql/jvm-options.conf", "r");
  *opt_count = 1;    
  if (option_config != NULL)
  {
    int count = option_count(option_config);
    if(count < 0) 
    {
      Logging::warn("Could not successfully count the options in /etc/mysql/jvm-options.conf");
      goto error; 
    }

    *opt_count += count;
    options = new JavaVMOption[*opt_count];
	option = options;
    option->optionString = class_path;
	option++;
	int index = 1;
    while(!feof(option_config))
    {
      int line_len = line_length(option_config);
      if (line_len < 0)
      {
        Logging::warn("Line length returned less than 0. Read only %d of %d lines. Not reading the rest of /etc/mysql/jvm-options.conf", index, *opt_count);
        goto error;
      }

      if (line_len == 0 || index >= *opt_count)
      {
        break;
      }
      
      option->optionString = new char[line_len];
      fgets(option->optionString, line_len, option_config);
      fgetc(option_config); // Skip the newline
	  option++;
    }
  }
  else
  {
	Logging::info("No jvm-options.conf found. Using classpath as the only jvm option.");
    options = new JavaVMOption[*opt_count];
    options->optionString = class_path;
  }

error:
  if (option_config)
  {
    fclose(option_config);
  }

  return options;
}

static void destruct(JavaVMOption* options, int option_count)
{
  if(options == NULL)
  {
    return;
  }

  JavaVMOption* option = options;
  for(int i = 0 ; i < option_count ; i++)
  {
    ARRAY_DELETE(option->optionString);
	option++;
  }

  ARRAY_DELETE(options);
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
    abort_with_fatal_error("The HBaseAdapter class could not be found. Make sure classpath.conf has the correct jar path.");
  }

  jmethodID initialize_method = env->GetStaticMethodID(adapter_class, "initialize", "()V");
  env->CallStaticVoidMethod(adapter_class, initialize_method);
  if (print_java_exception(env))
  {
    abort_with_fatal_error("Initialize failed with an error. Check HBaseAdapter.log and cloud.log for more information.");
  }
}

static long file_size(FILE* file)
{
  fseek(file, 0, SEEK_END);
  long size = ftell(file);
  rewind(file);

  return size;
}

static char* read_classpath_conf_file(FILE* config)
{
  char* class_path = NULL, *newline = NULL;
  long class_path_length = 0;
  size_t read_bytes = 0;
  long size = file_size(config);
  if(size <= 0)
  {
    goto error;
  }

  class_path_length = prefix_length + size;
  if (class_path_length < size || class_path_length < 0) // In case the path length wraps around below zero.
  {
    abort_with_fatal_error("The class path is too long.");
  }

  class_path = new char[class_path_length];
  strncpy(class_path, prefix, prefix_length);
  read_bytes = fread(class_path + prefix_length, sizeof(char), size, config);
  if(read_bytes == 0)
  {
    goto error;
  }

  newline = strpbrk(class_path, "\n\r");
  if(newline != NULL)
  {
    *newline = '\0';
  }

  return class_path;
error:
  if(class_path)
  {
    ARRAY_DELETE(class_path);
  }

  return NULL;
}

static char* find_java_classpath()
{
  char* class_path = NULL;
  FILE* config = fopen("/etc/mysql/classpath.conf", "r");
  if(config != NULL)
  {
	Logging::info("Reading the path to HBaseAdapter jar out of /etc/mysql/classpath.conf");
    class_path = read_classpath_conf_file(config);
    fclose(config);
    if (class_path == NULL)
    {
      abort_with_fatal_error("A class path was not found in /etc/mysql/classpath.conf");
    }
  }
  else
  {
    abort_with_fatal_error("Could not open \"classpath.conf\". /etc/mysql/classpath.conf must be readable.");
  }

  Logging::info("Full class path: %s", class_path);
  return class_path;
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
  }
}

extern bool volatile abort_loop;
extern pthread_handler_t kill_server_thread(void *arg);
static void handler(int sig)
{
  abort_loop = true;
  pthread_t tmp;
  if (mysql_thread_create(0, &tmp, &connection_attrib, kill_server_thread, (void*) &sig))
	  sql_print_error("Can't create thread to kill server");
}

void create_or_find_jvm(JavaVM** jvm)
{
  JavaVM* created_vms;
  JNIEnv* env;
  jsize vm_count;
  jint result = JNI_GetCreatedJavaVMs(&created_vms, 1, &vm_count);
  if (result == 0 && vm_count > 0)
  {
    *jvm = created_vms;
    initialize_adapter(true, *jvm, env);
  }
  else
  {
    char* class_path = find_java_classpath();
    JavaVMInitArgs vm_args;
    uint option_count;
    JavaVMOption* options = initialize_options(class_path, &option_count);
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

    destruct(options, option_count);
    print_java_classpath(env);
    initialize_adapter(false, *jvm, env);
    (*jvm)->DetachCurrentThread();
    signal(SIGTERM, handler);
  }
}
