#include "JNISetup.h"

static const char* prefix = "-Djava.class.path=";
static const int prefix_length = strlen(prefix);

static int line_length(FILE* option_config)
{
  int ch;
  fpos_t start;
  long int start_pos = ftell(option_config);
  fgetpos(option_config, &start);
  do
  {
    ch = fgetc(option_config);
  }
  while(ch != '\n' && ch != EOF);
  long int end_pos = ftell(option_config);
  fsetpos(option_config, &start);

  return end_pos - start_pos;
}

static int option_count(FILE* option_config)
{
  int ch, count = 0;
  do
  {
    ch = fgetc(option_config);
    if (ch == '\n')
    {
      count++;
    }

  }
  while(ch != EOF);
  rewind(option_config);

  return count;
}

static JavaVMOption* initialize_options(char* class_path, int* opt_count)
{
  JavaVMOption* options, *option;
  FILE* option_config = fopen("/etc/mysql/jvm-options.conf", "r");
  *opt_count = 1;    
  if (option_config != NULL)
  {
    *opt_count += option_count(option_config);
    options = new JavaVMOption[*opt_count];
	option = options;
    option->optionString = class_path;
	option++;
	int index = 1;
    while(!feof(option_config))
    {
      int line_len = line_length(option_config);
      if (line_len == 0 || index >= *opt_count)
      {
        break;
      }
      
      option->optionString = new char[line_len];
      fgets(option->optionString, line_len, option_config);
      fgetc(option_config); // Skip the newline
	  option++;
    }

    fclose(option_config);
  }
  else
  {
	Logging::info("No jvm-options.conf found. Using classpath as the only jvm option.");
    options = new JavaVMOption[*opt_count];
    options->optionString = class_path;
  }

  return options;
}

static void destruct(JavaVMOption* options, int option_count)
{
  JavaVMOption* option = options;
  for(int i = 0 ; i < option_count ; i++)
  {
    delete[] option->optionString;		
	option++;
  }

  delete[] options;
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
  }

  jclass adapter_class = find_jni_class("HBaseAdapter", env);
  jmethodID initialize_method = env->GetStaticMethodID(adapter_class, "initialize", "()V");
  env->CallStaticVoidMethod(adapter_class, initialize_method);
  print_java_exception(env);
}

static void test_jvm(bool attach_thread, JavaVM* jvm, JNIEnv* env)
{
#ifndef DBUG_OFF
  if(attach_thread)
  {
    JavaVMAttachArgs attachArgs;
    attachArgs.version = JNI_VERSION_1_6;
    attachArgs.name = NULL;
    attachArgs.group = NULL;
    jint ok = jvm->AttachCurrentThread((void**)&env, &attachArgs);
  }

  jclass adapter_class = find_jni_class("HBaseAdapter", env);
  Logging::info("Adapter class %p", adapter_class);
  print_java_exception(env);
#endif
}

static char* read_classpath_conf_file(FILE* config)
{
  fseek(config, 0, SEEK_END);
  long size = ftell(config);
  rewind(config);
  int class_path_length = prefix_length + size;
  char* class_path = new char[class_path_length];
  strncpy(class_path, prefix, prefix_length);
  fread(class_path + prefix_length, sizeof(char), size, config);
  fclose(config);

  char* newline = strpbrk(class_path, "\n\r");
  if(newline != NULL)
  {
    *newline = '\0';
  }

  return class_path;
}

static char* create_default_classpath()
{
  char* home = getenv("MYSQL_HOME");
  const char* suffix = "/lib/plugin/mysqlengine-0.1-jar-with-dependencies.jar";
  char* class_path = new char[prefix_length + strlen(home) + strlen(suffix)];
  sprintf(class_path, "%s%s%s", prefix, home, suffix);
  FILE* jar = fopen(class_path, "r");
  if(jar == NULL)
  {
    Logging::error("No jar classpath specified and the default jar path %s cannot be opened. Either place \"classpath.conf\" in /etc/mysql/ or create %s. Place the java classpath in classpath.conf.", class_path, class_path);
  }
  else
  {
    fclose(jar);
  }

  return class_path;
}

static char* find_java_classpath()
{
  char* class_path;
  FILE* config = fopen("/etc/mysql/classpath.conf", "r");
  if(config != NULL)
  {
	Logging::info("Reading the path to HBaseAdapter jar out of /etc/mysql/classpath.conf");
    class_path = read_classpath_conf_file(config);
  }
  else
  {
	Logging::info("Trying to construct the default path to the HBaseAdapter jar");
    class_path = create_default_classpath();
  }

  Logging::info("Full class path: %s", class_path);
  return class_path;
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
    test_jvm(true, *jvm, env);
  }
  else
  {
    char* class_path = find_java_classpath();
    JavaVMInitArgs vm_args;
    int option_count;
    JavaVMOption* options = initialize_options(class_path, &option_count);
    vm_args.options = options;
    vm_args.nOptions = option_count;
    vm_args.version = JNI_VERSION_1_6;
    jint result = JNI_CreateJavaVM(jvm, (void**)&env, &vm_args);
    if (result != 0)
    {
      Logging::error("*** Failed to create JVM. Error result = %d ***", result);
	  destruct(options, option_count);
	  return;
    }

    destruct(options, option_count);
    test_jvm(false, *jvm, env);
    initialize_adapter(false, *jvm, env);
    (*jvm)->DetachCurrentThread();
  }
}
