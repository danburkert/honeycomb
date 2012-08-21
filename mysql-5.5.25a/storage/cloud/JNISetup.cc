#include "JNISetup.h"

static const char* prefix = "-Djava.class.path=";
static const int prefix_length = strlen(prefix);

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
  INFO(("Adapter class %p", adapter_class));
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
    ERROR(("No jar classpath specified and the default jar path %s cannot be opened. Either place \"classpath.conf\" in /etc/mysql/ or create %s. Place the java classpath in classpath.conf.", class_path, class_path));
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
    class_path = read_classpath_conf_file(config);
  }
  else
  {
    class_path = create_default_classpath();
  }

  INFO(("Full class path: %s", class_path));
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
    test_jvm(true, *jvm, env);
  }
  else
  {
    JavaVMInitArgs vm_args;
#ifdef __APPLE__
    const int option_count = 3;
    JavaVMOption option[option_count];
    option[1].optionString = "-Djava.security.krb5.realm=OX.AC.UK";
    option[2].optionString = "-Djava.security.krb5.kdc=kdc0.ox.ac.uk:kdc1.ox.ac.uk";
#else
    const int option_count = 1;
    JavaVMOption option[option_count];
#endif
    char* class_path = find_java_classpath();
    option[0].optionString = class_path;
    vm_args.nOptions = option_count;
    vm_args.options = option;
    vm_args.version = JNI_VERSION_1_6;
    jint result = JNI_CreateJavaVM(jvm, (void**)&env, &vm_args);
    if (result != 0)
    {
      ERROR(("Failed to create JVM"));
    }

    delete class_path;
    test_jvm(false, *jvm, env);
  }
}
