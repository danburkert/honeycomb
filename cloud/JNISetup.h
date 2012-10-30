#ifndef JNISETUP_H
#define JNISETUP_H
#include <jni.h>
#include <stdlib.h>
#include "sql_plugin.h"

#include "Macros.h"
#include "Util.h"
#include "Java.h"

void create_or_find_jvm(JavaVM** jvm);

#endif
