/*
 * Copyright (C) 2013 Near Infinity Corporation
 *
 * This file is part of Honeycomb Storage Engine.
 *
 * Honeycomb Storage Engine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Honeycomb Storage Engine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Honeycomb Storage Engine.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "JVMOptions.h"
//#include "Logging.h"

#include <cstring>
#include <cstdlib>
#include <jni.h>
#include <cctype>

/**
 * @brief Trim whitespace from left and right of string.
 */
static char* trim(char *string)
{
  if (string == NULL) return string;

  while(isspace(*string)) string++;

 if(*string == 0) return string;

  char *right = string + strlen(string) - 1;
  while(right > string && isspace(*right)) right--;

  *(right + 1) = '\0';

  return string;
}

JVMOptions::JVMOptions()
{
  index = 0;
  char* jvm_opts = trim(getenv(JVM_OPTS));
  char* classpath = trim(getenv(CLASSPATH));
  set_options_count(classpath, jvm_opts);
  options = (JavaVMOption*) malloc(count * sizeof(JavaVMOption));
  set_classpath(classpath);
  set_options(jvm_opts);
}

JVMOptions::~JVMOptions()
{
  for(int i = 0; i < count; i++)
  {
    free(options[i].optionString);
  }

  free(options);
}

JavaVMOption* JVMOptions::get_options()
{
  return options;
}

unsigned int JVMOptions::get_options_count()
{
  return count;
}

void JVMOptions::set_classpath(char* classpath)
{
  if (classpath == NULL || strlen(classpath) == 0)
  {
    //Logging::error(CLASSPATH " environment variable not set");
    return;
  }

  const char* classpath_prefix = "-Djava.class.path=";
  int len = 1 + strlen(classpath_prefix) + strlen(classpath);
  char* full_classpath = (char*) malloc(sizeof(char) * len);
  strcpy(full_classpath, classpath_prefix);
  strcat(full_classpath, classpath);
  options[index++].optionString = full_classpath;
}

void JVMOptions::set_options(char* jvm_opts)
{
  if (jvm_opts == NULL || strlen(jvm_opts) == 0)
  {
    //Logging::warn(JVM_OPTS " environment variable not set");
    return;
  }

  char* left = jvm_opts;
  char* right = jvm_opts;

  while((right = strstr(right, " -")))
  {
    *right = '\0';
    options[index++].optionString = strdup(trim(left));
    left = right + 1;
    right += 2;
  }
  options[index++].optionString = strdup(trim(left));
}

void JVMOptions::set_options_count(char* classpath, char* jvm_opts)
{
  count = 0;
  if (classpath != NULL && strlen(classpath) != 0) count++;
  if (jvm_opts == NULL || strlen(jvm_opts) == 0) return;
  while((jvm_opts = strstr(jvm_opts, " -")))
  {
    jvm_opts += 2;
    count++;
  }
  count++;
}
