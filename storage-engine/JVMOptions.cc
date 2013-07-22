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
#include "Logging.h"

#include <cstring>
#include <cstdlib>
#include <jni.h>
#include <cctype>

#define OPTION_SEPARATOR "-"
#define CLASSPATH_PREFIX "-Djava.class.path="

class JVMOptionsPrivate
{
  private:
    int index;
  public:
    JavaVMOption* options;
    int count;
    JVMOptionsPrivate(int count) : 
      index(0),
      options(new JavaVMOption[count]), 
      count(count)
    {
    }

    ~JVMOptionsPrivate()
    {
      for(int i = 0; i < count; i++)
      {
        delete[] options[i].optionString;
      }

      delete[] options;
    }

    void add_option_string(char* option_string)
    {
      options[index++].optionString = option_string;
    }
};

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

static int calc_option_count(char* classpath, char* jvm_opts)
{
  int count = 0;
  if (classpath != NULL && strlen(classpath) != 0) 
    count++;
  if (jvm_opts == NULL || strlen(jvm_opts) == 0) 
    return count;
  
  char* saveptr, *opt;
  char* jvm_copy = strdup(jvm_opts); //Stupid strtok_r changes original string
  for(opt = strtok_r(jvm_copy, OPTION_SEPARATOR, &saveptr); 
      opt;
      opt = strtok_r(NULL, OPTION_SEPARATOR, &saveptr))
  {
    if (strlen(trim(opt)) != 0)
      count++;
  }

  free(jvm_copy);

  return count;
}

JVMOptions::JVMOptions() 
{
  char* jvm_opts = trim(getenv(JVM_OPTS));
  char* classpath = trim(getenv(CLASSPATH));
  int count = calc_option_count(classpath, jvm_opts);
  internal = new JVMOptionsPrivate(count);
  internal->options = new JavaVMOption[count];
  set_classpath(classpath);
  set_options(jvm_opts);
}

JVMOptions::~JVMOptions()
{
  delete internal;
}

JavaVMOption* JVMOptions::get_options()
{
  return internal->options;
}

unsigned int JVMOptions::get_options_count()
{
  return internal->count;
}

void JVMOptions::set_classpath(char* classpath)
{
  if (classpath == NULL || strlen(classpath) == 0)
  {
    Logging::error(CLASSPATH " environment variable not set");
    return;
  }

  int len = 1 + strlen(CLASSPATH_PREFIX) + strlen(classpath);
  char* full_classpath = new char[len];
  snprintf(full_classpath, len, "%s%s", classpath_prefix, classpath);
  internal->add_option_string(full_classpath);
}

void JVMOptions::set_options(char* jvm_opts)
{
  if (jvm_opts == NULL || strlen(jvm_opts) == 0)
  {
    Logging::warn(JVM_OPTS " environment variable not set");
    return;
  }

  const int string_pad = 2; // For dash and null terminator
  char* saveptr;
  for(char* opt = strtok_r(jvm_opts, OPTION_SEPARATOR, &saveptr); 
      opt;
      opt = strtok_r(NULL, OPTION_SEPARATOR, &saveptr))
  {
    char* trimmed_opt = trim(opt);
    size_t opt_len = strlen(trimmed_opt) + string_pad;
    if (opt_len == string_pad) // String is zero length
      continue;
    char* string = new char[opt_len];
    snprintf(string, opt_len, "-%s", trimmed_opt);
    internal->add_option_string(string);
  }
}

