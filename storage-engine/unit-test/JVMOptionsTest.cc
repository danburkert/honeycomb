/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright 2013 Near Infinity Corporation.
 */

#include "gtest/gtest.h"
#include "JVMOptions.h"
#include <jni.h>
#include <Logging.h>

struct JavaVMOption;

class JVMOptionsTest : public ::testing::Test
{
  protected:
    virtual void SetUp() {
      Logging::setup_logging(NULL);
    }
};


TEST_F(JVMOptionsTest, Classpath)
{
  setenv(JVM_OPTS, "", 1);

  {
    const char* classpath = "foo:bar:baz";
    const char* full_classpath = "-Djava.class.path=foo:bar:baz";
    setenv(CLASSPATH, classpath, 1);
    JVMOptions options;
    ASSERT_STREQ(full_classpath, options.get_options()[0].optionString);
  }

  {
    const char* classpath = "   foo:bar:baz         ";
    const char* full_classpath = "-Djava.class.path=foo:bar:baz";
    setenv(CLASSPATH, classpath, 1);
    JVMOptions options;
    ASSERT_STREQ(full_classpath, options.get_options()[0].optionString);
  }

  {
    setenv(CLASSPATH, "", 1);
    setenv(JVM_OPTS, "-buzz", 1);
    JVMOptions options;
    ASSERT_STREQ("-buzz", options.get_options()[0].optionString);
  }

  {
    unsetenv(CLASSPATH);
    setenv(JVM_OPTS, "-buzz", 1);
    JVMOptions options;
    ASSERT_STREQ("-buzz", options.get_options()[0].optionString);
  }

};

TEST_F(JVMOptionsTest, OptionsCount)
{
  setenv(CLASSPATH, "foo:bar:baz", 1);

  {
    setenv(JVM_OPTS, "", 1);
    JVMOptions options;
    ASSERT_EQ(1, options.get_options_count());
  }

  {
    setenv(JVM_OPTS, "-foo", 1);
    JVMOptions options;
    ASSERT_EQ(2, options.get_options_count());
  }

  {
    setenv(JVM_OPTS, "-foo = bar", 1);
    JVMOptions options;
    ASSERT_EQ(2, options.get_options_count());
  }

  {
    setenv(JVM_OPTS, "-foo -bar", 1);
    JVMOptions options;
    ASSERT_EQ(3, options.get_options_count());
  }
  {
    setenv(JVM_OPTS, "-foo -", 1);
    JVMOptions options;
    ASSERT_EQ(3, options.get_options_count());
  }
  {
    setenv(JVM_OPTS, "-foo -      ", 1);
    JVMOptions options;
    ASSERT_EQ(3, options.get_options_count());
  }

  {
    setenv(JVM_OPTS, "-foo --bar", 1);
    JVMOptions options;
    ASSERT_EQ(3, options.get_options_count());
  }
  {
    setenv(JVM_OPTS, "-foo -- ", 1);
    JVMOptions options;
    ASSERT_EQ(3, options.get_options_count());
  }
  {
    setenv(JVM_OPTS, "-foo - -bar", 1);
    JVMOptions options;
    ASSERT_EQ(4, options.get_options_count());
  }
  {
    setenv(JVM_OPTS, "-foo - -bar-foo", 1);
    JVMOptions options;
    ASSERT_EQ(4, options.get_options_count());
  }
  {
    setenv(JVM_OPTS, "   -foo=     bar -baz    ", 1);
    JVMOptions options;
    ASSERT_EQ(3, options.get_options_count());
  }

  {
    setenv(CLASSPATH, "  ", 1);
    setenv(JVM_OPTS, "  ", 1);
    JVMOptions options;
    ASSERT_EQ(0, options.get_options_count());
  }


  {
    unsetenv(CLASSPATH);
    unsetenv(JVM_OPTS);
    JVMOptions options;
    ASSERT_EQ(0, options.get_options_count());
  }

};

TEST_F(JVMOptionsTest, Options)
{
  setenv(CLASSPATH, "foo:bar:baz", 1);

  {
    setenv(JVM_OPTS, "-foo", 1);
    JVMOptions options;
    ASSERT_STREQ("-foo", options.get_options()[1].optionString);
  }

  {
    setenv(JVM_OPTS, "-foo = bar", 1);
    JVMOptions options;
    ASSERT_STREQ("-foo = bar", options.get_options()[1].optionString);
  }

  {
    setenv(JVM_OPTS, "-foo -bar", 1);
    JVMOptions options;
    ASSERT_STREQ("-foo", options.get_options()[1].optionString);
    ASSERT_STREQ("-bar", options.get_options()[2].optionString);
  }

  {
    setenv(JVM_OPTS, "   -foo=     bar  -baz    ", 1);
    JVMOptions options;
    ASSERT_STREQ("-foo=     bar", options.get_options()[1].optionString);
    ASSERT_STREQ("-baz", options.get_options()[2].optionString);
  }

  {
    setenv(JVM_OPTS, "-foo -bar -baz  ", 1);
    JVMOptions options;
    ASSERT_STREQ("-foo", options.get_options()[1].optionString);
    ASSERT_STREQ("-bar", options.get_options()[2].optionString);
    ASSERT_STREQ("-baz", options.get_options()[3].optionString);
  }

  {
    setenv(JVM_OPTS, "-foo -bar   -baz  ", 1);
    JVMOptions options;
    ASSERT_STREQ("-foo", options.get_options()[1].optionString);
    ASSERT_STREQ("-bar", options.get_options()[2].optionString);
    ASSERT_STREQ("-baz", options.get_options()[3].optionString);
  }

  {
    setenv(JVM_OPTS, "-XX:+UseConcMarkSweepGC -Xmx2g -Xms1g -ea -server -Xrs", 1);
    JVMOptions options;
    ASSERT_STREQ("-XX:+UseConcMarkSweepGC", options.get_options()[1].optionString);
    ASSERT_STREQ("-Xmx2g", options.get_options()[2].optionString);
    ASSERT_STREQ("-Xms1g", options.get_options()[3].optionString);
    ASSERT_STREQ("-ea", options.get_options()[4].optionString);
    ASSERT_STREQ("-server", options.get_options()[5].optionString);
    ASSERT_STREQ("-Xrs", options.get_options()[6].optionString);
  }
  {
    setenv(JVM_OPTS, "-XX:-UseConcMarkSweepGC -Xmx2g -Xms1g -ea -server -Xrs", 1);
    JVMOptions options;
    ASSERT_STREQ("-XX:-UseConcMarkSweepGC", options.get_options()[1].optionString);
    ASSERT_STREQ("-Xmx2g", options.get_options()[2].optionString);
    ASSERT_STREQ("-Xms1g", options.get_options()[3].optionString);
    ASSERT_STREQ("-ea", options.get_options()[4].optionString);
    ASSERT_STREQ("-server", options.get_options()[5].optionString);
    ASSERT_STREQ("-Xrs", options.get_options()[6].optionString);
  }

};
