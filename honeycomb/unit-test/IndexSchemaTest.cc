#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <avro.h>
#include "TestMacros.h"
#include "gtest/gtest.h"
#include "../IndexSchema.h"

class IndexTest : public ::testing::Test {
  protected:
    IndexSchema schema;
};

TEST_F(IndexTest, Defaults) {
  ASSERT_FALSE(schema.reset());
  ASSERT_FALSE(schema.get_is_unique());
  ASSERT_EQ(0, schema.size());
};

TEST_F(IndexTest, SetUnique) {
  ASSERT_FALSE(schema.reset());
  ASSERT_FALSE(schema.set_is_unique(true));
  ASSERT_TRUE(schema.get_is_unique());

  ASSERT_FALSE(schema.set_is_unique(false));
  ASSERT_FALSE(schema.get_is_unique());
};

TEST_F(IndexTest, AddColumn) {
  ASSERT_FALSE(schema.reset());
  ASSERT_FALSE(schema.add_column("foobar"));
  const char* str;
  size_t len;
  schema.get_column(0, &str, &len);
  //ASSERT_STREQ("foobar", str);
};
