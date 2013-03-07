#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <avro.h>
#include "Generator.h"
#include "gtest/gtest.h"
#include "../IndexSchema.h"

const int ITERATIONS = 1000;

class IndexSchemaTest : public ::testing::Test
{
  protected:
    IndexSchema schema;
};

TEST_F(IndexSchemaTest, Defaults)
{
  ASSERT_FALSE(schema.reset());
  ASSERT_FALSE(schema.get_is_unique());
  ASSERT_EQ(0, schema.size());
};

TEST_F(IndexSchemaTest, SetUnique)
{
  ASSERT_FALSE(schema.reset());
  ASSERT_FALSE(schema.set_is_unique(true));
  ASSERT_TRUE(schema.get_is_unique());

  ASSERT_FALSE(schema.set_is_unique(false));
  ASSERT_FALSE(schema.get_is_unique());
};

TEST_F(IndexSchemaTest, AddColumn)
{
  ASSERT_FALSE(schema.reset());
  ASSERT_FALSE(schema.add_column("foobar"));
  const char* str;
  size_t len;
  schema.get_column(0, &str, &len);
  ASSERT_STREQ("foobar", str);
};

void rand_columns_add(IndexSchema* schema)
{
  ASSERT_FALSE(schema->reset());

  int num_columns = 1 + rand() % 4;
  char** column_names = new char*[num_columns];
  int length;

  for (int i = 0; i < num_columns; i++)
  {
    length = 1 + rand() % 64;
    column_names[i] = new char[length];
    gen_random_string(column_names[i], length);

    ASSERT_FALSE(schema->add_column(column_names[i]));
  }

  const char* val;
  for (int i = 0; i < num_columns; i++)
  {
    ASSERT_FALSE(schema->get_column(i, &val, NULL));
    ASSERT_STREQ(column_names[i], val);
    delete[] column_names[i];
  }
  delete[] column_names;
}
TEST_F(IndexSchemaTest, RandColumnsAdd)
{
  for(int i = 0; i < ITERATIONS; i++) {
    rand_columns_add(&schema);
  }
}
