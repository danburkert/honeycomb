#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <avro.h>
#include "gtest/gtest.h"
#include "../ColumnSchema.h"

const int ITERATIONS = 1000;
const int NUM_TYPES = 9;
const int MAX_LENGTH = 65535;
const int MAX_PRECISION = 65;
const int MAX_SCALE = 30;

class ColumnSchemaTest : public ::testing::Test
{
  protected:
    ColumnSchema schema;
    virtual void SetUp() {
      srand(time(NULL));
    }
};

TEST_F(ColumnSchemaTest, Defaults)
{
  ASSERT_FALSE(schema.reset());
  EXPECT_TRUE(schema.get_is_nullable());
  EXPECT_FALSE(schema.get_is_auto_increment());
  EXPECT_EQ(-1, schema.get_max_length());
  EXPECT_EQ(-1, schema.get_scale());
  EXPECT_EQ(-1, schema.get_precision());
};

TEST_F(ColumnSchemaTest, SetType)
{
  ColumnSchema::ColumnType type;
  ASSERT_FALSE(schema.reset());

  type = ColumnSchema::STRING;
  ASSERT_FALSE(schema.set_type(type));
  EXPECT_EQ(type, schema.get_type());

  type = ColumnSchema::BINARY;
  ASSERT_FALSE(schema.set_type(type));
  EXPECT_EQ(type, schema.get_type());

  type = ColumnSchema::ULONG;
  ASSERT_FALSE(schema.set_type(type));
  EXPECT_EQ(type, schema.get_type());

  type = ColumnSchema::LONG;
  ASSERT_FALSE(schema.set_type(type));
  EXPECT_EQ(type, schema.get_type());

  type = ColumnSchema::DOUBLE;
  ASSERT_FALSE(schema.set_type(type));
  EXPECT_EQ(type, schema.get_type());

  type = ColumnSchema::DECIMAL;
  ASSERT_FALSE(schema.set_type(type));
  EXPECT_EQ(type, schema.get_type());

  type = ColumnSchema::TIME;
  ASSERT_FALSE(schema.set_type(type));
  EXPECT_EQ(type, schema.get_type());

  type = ColumnSchema::DATE;
  ASSERT_FALSE(schema.set_type(type));
  EXPECT_EQ(type, schema.get_type());

  type = ColumnSchema::DATETIME;
  ASSERT_FALSE(schema.set_type(type));
  EXPECT_EQ(type, schema.get_type());
};

TEST_F(ColumnSchemaTest, SetIsNullable)
{
  ASSERT_FALSE(schema.reset());

  ASSERT_FALSE(schema.set_is_nullable(false));
  EXPECT_FALSE(schema.get_is_nullable());

  ASSERT_FALSE(schema.set_is_nullable(true));
  EXPECT_TRUE(schema.get_is_nullable());
};

TEST_F(ColumnSchemaTest, SetIsAutoIncrement)
{
  ASSERT_FALSE(schema.reset());

  ASSERT_FALSE(schema.set_is_auto_increment(false));
  EXPECT_FALSE(schema.get_is_auto_increment());

  ASSERT_FALSE(schema.set_is_auto_increment(true));
  EXPECT_TRUE(schema.get_is_auto_increment());
};

TEST_F(ColumnSchemaTest, SetMaxLength)
{
  int length = rand() % MAX_LENGTH;
  ASSERT_FALSE(schema.reset());

  ASSERT_FALSE(schema.set_max_length(length));
  EXPECT_EQ(length, schema.get_max_length());
};

TEST_F(ColumnSchemaTest, SetScale)
{
  int scale = rand() % MAX_SCALE;
  ASSERT_FALSE(schema.reset());

  ASSERT_FALSE(schema.set_scale(scale));
  EXPECT_EQ(scale, schema.get_scale());
};

TEST_F(ColumnSchemaTest, SetPrecision)
{
  int precision = rand() % MAX_PRECISION + 1;
  ASSERT_FALSE(schema.reset());

  ASSERT_FALSE(schema.set_precision(precision));
  EXPECT_EQ(precision, schema.get_precision());
};

/**
 * Randomize the schema.
 */
void column_schema_gen(ColumnSchema* schema) {
  ASSERT_FALSE(schema->set_type((ColumnSchema::ColumnType) (rand() % NUM_TYPES)));
  ASSERT_FALSE(schema->set_is_nullable(rand() % 2));
  ASSERT_FALSE(schema->set_is_auto_increment(rand() % 2));
  ASSERT_FALSE(schema->set_max_length(rand() % MAX_LENGTH));
  ASSERT_FALSE(schema->set_precision(rand() % MAX_PRECISION + 1));
  ASSERT_FALSE(schema->set_scale(rand() % MAX_SCALE));
}

void test_ser_de(ColumnSchema* schema)
{
  ASSERT_FALSE(schema->reset());
  ColumnSchema* schema_de = new ColumnSchema();

  column_schema_gen(schema);

  const char* serialized;
  size_t size;
  schema->serialize(&serialized, &size);

  schema_de->deserialize(serialized, (int64_t) size);
  ASSERT_TRUE(schema->equal(*schema_de));

  delete[] serialized;
  delete schema_de;
};
TEST_F(ColumnSchemaTest, SerDe)
{
  for (int i = 0; i < ITERATIONS; i++)
  {
    test_ser_de(&schema);
  }
};
