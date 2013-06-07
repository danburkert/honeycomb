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


#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <avro.h>
#include "gtest/gtest.h"
#include "../TableSchema.h"
#include "Generator.h"

const int ITERATIONS = 100;

class TableSchemaTest : public ::testing::Test
{
  protected:
    TableSchema schema;
    virtual void SetUp() {
      srand(time(NULL));
    }
};

TEST_F(TableSchemaTest, Defaults)
{
  ASSERT_EQ(0, schema.column_count());
  ASSERT_EQ(0, schema.index_count());
};

void test_ser_de(TableSchema* schema)
{
  ASSERT_FALSE(schema->reset());
  TableSchema* schema_de = new TableSchema();

  table_schema_gen(schema);

  const char* serialized;
  size_t size;
  schema->serialize(&serialized, &size);

  schema_de->deserialize(serialized, (int64_t) size);
  ASSERT_TRUE(schema->equals(*schema_de));

  delete[] serialized;
  delete schema_de;
};
TEST_F(TableSchemaTest, RandSerDe)
{
  for (int i = 0; i < ITERATIONS; i++)
  {
    test_ser_de(&schema);
  }
};

void test_add_column(TableSchema& schema)
{
  ASSERT_FALSE(schema.reset());
  ColumnSchema column_schema;
  ColumnSchema returned_schema;
  column_schema_gen(&column_schema);

  char name[64];
  gen_random_string(name, rand() % 50 + 15);

  ASSERT_FALSE(schema.add_column(name, &column_schema));
  ASSERT_EQ(1, schema.column_count());
  ASSERT_FALSE(schema.get_column(name, &returned_schema));
  ASSERT_TRUE(column_schema.equals(returned_schema));
};
TEST_F(TableSchemaTest, AddRandColumn)
{
  for (int i = 0; i < ITERATIONS; i++)
  {
    test_add_column(schema);
  }
}

void test_add_index(TableSchema& schema)
{
  ASSERT_FALSE(schema.reset());
  IndexSchema index_schema;
  IndexSchema returned_schema;
  index_schema_gen(&index_schema);

  char name[64];
  gen_random_string(name, rand() % 50 + 15);

  ASSERT_FALSE(schema.add_index(name, &index_schema));
  ASSERT_EQ(1, schema.index_count());
  ASSERT_FALSE(schema.get_index(name, &returned_schema));
  ASSERT_TRUE(index_schema.equals(returned_schema));
};
TEST_F(TableSchemaTest, AddRandIndex)
{
  for (int i = 0; i < ITERATIONS; i++)
  {
    test_add_column(schema);
  }
};
