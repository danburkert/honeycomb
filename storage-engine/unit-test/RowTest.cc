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
#include "Generator.h"
#include "gtest/gtest.h"
#include "../Row.h"

const int ITERATIONS = 1000;

class RowTest : public ::testing::Test
{
  protected:
    Row row;
    virtual void SetUp() {
      srand(time(NULL));
    }
};

TEST_F(RowTest, AddValue)
{
  const char* str = "hello";
  const char* get_val;
  const char* null_val;
  size_t size;
  row.reset();
  row.add_value((char*)str, 6);
  row.add_null();
  row.get_value(0, &get_val, &size);
  row.get_value(1, &null_val, &size);
  ASSERT_TRUE(strcmp(get_val, str) == 0);
  ASSERT_TRUE(null_val == NULL);
}

void rand_uuid(Row& row)
{
  ASSERT_FALSE(row.reset());
  unsigned char* uuid_buf = new unsigned char[16];
  const char* out_buf;
  gen_random_bytes((char *) uuid_buf, 16);

  ASSERT_FALSE(row.set_UUID(uuid_buf));
  ASSERT_FALSE(row.get_UUID(&out_buf));
  ASSERT_EQ(0, memcmp(uuid_buf, out_buf, 16));
  delete[] uuid_buf;
}
TEST_F(RowTest, RandUUID)
{
  for(int i = 0; i < ITERATIONS; i++)
  {
    rand_uuid(row);
  }
}

void rand_ser_de(Row& row_se)
{
  ASSERT_FALSE(row_se.reset());
  Row* row_de = new Row();

  // Setup row with random records & UUID

  int num_records = rand() % 100; // [0, 100) records
  char** vals = new char*[num_records];
  int* val_lens = new int[num_records];
  for (int i = 0; i < num_records; i++)
  {
    val_lens[i] = rand() % 1023 + 1; // [1, 1024) bytes per record
    vals[i] = new char[val_lens[i]];
    gen_random_bytes(vals[i], val_lens[i]);
    row_se.add_value(vals[i], val_lens[i]);
  }

  unsigned char* uuid_buf = new unsigned char[16];
  gen_random_bytes((char*) uuid_buf, 16);
  ASSERT_FALSE(row_se.set_UUID(uuid_buf));

  const char* serialized;
  size_t size;
  row_se.serialize(&serialized, &size);

  row_de->deserialize(serialized, (int64_t) size);
  ASSERT_TRUE(row_se.equals(*row_de));

  for(int i = 0; i < num_records; i++)
  {
    delete[] vals[i];
  }

  delete[] val_lens;
  delete[] vals;
  delete[] uuid_buf;
  delete[] serialized;
  delete row_de;
}
TEST_F(RowTest, RandSerDe)
{
  for(int i = 0; i < ITERATIONS; i++) {
    rand_ser_de(row);
  }
}
