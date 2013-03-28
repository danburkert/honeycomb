#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <avro.h>
#include "Generator.h"
#include "gtest/gtest.h"
#include "../Row.h"
#include "map_test.hpp"

const int ITERATIONS = 1000;

class RowTest : public ::testing::Test
{
  protected:
    Row row;
    virtual void SetUp() {
      srand(time(NULL));
    }
};

TEST_F(RowTest, RandRecordMap)
{
  for(int i = 0; i < ITERATIONS; i++)
  {
    rand_record_map(row);
  }
}


TEST_F(RowTest, BytesRecord)
{
  for(int i = 0; i < ITERATIONS; i++)
  {
    bytes_record(row);
  }
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
  rand_record_map(row_se);
  unsigned char* uuid_buf = new unsigned char[16];
  gen_random_bytes((char*) uuid_buf, 16);
  ASSERT_FALSE(row_se.set_UUID(uuid_buf));

  const char* serialized;
  size_t size;
  row_se.serialize(&serialized, &size);

  row_de->deserialize(serialized, (int64_t) size);
  ASSERT_TRUE(row_se.equals(*row_de));

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
