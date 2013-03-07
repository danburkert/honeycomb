#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <avro.h>
#include "../Row.h"
#include "Generator.h"
#include "gtest/gtest.h"

const int ITERATIONS = 1000;

class RowTest : public ::testing::Test {
  protected:
    Row row;
};

void rand_record_map(Row* row) {
  ASSERT_FALSE(row->reset());
  int num_records = rand() % 100; // [0, 100) records
  int val_len;
  int key_len;
  char** keys = new char*[num_records];
  char** vals = new char*[num_records];
  int* val_lens = new int[num_records];

  for (int i = 0; i < num_records; i++)
  {
    key_len = (rand() % 500) + 500; // [500, 1000) bytes per key
    val_lens[i] = rand() % 1023 + 1; // [1, 1024) bytes per record
    keys[i] = new char[key_len];
    vals[i] = new char[val_lens[i]];
    gen_random_string(keys[i], key_len);
    gen_random_bytes(vals[i], val_lens[i]);
    ASSERT_FALSE(row->set_bytes_record(keys[i], vals[i], val_lens[i]));
    row->set_bytes_record(keys[i], vals[i], val_lens[i]);
  }

  size_t count;
  ASSERT_FALSE(row->record_count(&count));
  ASSERT_EQ(count, num_records);

  for (int i = 0; i < num_records; i++)
  {
    const char* get_val;
    size_t size;
    ASSERT_FALSE(row->get_bytes_record(keys[i], &get_val, &size));
    ASSERT_EQ(size, val_lens[i]);
    ASSERT_EQ(0, memcmp(vals[i], get_val, size));
  }

  for(int i = 0; i < num_records; i++)
  {
    delete[] vals[i];
    delete[] keys[i];
  }
  delete[] keys;
  delete[] vals;
  delete[] val_lens;
}
TEST_F(RowTest, RandRecordMap) {
  for(int i = 0; i < ITERATIONS; i++) {
    rand_record_map(&row);
  }
}

void bytes_record(Row* row) {
  ASSERT_FALSE(row->reset());
  const char keys[][8] = {"column0",
                          "column1",
                          "column2",
                          "column3",
                          "column4",
                          "column5"};

  char vals[][4] = {{0xaa, 0xbb, 0xcc, 0xdd},
                    {0xde, 0xad, 0xbe, 0xef},
                    {0x61, 0x62, 0x63, 0x64},
                    {0x01, 0x23, 0x45, 0x66},
                    {0x00, 0x00, 0x00, 0x00},
                    {0xff, 0xff, 0xff, 0xff}};

  for (int i = 0; i < 6; i++)
  {
    ASSERT_FALSE(row->set_bytes_record(keys[i], vals[i], 4));
  }

  size_t count;
  ASSERT_FALSE(row->record_count(&count));
  ASSERT_EQ(count, 6);

  for (int i = 0; i < 6; i++)
  {
    const char* get_val;
    size_t size;
    ASSERT_FALSE(row->get_bytes_record(keys[i], &get_val, &size));
    ASSERT_EQ(size, 4);
    ASSERT_EQ(0, memcmp(vals[i], get_val, size));
  }

  // Test that puts to existing row keys inserts the new value
  const char* get_val;
  ASSERT_FALSE(row->set_bytes_record(keys[0], vals[1], 4));
  ASSERT_FALSE(row->get_bytes_record(keys[0], &get_val, NULL));
  ASSERT_EQ(0, memcmp(vals[1], get_val, 4));

  // Test that a non-existant row key returns NULL
  ASSERT_FALSE(row->get_bytes_record("foozball", &get_val, NULL));
  ASSERT_EQ(NULL, get_val);
}
TEST_F(RowTest, BytesRecord) {
  for(int i = 0; i < ITERATIONS; i++) {
    bytes_record(&row);
  }
}

void rand_uuid(Row* row) {
  ASSERT_FALSE(row->reset());
  char* uuid_buf = new char[16];
  const char* out_buf;
  gen_random_bytes(uuid_buf, 16);

  ASSERT_FALSE(row->set_UUID(uuid_buf));
  ASSERT_FALSE(row->get_UUID(&out_buf));
  ASSERT_EQ(0, memcmp(uuid_buf, out_buf, 16));
  delete[] uuid_buf;
}
TEST_F(RowTest, RandUUID) {
  for(int i = 0; i < ITERATIONS; i++) {
    rand_uuid(&row);
  }
}

void rand_ser_de(Row* row_se) {
  ASSERT_FALSE(row_se->reset());
  Row* row_de = new Row();

  // Setup row with random records & UUID
  rand_record_map(row_se);
  char* uuid_buf = new char[16];
  gen_random_bytes(uuid_buf, 16);
  ASSERT_FALSE(row_se->set_UUID(uuid_buf));

  const char* serialized;
  size_t size;
  row_se->serialize(&serialized, &size);

  row_de->deserialize(serialized, (int64_t) size);
  ASSERT_TRUE(row_se->equal(*row_de));

  delete[] uuid_buf;
  delete[] serialized;
  delete row_de;
}
TEST_F(RowTest, RandSerDe) {
  for(int i = 0; i < ITERATIONS; i++) {
    rand_ser_de(&row);
  }
}
