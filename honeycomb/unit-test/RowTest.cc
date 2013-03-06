#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <avro.h>
#include "../Row.h"
#include "TestMacros.h"

/**
 * Generate random byte string of required length
 */
void gen_random_bytes(char *s, const int len) {
  size_t int_size = sizeof(int);

  for (int i = 0; i < len; ++i) {
    int rint = rand();
    s[i] = ((char*) &rint)[0]; // grab LSB of random int
  }
}

/**
 * Generate random C string of required length
 */
void gen_random_string(char *s, const int len) {
  size_t int_size = sizeof(int);
  for (int i = 0; i < (len - 1); ++i) {
    int rint = rand();
    s[i] = ((char*) &rint)[0]; // grab LSB of random int
    s[i] = (s[i] == 0x00) ? 0x01 : s[i]; // ensure not null byte
  }
  s[len - 1] = 0;
}

int test_rand_record_map(Row* row) {
  try(row->reset(), "Error while calling reset in test_rand_record_map.");
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
    try(row->set_bytes_record(keys[i], vals[i], val_lens[i]),
        "Error calling set_bytes_record in test_rand_record_map");
  }

  size_t count;
  try(row->record_count(&count), "Error while calling record_count().");
  assert_that(count == num_records, "Number of records in test_rand_record_map does not match.");

  for (int i = 0; i < num_records; i++)
  {
    const char* get_val;
    size_t size;
    try(row->get_bytes_record(keys[i], &get_val, &size),
        "Error calling get_bytes_record() in test_rand_record_map.");
    assert_that(size == val_lens[i], "Retrieved rand value is wrong size.");
    assert_that(memcmp(vals[i], get_val, size) == 0, "Retrieved rand value is wrong.");
  }

  for(int i = 0; i < num_records; i++)
  {
    delete[] vals[i];
    delete[] keys[i];
  }
  delete[] keys;
  delete[] vals;
  delete[] val_lens;
  return 0;
}

int test_bytes_records(Row* row) {
  try(row->reset(), "Error while calling reset in test_bytes_records.");
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
    try(row->set_bytes_record(keys[i], vals[i], 4),
        "Error while calling set_bytes_record in test_bytes_records.");
  }

  size_t count;
  try(row->record_count(&count), "Error while calling record_count().");
  assert_that(count == 6, "Wrong number of records during test_bytes_records.");

  for (int i = 0; i < 6; i++)
  {
    const char* get_val;
    size_t size;
    try(row->get_bytes_record(keys[i], &get_val, &size),
        "Error calling get_bytes_record in test_bytes_records.");
    assert_that(size == 4, "Retrieved bytes value is wrong size.");
    assert_that(memcmp(vals[i], get_val, size) == 0, "Retrieved bytes value does not match.");
  }

  // Test that puts to existing row keys inserts the new value
  const char* get_val;
  try(row->set_bytes_record(keys[0], vals[1], 4),
      "Error while adding new value to existing key in test_bytes_records.");
  try(row->get_bytes_record(keys[0], &get_val, NULL),
      "Error while retrieving new value from existing key in test_bytes_records.");
  assert_that(memcmp(vals[1], get_val, 4) == 0, "Overwritten bytes record does not match.");

  // Test that a non-existant row key returns NULL
  try(row->get_bytes_record("foozball", &get_val, NULL),
      "Error while retrieving non-existent record in test_bytes_records.");
  assert_that(!get_val, "Retrieving non-existant record does not return NULL.");
  return 0;
}

int test_rand_uuid(Row* row) {
  try(row->reset(), "Error while calling reset in test_rand_uuid.");
  char* uuid_buf = new char[16];
  const char* out_buf;
  gen_random_bytes(uuid_buf, 16);

  try(row->set_UUID(uuid_buf), "Error while calling set_UUID.");
  try(row->get_UUID(&out_buf), "Error while calling get_UUID.");
  assert_that(memcmp(uuid_buf, out_buf, 16) == 0, "Retrieved UUID does not match.");
  delete[] uuid_buf;
  return 0;
}

int test_rand_serde(Row* row_se) {
  try(row_se->reset(), "Error while calling reset in test_serde.");
  Row* row_de = new Row();

  // Setup row with random records & UUID
  try(test_rand_record_map(row_se),
      "Error while calling test_rand_record_map from test_serde.");
  char* uuid_buf = new char[16];
  gen_random_bytes(uuid_buf, 16);
  try(row_se->set_UUID(uuid_buf),
      "Error while calling set_UUID from test_serde.");

  const char* serialized;
  size_t size;
  row_se->serialize(&serialized, &size);

  row_de->deserialize(serialized, (int64_t) size);
  assert_that(row_se->equal(*row_de),
      "The deserialized row does not equal the serialized row.");

  delete[] uuid_buf;
  delete[] serialized;
  delete row_de;
  return 0;
}

int main(int argc, char **argv) {
  srand(time(NULL));
  int ret = 0;
  Row* row = new Row();
  ret |= test_bytes_records(row);
  for (int i = 0; i < 100;   i++) { ret |= test_rand_record_map(row); }
  for (int i = 0; i < 5000; i++) { ret |= test_rand_uuid(row); }
  for (int i = 0; i < 5000; i++) { ret |= test_rand_serde(row); }
  printf("Successfully ran all tests.\n");
  delete row;
  return ret;
}
