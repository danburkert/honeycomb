#ifndef MAP_TEST_H
#define MAP_TEST_H

#include <gtest/gtest.h>
template<typename TEntity>
void rand_record_map(TEntity& entity)
{
  ASSERT_FALSE(entity.reset());
  int num_records = rand() % 100; // [0, 100) records
  int key_len;
  char** keys = new char*[num_records];
  char** vals = new char*[num_records];
  int* val_lens = new int[num_records];

  for (int i = 0; i < num_records; i++)
  {
    key_len = (rand() % 51) + 14; // [14, 64) chars per column name
    val_lens[i] = rand() % 1023 + 1; // [1, 1024) bytes per record
    keys[i] = new char[key_len];
    vals[i] = new char[val_lens[i]];
    gen_random_string(keys[i], key_len);
    gen_random_bytes(vals[i], val_lens[i]);
    ASSERT_FALSE(entity.set_value(keys[i], vals[i], val_lens[i]));
  }

  size_t count;
  ASSERT_FALSE(entity.record_count(&count));
  ASSERT_EQ(count, num_records);

  for (int i = 0; i < num_records; i++)
  {
    const char* get_val;
    size_t size;
    ASSERT_FALSE(entity.get_value(keys[i], &get_val, &size));
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
  
template<typename TEntity>
void bytes_record(TEntity& entity)
{
  ASSERT_FALSE(entity.reset());
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
    ASSERT_FALSE(entity.set_value(keys[i], vals[i], 4));
  }

  size_t count;
  ASSERT_FALSE(entity.record_count(&count));
  ASSERT_EQ(count, 6);

  for (int i = 0; i < 6; i++)
  {
    const char* get_val;
    size_t size;
    ASSERT_FALSE(entity.get_value(keys[i], &get_val, &size));
    ASSERT_EQ(size, 4);
    ASSERT_EQ(0, memcmp(vals[i], get_val, size));
  }

  // Test that puts to existing entity keys inserts the new value
  const char* get_val;
  ASSERT_FALSE(entity.set_value(keys[0], vals[1], 4));
  ASSERT_FALSE(entity.get_value(keys[0], &get_val, NULL));
  ASSERT_EQ(0, memcmp(vals[1], get_val, 4));

  // Test that a non-existant entity key returns NULL
  ASSERT_FALSE(entity.get_value("foozball", &get_val, NULL));
  ASSERT_EQ(NULL, get_val);
}

#endif 
