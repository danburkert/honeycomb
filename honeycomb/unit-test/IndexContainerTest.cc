#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <avro.h>
#include "gtest/gtest.h"
#include "../IndexContainer.h"
#include "Generator.h"

const int ITERATIONS = 100;

class IndexContainerTest : public ::testing::Test
{
  protected:
    IndexContainer index;
    virtual void SetUp() {
      srand(time(NULL));
    }
};

TEST_F(IndexContainerTest, SetType)
{
  ASSERT_FALSE(index.reset());

#define TEST_TYPE(query_type) do {\
  ASSERT_FALSE(index.set_type(query_type)); \
  EXPECT_EQ(query_type, index.get_type()); } while(0);

  TEST_TYPE(IndexContainer::EXACT_KEY);
  TEST_TYPE(IndexContainer::AFTER_KEY);
  TEST_TYPE(IndexContainer::KEY_OR_NEXT);
  TEST_TYPE(IndexContainer::KEY_OR_PREVIOUS);
  TEST_TYPE(IndexContainer::BEFORE_KEY);
  TEST_TYPE(IndexContainer::INDEX_FIRST);
  TEST_TYPE(IndexContainer::INDEX_LAST);
#undef TEST_TYPE
}

void rand_record_map(IndexContainer& index)
{
    ASSERT_FALSE(index.reset());
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
      ASSERT_FALSE(index.set_bytes_record(keys[i], vals[i], val_lens[i]));
    }

    size_t count;
    ASSERT_FALSE(index.record_count(&count));
    ASSERT_EQ(count, num_records);

    for (int i = 0; i < num_records; i++)
    {
      const char* get_val;
      size_t size;
      ASSERT_FALSE(index.get_bytes_record(keys[i], &get_val, &size));
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

TEST_F(IndexContainerTest, RandRecords)
{
  for(int i = 0; i < ITERATIONS; i++)
  {
    rand_record_map(index);
  }
}
