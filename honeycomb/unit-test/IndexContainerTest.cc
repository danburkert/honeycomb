#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <avro.h>
#include "gtest/gtest.h"
#include "../IndexContainer.h"
#include "Generator.h"
#include "map_test.hpp"

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

TEST_F(IndexContainerTest, RandRecords)
{
  for(int i = 0; i < ITERATIONS; i++)
  {
    rand_record_map(index);
  }
}

TEST_F(IndexContainerTest, BytesRecord)
{
  for(int i = 0; i < ITERATIONS; i++)
  {
    bytes_record(index);
  }
}

TEST_F(IndexContainerTest, Name)
{
  int i;
  char name[256];
  const char* name_cmp;
  memset(name, 0, 256);
  index.reset();
  for (i = 0; i < ITERATIONS; i++) 
  {
    snprintf(name, 256, "%s%d", "i", i);
    index.set_name(name);
    name_cmp = index.get_name();
    ASSERT_TRUE(strcmp(name, name_cmp) == 0);
  }
}

TEST_F(IndexContainerTest, CanHaveNullValue)
{
  index.reset();
  const char* value;

  ASSERT_EQ(index.set_bytes_record("test", NULL, 0), 0);
  ASSERT_EQ(index.get_bytes_record("test", &value, NULL), 0);
  ASSERT_TRUE(value == NULL);
}
