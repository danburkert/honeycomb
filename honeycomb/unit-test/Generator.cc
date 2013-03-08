#include "Generator.h"
#include "gtest/gtest.h"
#include <stdlib.h>

void gen_random_bytes(char *s, const int len) {
  size_t int_size = sizeof(int);

  for (int i = 0; i < len; ++i) {
    int rint = rand();
    s[i] = ((char*) &rint)[0]; // grab LSB of random int
  }
}

void gen_random_string(char *s, const int len) {
  size_t int_size = sizeof(int);
  for (int i = 0; i < (len - 1); ++i) {
    int rint = rand();
    s[i] = ((char*) &rint)[0]; // grab LSB of random int
    s[i] = (s[i] == 0x00) ? 0x01 : s[i]; // ensure not null byte
  }
  s[len - 1] = 0;
}

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

void index_schema_gen(IndexSchema* schema)
{
  ASSERT_FALSE(schema->reset());

  int num_columns = 1 + rand() % 4;
  char column_name[64];
  int length;

  for (int i = 0; i < num_columns; i++)
  {
    length = 1 + rand() % 64;
    gen_random_string(column_name, length);
    ASSERT_FALSE(schema->add_column(column_name));
  }
}

void table_schema_gen(TableSchema* schema)
{
  ASSERT_FALSE(schema->reset());

  int num_columns = rand() % 4096 + 1;
  int num_indices = rand() % 17;

  ColumnSchema column_schema;
  IndexSchema index_schema;

  char name[64];
  int length;

  for (int i = 0; i < num_columns; i++)
  {
    length = 14 + rand() % 51;
    gen_random_string(name, length);
    column_schema_gen(&column_schema);
    ASSERT_FALSE(schema->add_column(name, &column_schema));
  }

  for (int i = 0; i < num_indices; i++)
  {
    length = 14 + rand() % 51;
    gen_random_string(name, length);
    index_schema_gen(&index_schema);
    ASSERT_FALSE(schema->add_index(name, &index_schema));
  }
  ASSERT_EQ(num_columns, schema->column_count());
  ASSERT_EQ(num_indices, schema->index_count());
}
