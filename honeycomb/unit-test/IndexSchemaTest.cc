#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <avro.h>
#include "TestMacros.h"
#include "IndexSchema.h"

int test_defaults(IndexSchema *schema) {
  try(schema->reset(), "Error while calling reset in test_defaults");
  assert_that(!schema->get_is_unique(), "Unexpected default is_unique value.  Expected false");
  assert_that(schema->size() == 0, "Unexpected default size.  Expected 0");
};

int test_set_unique(IndexSchema *schema) {
  try(schema->reset(), "Error while calling reset in test_set_unique");
  try(schema->set_is_unique(true), "Error while calling set_is_unique");
  assert_that(schema->get_is_unique(), "Unexpected is_unique value.  Expected true");

  try(schema->set_is_unique(false), "Error while calling set_is_unique");
  assert_that(!schema->get_is_unique(), "Unexpected is_unique value.  Expected false");
}

int test_add_column(IndexSchema *schema) {
  try(schema->reset(), "Error while calling reset it test_add_column");
  try(schema->add_column("foobar"), "Error while calling add_column");
  const char* str;
  size_t len;
  schema->get_column(0, &str, &len);
  assert_that(strcmp(str, "foobar") == 0, "Unexpected string return from get_column.  Expected foobar");
}


int main(int argc, char** argv) {
  int ret = 0;
  IndexSchema* schema = new IndexSchema();

  ret |= test_set_unique(schema);
  ret |= test_defaults(schema);
  ret |= test_add_column(schema);

  delete schema;
  //sleep(4);
  return ret;
}
