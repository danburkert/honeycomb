#ifndef GENERATOR_H
#define GENERATOR_H

#include "../TableSchema.h"
#include "../IndexSchema.h"
#include "../ColumnSchema.h"

const int NUM_TYPES = 9;
const int MAX_LENGTH = 65535;
const int MAX_PRECISION = 65;
const int MAX_SCALE = 30;

/**
 * Generate random byte string of required length
 */
void gen_random_bytes(char *s, const int len);

/**
 * Generate random C string of required length
 */
void gen_random_string(char *s, const int len);

/**
 * Randomize the ColumnSchema.
 */
void column_schema_gen(ColumnSchema* schema);

/**
 * Randomize the IndexSchema.
 */
void index_schema_gen(IndexSchema* schema);

/**
 * Randomize the TableSchema.
 */
void table_schema_gen(TableSchema* schema);

#endif
