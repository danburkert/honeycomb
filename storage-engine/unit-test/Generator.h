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
