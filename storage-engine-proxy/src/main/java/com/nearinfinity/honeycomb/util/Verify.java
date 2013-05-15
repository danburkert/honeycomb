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
 * Copyright 2013 Altamira Corporation.
 */


package com.nearinfinity.honeycomb.util;

import com.google.common.collect.Sets;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Checks that operations are valid.
 */
public class Verify {
    private static final Logger logger = Logger.getLogger(Verify.class);

    /**
     * Verifies that the provided table schema has an auto increment column name
     *
     * @param schema The table schema to verify
     * @return True if the schema has an auto increment column name, false otherwise
     */
    public static boolean hasAutoIncrementColumn(final TableSchema schema) {
        return schema.getAutoIncrementColumn() != null;
    }

    /**
     * Verifies that the provided entity identifier is valid
     *
     * @param id The entity identifier to verify, must be greater than or equal to 0
     * @param message Additional messages to include on verification failure
     * @throws IllegalArgumentException Thrown if the entity identifier is invalid
     */
    public static void isValidId(final long id, final String... message) {
        checkArgument(id >= 0, "Id must be greater than or equal to zero. " + Arrays.toString(message));
    }

    /**
     * Verifies that the provided value is not empty or null
     *
     * @param value The value to verify
     * @param message Messages to include on verification failure
     * @throws NullPointerException Thrown if the value is null
     * @throws IllegalArgumentException Thrown if the value is empty
     */
    public static void isNotNullOrEmpty(final String value, final String... message) {
        checkNotNull(value, message);
        checkArgument(!value.isEmpty(), message);
    }

    /**
     * Verifies that the table schema is valid
     * @param schema Table schema to verify
     * @throws NullPointerException Thrown if the schema is null
     */
    public static void isValidTableSchema(final TableSchema schema) {
        checkNotNull(schema);
    }

    /**
     * Verifies that the index schema only index columns for columns that are available
     * @param indices A mapping of the index details, not null
     * @param columns A mapping of column details, not null
     * @throws NullPointerException Thrown if the indices or columns container is null
     * @throws IllegalArgumentException Thrown if a {@link IndexSchema} indexes
     *                                  a column that is not an available column
     */
    public static void isValidIndexSchema(final Collection<IndexSchema> indices,
                                          final Collection<ColumnSchema> columns) {
        checkNotNull(indices);
        checkNotNull(columns);

        Set<String> columnNames = Sets.newHashSet();
        for (ColumnSchema column : columns) {
            columnNames.add(column.getColumnName());
        }

        for (final IndexSchema index : indices) {
            for (final String column : index.getColumns()) {
                if (!columnNames.contains(column)) {
                    throw new IllegalArgumentException("Only columns in the table may be indexed.");
                }
            }
        }
    }
}