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


package com.nearinfinity.honeycomb.hbase.bulkload;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Parser to turn CSV lines into Honeycomb rows
 */
public class RowParser {
    private final TableSchema schema;
    private final String[] columns;
    private final CSVParser csvParser;

    /**
     * Construct a row parser for a specific table format.
     *
     * @param schema Schema of the table to parse data for
     * @param columns Names of the columns in the table
     * @param separator Character separator for the data received.
     */
    public RowParser(TableSchema schema, String[] columns, char separator) {
        this.schema = schema;
        this.columns = columns;
        this.csvParser = new CSVParser(separator);
    }

    /**
     * Parse a line of a CSV into a Honeycomb {@link Row}
     *
     * @param line CSV line
     * @return Honeycomb {@link Row}
     * @throws IOException    The CSV line was improperly formed
     * @throws ParseException The CSV line contained invalid data.
     */
    public Row parseRow(String line) throws IOException, ParseException {
        checkNotNull(line);
        String[] unparsedFields = csvParser.parseLine(line);
        checkArgument((schema.getColumns().size() == unparsedFields.length),
                "Line contains wrong number of columns.");

        ImmutableMap.Builder<String, ByteBuffer> fields = ImmutableMap.builder();

        for (int i = 0; i < columns.length; i++) {
            fields.put(columns[i],
                    FieldParser.parse(unparsedFields[i],
                            schema.getColumnSchema(columns[i])));
        }
        return new Row(fields.build(), UUID.randomUUID());
    }
}