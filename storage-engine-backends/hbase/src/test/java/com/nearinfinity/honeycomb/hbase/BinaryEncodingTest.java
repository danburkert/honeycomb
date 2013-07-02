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


package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import com.nearinfinity.honeycomb.hbase.generators.RowKeyGenerator;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKey;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import net.java.quickcheck.collection.Pair;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class BinaryEncodingTest extends EncodingTest<byte[]> {

    private static TableSchema tableSchema =
            new TableSchema(
                    ImmutableList.of(ColumnSchema.builder(COLUMN, ColumnType.BINARY).setMaxLength(32).build()),
                    ImmutableList.of(new IndexSchema("i1", ImmutableList.of(COLUMN), false))
            );

    @Test
    public void testAscendingCorrectlySortsStrings() {
        List<Pair<byte[], byte[]>> rows = Lists.newArrayList();
        RowKeyGenerator.IndexRowKeyGenerator rowKeyGen =
                RowKeyGenerator.getAscIndexRowKeyGenerator(tableSchema);
        addPairs(rows, rowKeyGen);

        Collections.sort(rows, new RowComparator());

        Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
        for (int i = 1; i < rows.size(); i++) {
            Pair<byte[], byte[]> previous = rows.get(i - 1);
            Pair<byte[], byte[]> current = rows.get(i);
            assertTrueWithPairs(comparator.compare(previous.getFirst(), current.getFirst()) <= 0, previous, current);
        }
    }

    @Test
    public void testDescendingCorrectlySortsStrings() {
        List<Pair<byte[], byte[]>> rows = Lists.newArrayList();
        RowKeyGenerator.IndexRowKeyGenerator rowKeyGen =
                RowKeyGenerator.getDescIndexRowKeyGenerator(tableSchema);
        addPairs(rows, rowKeyGen);

        Collections.sort(rows, new RowComparator());

        Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
        for (int i = 1; i < rows.size(); i++) {
            Pair<byte[], byte[]> previous = rows.get(i - 1);
            Pair<byte[], byte[]> current = rows.get(i);

            assertTrueWithPairs(comparator.compare(previous.getFirst(), current.getFirst()) >= 0, previous, current);
        }
    }

    @Override
    protected byte[] getValue(Pair<IndexRowKey, QueryKey> pair) {
        return pair.getSecond().getKeys().get(COLUMN).array();
    }
}
