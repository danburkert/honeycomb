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

import com.google.common.primitives.UnsignedBytes;
import com.nearinfinity.honeycomb.hbase.generators.RowKeyGenerator;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKey;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import net.java.quickcheck.collection.Pair;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertTrue;

public abstract class EncodingTest<T> {
    protected static final String COLUMN = "c1";

    protected void addPairs(List<Pair<T, byte[]>> rows, RowKeyGenerator.IndexRowKeyGenerator rowKeyGen) {
        Pair<IndexRowKey, QueryKey> pair;
        for (int i = 0; i < 200; i++) {
            pair = rowKeyGen.nextWithQueryKey();
            if (pair.getSecond().getKeys().get(COLUMN) != null) {
                rows.add(new Pair<T, byte[]>(
                        getValue(pair),
                        pair.getFirst().encode()));
            } else {
                i--;
            }
        }
    }

    protected abstract T getValue(Pair<IndexRowKey, QueryKey> pair);

    protected void assertTrueWithPairs(boolean condition, Pair<T, byte[]> previous, Pair<T, byte[]> current) {
        assertTrue(previous.getFirst() + " / " +
                Bytes.toStringBinary(previous.getSecond()) + " : " +
                current.getFirst() + " / " +
                Bytes.toStringBinary(current.getSecond()), condition);
    }

    protected class RowComparator implements Comparator<Pair<T, byte[]>> {
        private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

        @Override
        public int compare(Pair<T, byte[]> o1, Pair<T, byte[]> o2) {
            return comparator.compare(o1.getSecond(), o2.getSecond());
        }
    }
}
