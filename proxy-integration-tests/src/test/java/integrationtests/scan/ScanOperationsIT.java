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


package integrationtests.scan;

import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import integrationtests.HoneycombIntegrationTest;
import integrationtests.ITUtils;
import integrationtests.TestConstants;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;

public class ScanOperationsIT extends HoneycombIntegrationTest {

    private static final int ROW_COUNT = 3;
    private static final int INDEX_COL_VALUE = 5;

    @Test
    public void testIndexExactScan() {
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE);

        final QueryKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.EXACT_KEY);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT);
    }

    @Test
    public void testAfterKeyScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, TestConstants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE + 1, TestConstants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE + 1);

        final QueryKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.AFTER_KEY);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 1);
    }

    @Test
    public void testBeforeKeyScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, TestConstants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE - 1, TestConstants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE - 1);

        final QueryKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.BEFORE_KEY);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 1);
    }

    @Test
    public void testKeyOrNextScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, TestConstants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE + 1, TestConstants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE + 1);

        final QueryKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.KEY_OR_NEXT);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 2);
    }

    @Test
    public void testKeyOrPreviousScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, TestConstants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE - 1, TestConstants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE - 1);

        final QueryKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.KEY_OR_PREVIOUS);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 2);
    }

    @Test
    public void testIndexLastScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, TestConstants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE - 1, TestConstants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE - 1);

        final QueryKey key = new QueryKey(TestConstants.INDEX1, QueryType.INDEX_LAST, Maps.<String, ByteBuffer>newHashMap());
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 2);
    }

    @Test
    public void testIndexFirstScan() {
        ITUtils.insertNullData(proxy, 2);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, TestConstants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE + 1, TestConstants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE + 1);

        final QueryKey key = new QueryKey(TestConstants.INDEX1, QueryType.INDEX_FIRST, Maps.<String, ByteBuffer>newHashMap());
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 4);
    }

    @Test
    public void testAfterKeyWithNullScan() {
        ITUtils.insertNullData(proxy, ROW_COUNT, TestConstants.COLUMN1);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE + 1);

        final Map<String, ByteBuffer> keyValues = Maps.newHashMap();
        keyValues.put(TestConstants.COLUMN1, ITUtils.encodeValue(2));

        final QueryKey key = new QueryKey(TestConstants.INDEX2, QueryType.AFTER_KEY, keyValues);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + ROW_COUNT);
    }

    @Test
    public void testFullTableScan() {
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE);
        ITUtils.assertReceivingDifferentRows(proxy, ROW_COUNT);
    }

    @Test
    public void testCloseNonExistentScanNoException() {
        proxy.endScan();
        proxy.endScan();
    }
}
