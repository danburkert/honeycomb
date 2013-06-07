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


package integrationtests.rowcount;

import com.nearinfinity.honeycomb.mysql.HandlerProxy;
import integrationtests.HoneycombIntegrationTest;
import integrationtests.ITUtils;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RowCountOperationsIT extends HoneycombIntegrationTest {

    @Test
    public void testGetRowCount() {
        proxy.incrementRowCount(2);

        assertThat(proxy.getRowCount(), equalTo(2L));
    }

    @Test
    public void testTruncateRowCount() {
        proxy.incrementRowCount(5);
        proxy.truncateRowCount();

        assertThat(proxy.getRowCount(), equalTo(0L));
    }

    @Test
    public void testIncrementRowCountConcurrently() throws Exception{
        final long amount = 13;
        final int concurrency = 8;
        final long expectedRowCount = amount * concurrency;
        ITUtils.startProxyActionConcurrently(
                concurrency,
                ITUtils.openTable,
                new IncrementRowCount(amount),
                ITUtils.closeTable,
                factory);
        assertEquals(expectedRowCount, proxy.getRowCount());
    }

    private class IncrementRowCount implements ITUtils.ProxyRunnable {
        private long amount;
        public IncrementRowCount(long amount) {
            this.amount = amount;
        }
        @Override
        public void run(HandlerProxy proxy) {
            proxy.incrementRowCount(amount);
        }
    }
}