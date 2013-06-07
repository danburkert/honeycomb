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


package integrationtests.table;

import integrationtests.HoneycombIntegrationTest;
import integrationtests.TestConstants;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class TableOperationsIT extends HoneycombIntegrationTest {

    @Test
    public void testRenameTable() {
        final String newTableName = TestConstants.TABLE_NAME + "-renamed";

        proxy.renameTable(TestConstants.TABLE_NAME, newTableName);
        assertThat(newTableName, equalTo(proxy.getTableName()));

        // Restore the original table name to allow for proper testcase teardown
        proxy.renameTable(newTableName, TestConstants.TABLE_NAME);
    }
}
