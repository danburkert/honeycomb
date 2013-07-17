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


package integrationtests;

import com.nearinfinity.honeycomb.mysql.Bootstrap;
import com.nearinfinity.honeycomb.mysql.HandlerProxy;
import com.nearinfinity.honeycomb.mysql.HandlerProxyFactory;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Test class used for running integration tests. Ensures setup and teardown
 * of the application framework between test classes in the test suite.
 */
public abstract class HoneycombIntegrationTest {
    @Rule
    public final TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            System.out.println(String.format("Executing: %s", description.getMethodName()));
        }
    };

    protected static HandlerProxyFactory factory;
    protected HandlerProxy proxy;

    @BeforeClass
    public static void setupFramework() {
        factory = Bootstrap.startup();
        System.out.println("Initialized application framework");
    }

    @Before
    public void setupTestCase() {
        proxy = factory.createHandlerProxy();

        proxy.createTable(TestConstants.TABLE_NAME,
                getTableSchema().serialize(), 1);

        proxy.openTable(TestConstants.TABLE_NAME);
    }

    @After
    public void teardownTestCase() {
        proxy.closeTable();

        proxy.dropTable(TestConstants.TABLE_NAME);
    }

    /**
     * Provides the {@link TableSchema} to use for a test case
     *
     * @return The schema used for testing
     */
    protected TableSchema getTableSchema() {
        return TestConstants.TABLE_SCHEMA;
    }
}
