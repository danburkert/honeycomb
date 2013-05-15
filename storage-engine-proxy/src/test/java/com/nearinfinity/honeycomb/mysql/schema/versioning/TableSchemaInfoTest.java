package com.nearinfinity.honeycomb.mysql.schema.versioning;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

public class TableSchemaInfoTest {
    private TableSchemaInfo tableSchemaInfo;

    @Before
    public void setupTestCases() {
        tableSchemaInfo = new TableSchemaInfo();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testFindSchemaNegativeSchemaVersion() {
        tableSchemaInfo.findSchema(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindSchemaExceedAvailableVersions() {
        tableSchemaInfo.findSchema(Integer.MAX_VALUE);
    }

    @Test
    public void testFindSchemaValidSchemaVersion() {
        assertNotNull(tableSchemaInfo.findSchema(0));
    }
}
