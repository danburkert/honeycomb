package com.nearinfinity.honeycomb.mysql.schema.versioning;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

public class RowSchemaInfoTest {
    private RowSchemaInfo rowSchemaInfo;

    @Before
    public void setupTestCases() {
        rowSchemaInfo = new RowSchemaInfo();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testFindSchemaNegativeSchemaVersion() {
        rowSchemaInfo.findSchema(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindSchemaExceedAvailableVersions() {
        rowSchemaInfo.findSchema(Integer.MAX_VALUE);
    }

    @Test
    public void testFindSchemaValidSchemaVersion() {
        assertNotNull(rowSchemaInfo.findSchema(0));
    }
}
