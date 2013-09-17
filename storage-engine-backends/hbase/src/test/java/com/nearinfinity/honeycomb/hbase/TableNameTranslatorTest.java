package com.nearinfinity.honeycomb.hbase;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TableNameTranslatorTest {
    @Test
    public void testTableId() throws Exception {
        assertEquals("database/table", TableNameTranslator.tableId("database/table"));

    }
}
