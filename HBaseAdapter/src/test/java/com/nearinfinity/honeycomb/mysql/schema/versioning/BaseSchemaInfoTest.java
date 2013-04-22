package com.nearinfinity.honeycomb.mysql.schema.versioning;

import org.junit.Test;
import org.mockito.Mockito;

public class BaseSchemaInfoTest {

    @Test(expected = IllegalArgumentException.class)
    public void testFindSchemaInvalidSchemaVersion() {
        final BaseSchemaInfo baseInfo = Mockito.mock(BaseSchemaInfo.class, Mockito.CALLS_REAL_METHODS);
        baseInfo.findSchema(0);
    }
}
