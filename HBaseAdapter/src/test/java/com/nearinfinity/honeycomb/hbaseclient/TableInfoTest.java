package com.nearinfinity.honeycomb.hbaseclient;

import com.nearinfinity.honeycomb.hbaseclient.ColumnMetadata;
import com.nearinfinity.honeycomb.hbaseclient.ColumnType;
import com.nearinfinity.honeycomb.hbaseclient.TableInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TableInfoTest {
    @Test
    public void testReadWrite() throws Exception {
        TableInfo info = new TableInfo("Test", 2);
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setMaxLength(2);
        metadata.setType(ColumnType.STRING);
        info.addColumn("x", 3, metadata);
        Map<String, byte[]> bytes = new HashMap<String, byte[]>();
        bytes.put("bob", new byte[]{91, 91, 91, 91, 91});
        info.setTableMetadata(bytes);
        String json = info.write();
        TableInfo result = new TableInfo("KJ", 4);
        result.read(json);
        Assert.assertNotNull(result);
    }
}
