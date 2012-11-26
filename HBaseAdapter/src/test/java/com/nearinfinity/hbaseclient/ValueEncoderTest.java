package com.nearinfinity.hbaseclient;

import com.nearinfinity.honeycomb.hbaseclient.ColumnMetadata;
import com.nearinfinity.honeycomb.hbaseclient.ColumnType;
import com.nearinfinity.honeycomb.hbaseclient.TableInfo;
import com.nearinfinity.honeycomb.hbaseclient.ValueEncoder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ValueEncoderTest {
    @Test
    public void negativeDoublesAreCorrectlyEncoded() {
        double value = -1.2;
        byte[] byteValue = Bytes.toBytes(value);
        long numeric = Double.doubleToLongBits(value);
        byte[] expected = Bytes.toBytes(~numeric);
        TableInfo info = new TableInfo("test", 0);
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setMaxLength(8);
        metadata.setType(ColumnType.DOUBLE);
        info.addColumn("x", 1, metadata);
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        map.put("x", byteValue);
        Map<String, byte[]> actual = ValueEncoder.correctAscendingValuePadding(info, map);
        byte[] result = actual.get("x");
        Assert.assertArrayEquals(expected, result);
    }
}
