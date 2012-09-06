package com.nearinfinity.hbaseclient;

import org.apache.hadoop.hbase.client.Result;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/20/12
 * Time: 3:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class ResultParser {
    public static UUID parseUUID(Result result) {
        byte[] rowKey = result.getRow();
        ByteBuffer byteBuffer = ByteBuffer.wrap(rowKey, rowKey.length - 16, 16);
        return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
    }

    public static byte[] parseUnireg(Result result) {
        return result.getValue(Constants.NIC, Constants.VALUE_MAP);
    }

    public static byte[] parseValue(Result result) {
        return result.getValue(Constants.NIC, Constants.VALUE_MAP);
    }

    public static TreeMap<String, byte[]> parseRowMap(Result result) {
        byte[] mapBytes = parseUnireg(result);
        TreeMap<String, byte[]> rowMap = null;

        try {
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(mapBytes));
            rowMap = (TreeMap<String, byte[]>) in.readObject();
            in.close();
        } catch (IOException e) {
        } catch (ClassNotFoundException e) {
        }

        return rowMap;
    }
}
