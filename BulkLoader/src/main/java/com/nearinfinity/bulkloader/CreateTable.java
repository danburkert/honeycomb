package com.nearinfinity.bulkloader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class CreateTable extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateTable(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] strings) throws Exception {
        String tableName = strings[0];
        Map<String, String> params = readConfigOptions();
        Configuration conf = HBaseConfiguration.create();
        conf.setIfUnset("hb_family", params.get("hbase_family"));
        conf.setIfUnset("zk_quorum", params.get("zk_quorum"));
        if ("localhost".equalsIgnoreCase(conf.get("hbase.zookeeper.quorum"))) {
            conf.set("hbase.zookeeper.quorum", params.get("zk_quorum"));
        } else {
            conf.setIfUnset("hbase.zookeeper.quorum", params.get("zk_quorum"));
        }
        HBaseAdmin admin = new HBaseAdmin(conf);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("nic");
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        descriptor.addFamily(columnDescriptor);
        byte[][] splits = createSplits();
        admin.createTable(descriptor, splits);
        admin.flush(tableName);
        admin.close();

        return 0;
    }

    public static byte[][] createSplits() {
        final int columns = 11;
        final int offset = 2;
        byte[][] splits = new byte[(2 * columns) + offset][];
        splits[0] = ByteBuffer.allocate(9).put((byte) 4).putLong(1L).array();
        splits[1] = ByteBuffer.allocate(9).put((byte) 4).putLong(2L).array();
        for (int i = offset, columnId = 1; i < splits.length; i += 2, columnId++) {
            splits[i] = ByteBuffer.allocate(17).put((byte) 5).putLong(1L).putLong(columnId).array();
            splits[i + 1] = ByteBuffer.allocate(17).put((byte) 6).putLong(1L).putLong(columnId).array();
        }
        return splits;
    }

    private static Map<String, String> readConfigOptions() throws FileNotFoundException {
        //Read config options from adapter.conf
        Scanner confFile = new Scanner(new File("/etc/mysql/adapter.conf"));
        Map<String, String> params = new TreeMap<String, String>();
        while (confFile.hasNextLine()) {
            Scanner line = new Scanner(confFile.nextLine());
            params.put(line.next(), line.next());
        }
        return params;
    }
}
