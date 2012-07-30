package com.nearinfinity.hbase_engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseClient {
    private HBaseAdmin hbase;

    public HBaseClient(String serverName, String location) throws ZooKeeperConnectionException, MasterNotRunningException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(serverName, location);

        this.hbase = new HBaseAdmin(conf);
    }

    public boolean createTable(String tableName, List<String> columnFamilies) {
         HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

        for (String familyName : columnFamilies)
        {
            HColumnDescriptor family = new HColumnDescriptor(familyName.getBytes());
            tableDescriptor.addFamily(family);
        }

        try {
            this.hbase.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public boolean addColumnFamily(String tableName, String familyName)
    {
        HTableDescriptor tableDescriptor = null;
        try {
            tableDescriptor = this.hbase.getTableDescriptor(tableName.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        if (tableDescriptor.hasFamily(familyName.getBytes()))
        {
            return false;
        }

        try {
            this.hbase.disableTable(tableName);
            HColumnDescriptor family = new HColumnDescriptor(familyName.getBytes());
            tableDescriptor.addFamily(family);
            this.hbase.enableTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return tableDescriptor.hasFamily(familyName.getBytes());
    }

    public boolean removeColumnFamily(String tableName, String familyName)
    {
        HTableDescriptor tableDescriptor = null;
        try {
            tableDescriptor = this.hbase.getTableDescriptor(tableName.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        if (!tableDescriptor.hasFamily(familyName.getBytes()))
        {
            return false;
        }

        this.disableTable(tableName);
        tableDescriptor.removeFamily(familyName.getBytes());
        this.enableTable(tableName);

        return true;
    }

    public boolean enableTable(String tableName)
    {
        try {
            if (this.hbase.isTableDisabled(tableName))
            {
                this.hbase.enableTable(tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public boolean disableTable(String tableName)
    {
        try {
            if (this.hbase.isTableEnabled(tableName))
            {
                this.hbase.disableTable(tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public boolean removeTable(String tableName)
    {
        try {
            this.disableTable(tableName);
            this.hbase.deleteTable(tableName);
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public void addData(String tableName, Map<String, Map<String, Map<String, String>>> data)
    {
        HTable table;
        try {
            table = new HTable(this.hbase.getConfiguration(), tableName.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        for (String row : data.keySet())
        {
            byte[] rowKey = row.getBytes();
            Put put = new Put(rowKey);
            Map<String, Map<String, String>> columnFamilyData = data.get(row);

            for (String familyKey : columnFamilyData.keySet())
            {
                for (Map.Entry<String, String> entry : columnFamilyData.get(familyKey).entrySet())
                {
                    byte[] familyBytes = familyKey.getBytes();
                    byte[] key = entry.getKey().getBytes();
                    byte[] value = entry.getValue().getBytes();

                    put.add(familyBytes, key, value);
                }
            }

            try {
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
