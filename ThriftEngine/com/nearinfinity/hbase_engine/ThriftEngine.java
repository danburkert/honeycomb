package com.nearinfinity.hbase_engine;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: acrute
 * Date: 7/28/12
 * Time: 12:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class ThriftEngine extends Engine implements Engine.Iface {
    private HBaseClient client;

    public ThriftEngine(String serverName, String host) throws ZooKeeperConnectionException, MasterNotRunningException {
        this.client = new HBaseClient(serverName, host);
    }

    public void open() throws TException {
        System.out.println("Connection established!");
    }

    @Override
    public boolean createTable(String tableName, List<String> columnFamilies) throws TException {
        return this.client.createTable(tableName, columnFamilies);
    }

    @Override
    public boolean addColumnFamily(String tableName, String familyName) throws TException {
        return this.client.addColumnFamily(tableName, familyName);
    }

    @Override
    public boolean removeColumnFamily(String tableName, String familyName) throws TException {
        return this.client.removeColumnFamily(tableName, familyName);
    }

    @Override
    public void addData(String tableName, Map<String, Map<String, Map<String, String>>> data) throws TException {
        this.client.addData(tableName, data);
    }

    @Override
    public boolean removeTable(String tableName) throws TException {
        return this.client.removeTable(tableName);
    }

    public boolean create(String name, List<String> family) throws TException {
        System.out.println("Creating table " + name + " with column family " + family + "...");

        return this.client.createTable(name, family);
    }
}
