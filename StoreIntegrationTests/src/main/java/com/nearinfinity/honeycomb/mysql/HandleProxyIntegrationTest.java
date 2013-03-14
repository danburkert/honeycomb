package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

import static org.fest.assertions.Assertions.assertThat;

public class HandleProxyIntegrationTest {
    private static HandlerProxyFactory factory;
    private static final String tableName = "db/test";

    public static void suiteSetup() throws IOException, SAXException, ParserConfigurationException {
        factory = Bootstrap.startup();
    }

    public static void testSuccessfulRename() throws Exception {
        System.out.println("Testing rename");
        final String newTableName = "db2/test2";
        HandlerProxy proxy = factory.createHandlerProxy();
        TableSchema schema = getTableSchema();

        proxy.createTable(tableName, Constants.HBASE_TABLESPACE, Util.serializeTableSchema(schema), 0);
        proxy.openTable(tableName, Constants.HBASE_TABLESPACE);
        proxy.renameTable(newTableName);
        assertThat(newTableName).isEqualTo(proxy.getTableName());
        proxy.closeTable();
        proxy.dropTable(newTableName, Constants.HBASE_TABLESPACE);
    }

    public static void testSuccessfulAlter() throws Exception {
        System.out.println("Testing alter");
        HandlerProxy proxy = factory.createHandlerProxy();
        TableSchema schema = getTableSchema();

        proxy.createTable(tableName, Constants.HBASE_TABLESPACE, Util.serializeTableSchema(schema), 0);
        proxy.openTable(tableName, Constants.HBASE_TABLESPACE);
        schema.getColumns().put("c3", new ColumnSchema(ColumnType.LONG, false, false, 8, 0, 0));
        proxy.alterTable(Util.serializeTableSchema(schema));
        proxy.closeTable();
        proxy.dropTable(tableName, Constants.HBASE_TABLESPACE);
    }

    public static void testGetAutoIncrement() throws Exception {
        System.out.println("Testing auto increment");
        HandlerProxy proxy = factory.createHandlerProxy();
        TableSchema schema = getTableSchema();
        schema.getColumns().put("c1", new ColumnSchema(ColumnType.LONG, true, true, 8, 0, 0));
        proxy.createTable(tableName, Constants.HBASE_TABLESPACE, Util.serializeTableSchema(schema), 1);
        proxy.openTable(tableName, Constants.HBASE_TABLESPACE);
        long autoIncValue = proxy.getAutoIncValue();
        assertThat(autoIncValue).isEqualTo(1);
        proxy.closeTable();
        proxy.dropTable(tableName, Constants.HBASE_TABLESPACE);
    }

    public static void testIncrementAutoIncrement() throws Exception {
        System.out.println("Testing increment auto increment");
        HandlerProxy proxy = factory.createHandlerProxy();
        TableSchema schema = getTableSchema();
        schema.getColumns().put("c1", new ColumnSchema(ColumnType.LONG, true, true, 8, 0, 0));
        proxy.createTable(tableName, Constants.HBASE_TABLESPACE, Util.serializeTableSchema(schema), 1);
        proxy.openTable(tableName, Constants.HBASE_TABLESPACE);
        long autoIncValue = proxy.incrementAutoIncrementValue(1);
        assertThat(autoIncValue).isEqualTo(2).isEqualTo(proxy.getAutoIncValue());
        proxy.closeTable();
        proxy.dropTable(tableName, Constants.HBASE_TABLESPACE);
    }

    private static TableSchema getTableSchema() {
        HashMap<String, ColumnSchema> columns = new HashMap<String, ColumnSchema>();
        HashMap<String, IndexSchema> indices = new HashMap<String, IndexSchema>();
        columns.put("c1", new ColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));
        columns.put("c2", new ColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));
        indices.put("i1", new IndexSchema(Lists.newArrayList("c1"), false));

        return new TableSchema(columns, indices);
    }

    public static void main(String[] args) throws Exception {
        try {
            suiteSetup();
            testSuccessfulRename();
            testSuccessfulAlter();
            testGetAutoIncrement();
            testIncrementAutoIncrement();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
