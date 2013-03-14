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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.fest.assertions.Assertions.assertThat;

public class HandleProxyIntegrationTest {
    public static final String COLUMN1 = "c1";
    public static final String COLUMN2 = "c2";
    private static final String tableName = "db/test";
    private static HandlerProxyFactory factory;

    public static void suiteSetup() throws IOException, SAXException, ParserConfigurationException {
        factory = Bootstrap.startup();
    }

    public static void testSuccessfulRename() throws Exception {
        final String newTableName = "db2/test2";
        TableSchema schema = getTableSchema();

        testProxy("Testing rename", schema, new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                proxy.renameTable(newTableName);
                assertThat(newTableName).isEqualTo(proxy.getTableName());
                proxy.renameTable(tableName);
            }
        });
    }

    public static void testSuccessfulAlter() throws Exception {
        final TableSchema schema = getTableSchema();
        testProxy("Testing alter", schema, new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                schema.getColumns().put("c3", new ColumnSchema(ColumnType.LONG, false, false, 8, 0, 0));
                proxy.alterTable(Util.serializeTableSchema(schema));
            }
        });
    }

    public static void testGetAutoIncrement() throws Exception {
        TableSchema schema = getTableSchema();
        schema.getColumns().put("c1", new ColumnSchema(ColumnType.LONG, true, true, 8, 0, 0));
        testProxy("Testing auto increment", schema, new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                long autoIncValue = proxy.getAutoIncValue();
                assertThat(autoIncValue).isEqualTo(1);

            }
        });
    }

    public static void testIncrementAutoIncrement() throws Exception {
        TableSchema schema = getTableSchema();
        schema.getColumns().put("c1", new ColumnSchema(ColumnType.LONG, true, true, 8, 0, 0));
        testProxy("Testing increment auto increment", schema, new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                long autoIncValue = proxy.incrementAutoIncrementValue(1);
                assertThat(autoIncValue).isEqualTo(2).isEqualTo(proxy.getAutoIncValue());
            }
        });
    }

    public static void testTruncateAutoInc() throws Exception {
        TableSchema schema = getTableSchema();
        schema.getColumns().put("c1", new ColumnSchema(ColumnType.LONG, true, true, 8, 0, 0));
        testProxy("Testing truncate auto increment", schema, new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                proxy.truncateAutoIncrement();
                assertThat(proxy.getAutoIncValue()).isEqualTo(0);
            }
        });
    }

    public static void testGetRowCount() throws Exception {
        testProxy("Testing get row count", new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                proxy.incrementRowCount(2);
                assertThat(proxy.getRowCount()).isEqualTo(2);
            }
        });
    }

    public static void testTruncateRowCount() throws Exception {
        testProxy("Testing truncate row count", new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                proxy.incrementRowCount(5);
                proxy.truncateRowCount();
                assertThat(proxy.getRowCount()).isEqualTo(0);
            }
        });
    }

    public static void testInsertRow() throws Exception {
        testProxy("Testing insert row", new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put(COLUMN1, ByteBuffer.allocate(8).putLong(5).rewind());
                map.put(COLUMN2, ByteBuffer.allocate(8).putLong(6).rewind());
                UUID uuid = UUID.randomUUID();
                Row row = new Row(map, uuid);
                proxy.insert(row);
                proxy.flush();
                Row result = proxy.getRow(uuid);
                assertThat(result).isEqualTo(row);
            }
        });
    }

    private static void testProxy(String message, Action test) throws Exception {
        testProxy(message, getTableSchema(), test);
    }

    private static void testProxy(String message, TableSchema schema, Action test) throws Exception {
        System.out.println(message);
        HandlerProxy proxy = factory.createHandlerProxy();
        proxy.createTable(tableName, Constants.HBASE_TABLESPACE, Util.serializeTableSchema(schema), 1);
        proxy.openTable(tableName, Constants.HBASE_TABLESPACE);
        test.execute(proxy);
        proxy.closeTable();
        proxy.dropTable(tableName, Constants.HBASE_TABLESPACE);
    }

    private static TableSchema getTableSchema() {
        HashMap<String, ColumnSchema> columns = new HashMap<String, ColumnSchema>();
        HashMap<String, IndexSchema> indices = new HashMap<String, IndexSchema>();
        columns.put(COLUMN1, new ColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));
        columns.put(COLUMN2, new ColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));
        indices.put("i1", new IndexSchema(Lists.newArrayList("c1"), false));
        indices.put("i2", new IndexSchema(Lists.newArrayList("c1", "c2"), false));

        return new TableSchema(columns, indices);
    }

    public static void main(String[] args) throws Exception {
        try {
            suiteSetup();
            testSuccessfulRename();
            testSuccessfulAlter();
            testGetAutoIncrement();
            testIncrementAutoIncrement();
            testTruncateAutoInc();
            testGetRowCount();
            testTruncateRowCount();
            testInsertRow();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private interface Action {
        public void execute(HandlerProxy proxy) throws Exception;
    }
}
