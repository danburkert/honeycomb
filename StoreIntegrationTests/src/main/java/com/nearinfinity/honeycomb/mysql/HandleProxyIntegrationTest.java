package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.RowNotFoundException;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.fest.assertions.Assertions.assertThat;

public class HandleProxyIntegrationTest {
    public static final String COLUMN1 = "c1";
    public static final String COLUMN2 = "c2";
    public static final String INDEX2 = "i2";
    public static final String INDEX1 = "i1";
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
                proxy.renameTable(tableName, Constants.HBASE_TABLESPACE, newTableName);
                assertThat(newTableName).isEqualTo(proxy.getTableName());
                proxy.renameTable(newTableName, Constants.HBASE_TABLESPACE, tableName);
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
                proxy.insert(row.serialize());
                proxy.flush();
                Row result = proxy.getRow(uuid);
                assertThat(result).isEqualTo(row);
            }
        });
    }

    public static void testDeleteRow() throws Exception {
        testProxy("Testing delete row", new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put(COLUMN1, ByteBuffer.allocate(8).putLong(5).rewind());
                map.put(COLUMN2, ByteBuffer.allocate(8).putLong(6).rewind());
                UUID uuid = UUID.randomUUID();
                Row row = new Row(map, uuid);
                proxy.insert(row.serialize());
                proxy.flush();
                proxy.deleteRow(uuid);
                proxy.flush();
                try {
                    proxy.getRow(uuid);
                } catch (RowNotFoundException e) {
                    return;
                }

                throw new AssertionError("Row was not deleted");
            }
        });
    }

    public static void testUpdateRow() throws Exception {
        testProxy("Testing update row", new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                UUID uuid = UUID.randomUUID();
                Map<String, Object> map = new HashMap<String, Object>();
                map.put(COLUMN1, ByteBuffer.allocate(8).putLong(5).rewind());
                map.put(COLUMN2, ByteBuffer.allocate(8).putLong(6).rewind());
                Row row = new Row(map, uuid);
                proxy.insert(row.serialize());
                proxy.flush();

                map.put(COLUMN1, ByteBuffer.allocate(8).putLong(3).rewind());
                Row newRow = new Row(map, uuid);
                proxy.updateRow(newRow.serialize());
                proxy.flush();
                Row result = proxy.getRow(uuid);
                assertThat(result).isEqualTo(newRow);
            }
        });
    }

    public static void testIndexExactScan() throws Exception {
        testProxy("Testing index exact scan", new Action() {
            @Override
            public void execute(HandlerProxy proxy) throws Exception {
                Map<String, Object> map = new HashMap<String, Object>();
                long keyColumnValue = 5;
                int rows = 1;
                map.put(COLUMN1, encodeValue(keyColumnValue));
                for (int x = 0; x < rows; x++) {
                    map.put(COLUMN2, encodeValue(x));
                    Row row = new Row(map, Constants.FULL_UUID);
                    proxy.insert(row.serialize());
                }
                map.clear();
                map.put(COLUMN1, encodeValue(keyColumnValue + 1));
                map.put(COLUMN2, encodeValue(0));
                proxy.insert(new Row(map, UUID.randomUUID()).serialize());
                proxy.flush();

                IndexKey key = new IndexKey(INDEX1, new HashMap<String, ByteBuffer>() {
                    {
                        put(COLUMN1, (ByteBuffer) encodeValue(5));
                    }
                });
                proxy.startIndexScan(key.serialize());
                Row previous = null;
                for (int x = 0; x < rows; x++) {
                    Row current = proxy.getNextScannerRow();
                    assertThat(current).isNotEqualTo(previous);
                    previous = current;
                }

                Row end = proxy.getNextScannerRow();
                assertThat(end).isNull();
            }
        });
    }

    private static Buffer encodeValue(long value) {
        return ByteBuffer.allocate(8).putLong(value).rewind();
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
        indices.put(INDEX1, new IndexSchema(Lists.newArrayList(COLUMN1), false));
        indices.put(INDEX2, new IndexSchema(Lists.newArrayList(COLUMN1, COLUMN2), false));

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
            testDeleteRow();
            testUpdateRow();
            testIndexExactScan();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private interface Action {
        public void execute(HandlerProxy proxy) throws Exception;
    }
}
