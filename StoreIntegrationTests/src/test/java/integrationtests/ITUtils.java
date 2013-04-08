package integrationtests;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.mysql.*;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

/**
 * Integration Test utility methods
 */
public class ITUtils {
    /**
     * Generates a new {@link com.nearinfinity.honeycomb.mysql.schema.TableSchema} with columns and indexes
     *
     * @return The created schema
     */
    public static TableSchema getTableSchema() {
        final HashMap<String, ColumnSchema> columns = Maps.newHashMap();
        final HashMap<String, IndexSchema> indices = Maps.newHashMap();

        // Add nullable, non-autoincrementing columns
        columns.put(TestConstants.COLUMN1, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));
        columns.put(TestConstants.COLUMN2, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        // Add non-unique index on one column
        indices.put(TestConstants.INDEX1, IndexSchemaFactory.createIndexSchema(Lists.newArrayList(TestConstants.COLUMN1), false, TestConstants.INDEX1));

        // Add non-unique compound index on (c1, c2)
        indices.put(TestConstants.INDEX2, IndexSchemaFactory.createIndexSchema(Lists.newArrayList(TestConstants.COLUMN1, TestConstants.COLUMN2), false, TestConstants.INDEX1));

        return TableSchemaFactory.createTableSchema(columns, indices);
    }

    /**
     * Encodes the provided value as a {@link ByteBuffer}
     *
     * @param value The value to encode
     * @return The buffer representing the provided value
     */
    public static ByteBuffer encodeValue(final long value) {
        return (ByteBuffer) ByteBuffer.allocate(8).putLong(value).rewind();
    }

    /**
     * Asserts that the rows returned from a table scan performed by the proxy are different
     *
     * @param proxy    The proxy to use for invoking a scan, not null
     * @param rowCount The valid number of rows expected to be returned from the scan
     */
    public static void assertReceivingDifferentRows(final HandlerProxy proxy, final int rowCount) {
        checkNotNull(proxy);
        verifyRowCount(rowCount);

        proxy.startTableScan();
        assertDifferentRows(proxy, rowCount);
        proxy.endScan();
    }

    /**
     * Asserts that the rows returned from an index scan performed by the proxy are different
     *
     * @param proxy    The proxy to use for invoking a scan, not null
     * @param key      The index used to run an index scan, not null
     * @param rowCount The valid number of rows expected to be returned from the scan
     */
    public static void assertReceivingDifferentRows(final HandlerProxy proxy, final QueryKey key, final int rowCount) {
        checkNotNull(proxy);
        checkNotNull(key);
        verifyRowCount(rowCount);

        proxy.startIndexScan(key.serialize());
        assertDifferentRows(proxy, rowCount);
        proxy.endScan();
    }

    private static void assertDifferentRows(final HandlerProxy proxy, final int rowCount) {
        byte[] previous = null;

        for (int x = 0; x < rowCount; x++) {
            final byte[] current = proxy.getNextRow();
            assertNotNull(current);
            assertThat(current, not(equalTo(previous)));
            previous = current;
        }

        assertNull(proxy.getNextRow());
    }

    /**
     * Inserts the number of specified rows with a random {@link UUID}
     *
     * @param proxy
     * @param rowCount
     * @param keyColumnValue
     * @see #insertData(HandlerProxy, int, long, UUID)
     */
    public static void insertData(final HandlerProxy proxy, final int rowCount, final long keyColumnValue) {
        insertData(proxy, rowCount, keyColumnValue, UUID.randomUUID());
    }

    /**
     * Inserts the number of specified rows with each row containing a column,
     * {@link TestConstants#COLUMN1}, with the value provided and another column,
     * {@link TestConstants#COLUMN2}, with an arbitrary value
     *
     * @param proxy          The proxy to use for invoking a scan, not null
     * @param rowCount       The valid number of rows expected to be returned from the scan
     * @param keyColumnValue The value stored in the first column, which is intended to be indexed
     * @param uuid           The unique identifier to associate with each row inserted, not null
     */
    public static void insertData(final HandlerProxy proxy, final int rowCount,
                                  final long keyColumnValue, final UUID uuid) {
        checkNotNull(proxy);
        verifyRowCount(rowCount);
        checkNotNull(uuid);

        final Map<String, ByteBuffer> map = Maps.newHashMap();
        map.put(TestConstants.COLUMN1, encodeValue(keyColumnValue));

        for (int x = 0; x < rowCount; x++) {
            map.put(TestConstants.COLUMN2, encodeValue(x));
            final Row row = new Row(map, uuid);
            proxy.insertRow(row.serialize());
        }

        proxy.flush();
    }

    /**
     * Inserts the number of specified rows with each row containing the column,
     * {@link TestConstants#COLUMN2}, with an arbitrary value and random {@link UUID}.
     *
     * @param proxy
     * @param rows
     * @see #insertNullData(HandlerProxy, int, String)
     */
    public static void insertNullData(HandlerProxy proxy, int rows) {
        insertNullData(proxy, rows, TestConstants.COLUMN2);
    }

    /**
     * Inserts the number of specified rows with each row containing the provided
     * column name with an arbitrary value and random {@link UUID}.  The rows created
     * do not use columns used for indexing such as {@link TestConstants#COLUMN1} and
     * {@link TestConstants#COLUMN2}
     *
     * @param proxy      The proxy to use for invoking a scan, not null
     * @param rowCount   The valid number of rows expected to be returned from the scan
     * @param columnName The column name to associated with the inserted row, not null or empty
     */
    public static void insertNullData(final HandlerProxy proxy, final int rowCount, final String columnName) {
        checkNotNull(proxy);
        verifyRowCount(rowCount);
        Verify.isNotNullOrEmpty(columnName);

        final Map<String, ByteBuffer> map = Maps.newHashMap();

        for (int x = 0; x < rowCount; x++) {
            map.put(columnName, encodeValue(x));
            final Row row = new Row(map, UUID.randomUUID());
            proxy.insertRow(row.serialize());
        }

        proxy.flush();
    }

    /**
     * Creates an index on {@link TestConstants#COLUMN1} for the provided value
     * with the index name of {@link TestConstants#INDEX1}
     *
     * @param keyValue  The value stored in the indexed column
     * @param queryType The query type that this index will be used for
     * @return The constructed index with the provided details
     */
    public static QueryKey createKey(final int keyValue, final QueryType queryType) {
        HashMap<String, ByteBuffer> keys = Maps.newHashMap();
        keys.put(TestConstants.COLUMN1, encodeValue(keyValue));
        return new QueryKey(TestConstants.INDEX1, queryType, keys);
    }

    private static void verifyRowCount(final long rowCount) {
        checkArgument(rowCount >= 0, "The provided row count is invalid");
    }
}
