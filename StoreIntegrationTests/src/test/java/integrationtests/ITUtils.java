package integrationtests;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.nearinfinity.honeycomb.mysql.HandlerProxy;
import com.nearinfinity.honeycomb.mysql.HandlerProxyFactory;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;

/**
 * Integration Test utility methods
 */
public class ITUtils {
    /**
     * Encodes the provided value as a {@link ByteBuffer}
     *
     * @param value The value to encode
     * @return The buffer representing the provided value
     */
    public static ByteBuffer encodeValue(final long value) {
        return ByteBuffer.wrap(Longs.toByteArray(value));
    }

    /**
     * Asserts that the rows returned from a table scan performed by the proxy are different
     *
     * @param proxy    The proxy to use for invoking a scan, not null
     * @param rowCount The valid number of rows expected to be returned from the scan
     */
    public static void assertReceivingDifferentRows(final HandlerProxy proxy,
                                                    final int rowCount) {
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
    public static void assertReceivingDifferentRows(final HandlerProxy proxy,
                                                    final QueryKey key,
                                                    final int rowCount) {
        checkNotNull(proxy);
        checkNotNull(key);
        verifyRowCount(rowCount);

        proxy.startIndexScan(key.serialize());
        assertDifferentRows(proxy, rowCount);
        proxy.endScan();
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

    public static Row createRow(final int columnValue) {
        final Map<String, ByteBuffer> map = Maps.newHashMap();
        map.put(TestConstants.COLUMN1, encodeValue(columnValue));
        return new Row(map, UUID.randomUUID());
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

    private static void verifyRowCount(final long rowCount) {
        checkArgument(rowCount >= 0, "The provided row count is invalid");
    }

    /**
     * Check that the table open on the proxy has the expected number of data rows
     * and index rows on each index (checks both ascending and descending
     * directions).  Note: this could be very slow for big tables.
     * @param proxy HandlerProxy with table already open
     * @param schema TableSchema of open table
     * @param expectedRowCount Expected number of rows
     */
    public static void assertRowCount(final HandlerProxy proxy,
                                      final TableSchema schema,
                                      final long expectedRowCount) {
        checkState(proxy.getTableName() != null, "Proxy must have an open table.");
        checkNotNull(schema);
        verifyRowCount(expectedRowCount);

        proxy.startTableScan();
        assertResultCount(proxy, expectedRowCount);
        proxy.endScan();

        QueryKey queryKey;
        for(IndexSchema indexSchema : schema.getIndices()) {
            queryKey = new QueryKey(indexSchema.getIndexName(),
                    QueryType.INDEX_FIRST, ImmutableMap.<String, ByteBuffer>of());
            proxy.startIndexScan(queryKey.serialize());
            assertResultCount(proxy, expectedRowCount);
            proxy.endScan();

            queryKey = new QueryKey(indexSchema.getIndexName(),
                    QueryType.INDEX_LAST, ImmutableMap.<String, ByteBuffer>of());
            proxy.startIndexScan(queryKey.serialize());
            assertResultCount(proxy, expectedRowCount);
            proxy.endScan();
        }
    }

    /**
     * Checks that the open scan on the proxy has the expected number of results.
     * @param proxy
     * @param expectedResultCount
     */
    private static void assertResultCount(final HandlerProxy proxy,
                                          final long expectedResultCount) {
        long actualResultCount = 0;
        while(proxy.getNextRow() != null) {
            actualResultCount++;
        }
        assertEquals(expectedResultCount, actualResultCount);
    }

    /**
     * Start concurrency number of actions concurrently and at the same time.  This
     * method ensures that the actions are started at the same time, and they are
     * not being blocked in a thread pool.  Obviously, this uses concurrency
     * number of threads simultaneously so use caution as thread starvation
     * deadlock can occur with high concurrency levels.
     *
     * @param concurrency Number of handler proxies to run the action against, concurrently
     * @param setup Action to be run on each proxy before starting concurrent actions
     * @param action Action to be run concurrently and in sync with other proxies
     * @param cleanup Action to be run to cleanup proxy
     * @param factory HandlerProxyFactory to supply proxies to be run concurrently
     * @throws InterruptedException
     */
    public static void startProxyActionConcurrently(int concurrency,
                                                    final ProxyRunnable setup,
                                                    final ProxyRunnable action,
                                                    final ProxyRunnable cleanup,
                                                    final HandlerProxyFactory factory)
            throws InterruptedException{
        final ExecutorService executor = Executors.newCachedThreadPool();
        final CountDownLatch ready = new CountDownLatch(concurrency);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(concurrency);

        for (int i = 0; i < concurrency; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    HandlerProxy proxy = factory.createHandlerProxy();
                    setup.run(proxy);
                    ready.countDown();
                    try {
                        start.await();
                        action.run(proxy);
                    } catch (InterruptedException e) {
                        System.out.println("Interuppted");
                        Thread.currentThread().interrupt();
                    }
                    cleanup.run(proxy);
                    done.countDown();
                }
            });
        }
        ready.await();
        start.countDown();
        done.await();
    }

    public static ProxyRunnable openTable = new ITUtils.ProxyRunnable() {
        @Override
        public void run(HandlerProxy proxy) {
            proxy.openTable(TestConstants.TABLE_NAME);
        }
    };

    public static ITUtils.ProxyRunnable closeTable = new ITUtils.ProxyRunnable() {
        @Override
        public void run(HandlerProxy proxy) {
            proxy.closeTable();
        }
    };

    /**
     * Basically Runnable, but run takes a HandlerProxy which the action
     * may use.
     */
    public interface ProxyRunnable {
        public void run(HandlerProxy proxy);
    }
}
