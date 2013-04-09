package integrationtests.index;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import integrationtests.HoneycombIntegrationTest;
import integrationtests.ITUtils;
import integrationtests.TestConstants;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexOperationsIT extends HoneycombIntegrationTest {

    private static final int ROW_COUNT = 1;
    private static final int INDEX_COL_VALUE = 5;
    private static final String NEW_INDEX_NAME = "i3";

    @Test
    public void testAddIndex() {
        final IndexSchema indexSchema = new IndexSchema(NEW_INDEX_NAME, Lists.newArrayList(TestConstants.COLUMN1), false);

        // Add data rows to index
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE);

        // Add the new index to the table
        proxy.addIndex(NEW_INDEX_NAME, indexSchema.serialize());

        // Perform a scan with the new index

        final QueryKey key = new QueryKey(NEW_INDEX_NAME, QueryType.EXACT_KEY,
                ImmutableMap.<String, ByteBuffer>of(TestConstants.COLUMN1, ITUtils.encodeValue(INDEX_COL_VALUE)));

        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT);
    }

    @Test
    public void testAddCompoundIndex() {
        // Create the compound index ordered as (col2, col1)
        final IndexSchema indexSchema = new IndexSchema(NEW_INDEX_NAME, Lists.newArrayList(TestConstants.COLUMN2, TestConstants.COLUMN1), false);

        final int column2Value = 0;

        // Add data rows to index
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE);

        // Add the new index to the table
        proxy.addIndex(NEW_INDEX_NAME, indexSchema.serialize());

        // Perform a scan with the new index

        final QueryKey key = new QueryKey(NEW_INDEX_NAME, QueryType.EXACT_KEY,
                ImmutableMap.<String, ByteBuffer>of(TestConstants.COLUMN1, ITUtils.encodeValue(INDEX_COL_VALUE),
                        TestConstants.COLUMN2, ITUtils.encodeValue(column2Value)));

        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT);
    }

    @Test(expected = NullPointerException.class)
    public void testDropIndex() {
        final int keyValue = 7;
        final QueryKey key = ITUtils.createKey(keyValue, QueryType.EXACT_KEY);

        // Add data rows to index
        ITUtils.insertData(proxy, ROW_COUNT, keyValue);

        // Verify that we can get a row from the index scan
        proxy.startIndexScan(key.serialize());
        final Row row = Row.deserialize(proxy.getNextRow());
        byte[] result = proxy.getRow(Util.UUIDToBytes(row.getUUID()));

        assertNotNull(row);
        assertEquals(Row.deserialize(result).getUUID(), row.getUUID());

        proxy.endScan();

        // Drop the index from the table
        proxy.dropIndex(TestConstants.INDEX1);

        // Verify that the data row is still available after the index has been removed
        result = proxy.getRow(Util.UUIDToBytes(row.getUUID()));
        assertEquals(Row.deserialize(result).getUUID(), row.getUUID());

        // Verify that the scan is unable to execute
        proxy.startIndexScan(key.serialize());
    }

}
