package integrationtests.scan;

import integrationtests.HoneycombIntegrationTest;
import integrationtests.ITUtils;
import integrationtests.TestConstants;

import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.config.Constants;
import com.nearinfinity.honeycomb.mysql.IndexKey;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;

public class ScanOperationsIT extends HoneycombIntegrationTest {

    private static final int ROW_COUNT = 3;
    private static final int INDEX_COL_VALUE = 5;

    @Test
    public void testIndexExactScan() {
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE);

        final IndexKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.EXACT_KEY);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT);
    }

    @Test
    public void testAfterKeyScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, Constants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE + 1, Constants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE + 1);

        final IndexKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.AFTER_KEY);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 1);
    }

    @Test
    public void testBeforeKeyScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, Constants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE - 1, Constants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE - 1);

        final IndexKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.BEFORE_KEY);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 1);
    }

    @Test
    public void testKeyOrNextScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, Constants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE + 1, Constants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE + 1);

        final IndexKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.KEY_OR_NEXT);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 2);
    }

    @Test
    public void testKeyOrPreviousScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, Constants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE - 1, Constants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE - 1);

        final IndexKey key = ITUtils.createKey(INDEX_COL_VALUE, QueryType.KEY_OR_PREVIOUS);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 2);
    }

    @Test
    public void testIndexLastScan() {
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, Constants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE - 1, Constants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE - 1);

        final IndexKey key = new IndexKey(TestConstants.INDEX1, QueryType.INDEX_LAST, Maps.<String, ByteBuffer>newHashMap());
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 2);
    }

    @Test
    public void testIndexFirstScan() {
        ITUtils.insertNullData(proxy, 2);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE, Constants.FULL_UUID);
        ITUtils.insertData(proxy, 1, INDEX_COL_VALUE + 1, Constants.ZERO_UUID);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE + 1);

        final IndexKey key = new IndexKey(TestConstants.INDEX1, QueryType.INDEX_FIRST, Maps.<String, ByteBuffer>newHashMap());
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + 4);
    }

    @Test
    public void testAfterKeyWithNullScan() {
        ITUtils.insertNullData(proxy, ROW_COUNT, TestConstants.COLUMN1);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE);
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE + 1);

        final Map<String, ByteBuffer> keyValues = Maps.newHashMap();
        keyValues.put(TestConstants.COLUMN1, ITUtils.encodeValue(2));

        final IndexKey key = new IndexKey(TestConstants.INDEX2, QueryType.AFTER_KEY, keyValues);
        ITUtils.assertReceivingDifferentRows(proxy, key, ROW_COUNT + ROW_COUNT);
    }

    @Test
    public void testFullTableScan() {
        ITUtils.insertData(proxy, ROW_COUNT, INDEX_COL_VALUE);
        ITUtils.assertReceivingDifferentRows(proxy, ROW_COUNT);
    }
}
