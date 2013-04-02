package com.nearinfinity.honeycomb.hbase.rowkey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;

public class IndexRowBuilderTest {

    private static final String COLUMN_BAR = "bar";
    private static final String COLUMN_FOO = "foo";

    private static final int ASC_INDEX_PREFIX = 0x07;
    private static final int DESC_INDEX_PREFIX = 0x08;

    private static final long TABLE_ID = 1;
    private static final long INDEX_ID = 5;
    private static final long INVERT_SIGN_MASK = 0x8000000000000000L;

    private IndexRowBuilder builder;

    @Before
    public void setupTestCases() {
        builder = IndexRowBuilder.newBuilder(TABLE_ID, INDEX_ID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewBuilderInvalidTableId() {
        final long invalidTableId = -1;
        IndexRowBuilder.newBuilder(invalidTableId, INDEX_ID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewBuilderInvalidIndexId() {
        final long invalidIndexId = -1;
        IndexRowBuilder.newBuilder(TABLE_ID, invalidIndexId);
    }

    @Test
    public void testNewBuilder() {
        assertNotNull(IndexRowBuilder.newBuilder(TABLE_ID, INDEX_ID));
    }


    @Test(expected = NullPointerException.class)
    public void testBuilderNullSortOrder() {
        builder.withSortOrder(null);
    }

    @Test
    public void testBuilderSortOrder() {
        assertNotNull(builder.withSortOrder(SortOrder.Descending));
    }

    @Test(expected = NullPointerException.class)
    public void testBuilderNullUUID() {
        builder.withUUID(null);
    }

    @Test
    public void testBuilderUUID() {
        assertNotNull(builder.withUUID(UUID.randomUUID()));
    }

    @Test(expected = NullPointerException.class)
    public void testBuilderRecordsNullRecords() {
        builder.withRecords(null, ImmutableMap.<String, ColumnType>of(), ImmutableList.<String>of());
    }

    @Test(expected = NullPointerException.class)
    public void testBuilderRecordsNullColumnTypes() {
        builder.withRecords(ImmutableMap.<String, ByteBuffer>of(), null, ImmutableList.<String>of());
    }

    @Test(expected = NullPointerException.class)
    public void testBuilderRecordsNullColumnOrder() {
        builder.withRecords(ImmutableMap.<String, ByteBuffer>of(), ImmutableMap.<String, ColumnType>of(), null);
    }

    @Test
    public void testBuilderRecords() {
        builder.withRecords(ImmutableMap.<String, ByteBuffer>of(), ImmutableMap.<String, ColumnType>of(), ImmutableList.<String>of());
    }

    @Test
    public void testBuildAscendingIndex() {
        final IndexRow row = builder.withSortOrder(SortOrder.Ascending).build();

        assertEquals(TABLE_ID, row.getTableId());
        assertEquals(INDEX_ID, row.getIndexId());
        assertEquals(ASC_INDEX_PREFIX, row.getPrefix());
        assertEquals(SortOrder.Ascending, row.getSortOrder());
    }


    @Test
    public void testBuildDescendingIndex() {
        final IndexRow row = builder.withSortOrder(SortOrder.Descending).build();

        assertEquals(TABLE_ID, row.getTableId());
        assertEquals(INDEX_ID, row.getIndexId());
        assertEquals(SortOrder.Descending, row.getSortOrder());
        assertEquals(DESC_INDEX_PREFIX, row.getPrefix());
    }


    @Test
    public void testBuildAscendingIndexWithEmptyRecords() {
        final IndexRow row = builder.withRecords(ImmutableMap.<String, ByteBuffer>of(),
                ImmutableMap.<String, ColumnType>of(), ImmutableList.<String>of()).build();

        assertEquals(ImmutableList.<byte[]>of(), row.getRecords());
    }

    @Test
    public void testBuildAscendingIndexWithSortedRecords() {
        final String fooValue = "something";
        final double barValue = -3.14;

        final ByteBuffer fooBuffer = ByteBuffer.wrap(fooValue.getBytes(Charsets.UTF_8));
        fooBuffer.rewind();

        final ByteBuffer barBuffer = ByteBuffer.allocate(8).putDouble(barValue);
        barBuffer.rewind();

        final Map<String, ByteBuffer> records = ImmutableMap.<String, ByteBuffer>of(COLUMN_FOO, fooBuffer, COLUMN_BAR, barBuffer);

        final IndexRow row = builder.withRecords(records,
                ImmutableMap.<String, ColumnType>of(COLUMN_FOO, ColumnType.STRING, COLUMN_BAR, ColumnType.DOUBLE),
                ImmutableList.<String>of(COLUMN_BAR, COLUMN_FOO)).build();

        // Verify that the records and that their column values are in the order specified
        assertEquals(records.size(), row.getRecords().size());
        assertEquals(barValue, decodeDoubleValue(row.getRecords().get(0), true, false), .001);
        assertEquals(fooValue, new String(row.getRecords().get(1), Charsets.UTF_8));
    }

    @Test
    public void testBuildDescendingIndexWithEmptyRecords() {
        final IndexRow row = builder.withSortOrder(SortOrder.Descending)
                .withRecords(ImmutableMap.<String, ByteBuffer>of(),
                ImmutableMap.<String, ColumnType>of(), ImmutableList.<String>of()).build();

        assertEquals(ImmutableList.<byte[]>of(), row.getRecords());
    }

    @Test
    public void testBuildDescendingIndexWithSortedRecords() {
        final long fooValue = 7;
        final double barValue = 0.0;

        final ByteBuffer fooBuffer = ByteBuffer.allocate(8).putLong(fooValue);
        fooBuffer.rewind();

        final ByteBuffer barBuffer = ByteBuffer.allocate(8).putDouble(barValue);
        barBuffer.rewind();

        final Map<String, ByteBuffer> records = ImmutableMap.<String, ByteBuffer>of(COLUMN_FOO, fooBuffer, COLUMN_BAR, barBuffer);

        final IndexRow row = builder.withSortOrder(SortOrder.Descending)
                .withRecords(records, ImmutableMap.<String, ColumnType>of(COLUMN_FOO, ColumnType.ULONG, COLUMN_BAR, ColumnType.DOUBLE),
                ImmutableList.<String>of(COLUMN_FOO, COLUMN_BAR)).build();

        // Verify that the records and that their column values are in the order specified
        // Since this is a descending index the values need to be reversed to obtain original value
        assertEquals(records.size(), row.getRecords().size());
        assertEquals(fooValue, decodeLongValue(row.getRecords().get(0), true));
        assertEquals(barValue, decodeDoubleValue(row.getRecords().get(1), true, true), .001);
    }

    /**
     * Decodes the encoded double value encoded by the {@link IndexRowBuilder}
     *
     * @param encodedBytes The encoded bytes
     * @param reverse Flag used to indicate if the value should be reversed during decoding
     * @param invertSign Flag used to indicate if the sign of the value needs to be inverted
     * @return The double value representing the original value
     */
    private static double decodeDoubleValue(final byte[] encodedBytes, boolean reverse, boolean invertSign) {
        final ByteBuffer buffer = ByteBuffer.wrap(encodedBytes);
        long longValue = buffer.getLong();

        if( reverse ) {
            longValue = ~longValue;
        }

        if( invertSign ) {
            return Double.longBitsToDouble(longValue ^ INVERT_SIGN_MASK);
        }

        return Double.longBitsToDouble(longValue);
    }


    /**
     * Decodes the encoded long value encoded by the {@link IndexRowBuilder}
     *
     * @param encodedBytes The encoded bytes
     * @param reverse Flag used to indicate if the value should be reversed during decoding
     * @return The long value representing the original value
     */
    private static long decodeLongValue(final byte[] encodedBytes, boolean reverse) {
        final ByteBuffer buffer = ByteBuffer.wrap(encodedBytes);
        long longValue = buffer.getLong();

        if( reverse ) {
            longValue = ~longValue;
        }

        return longValue ^ INVERT_SIGN_MASK;
    }
 }
