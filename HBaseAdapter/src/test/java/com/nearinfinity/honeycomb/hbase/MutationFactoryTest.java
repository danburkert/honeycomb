package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.nearinfinity.honeycomb.ColumnSchemaFactory;
import com.nearinfinity.honeycomb.IndexSchemaFactory;
import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.TableSchemaFactory;
import com.nearinfinity.honeycomb.exceptions.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRow;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.TableSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MutationFactoryTest {
    private static final String TABLE = "t1";
    private static final String COLUMN1 = "c1";
    private static final String COLUMN2 = "c2";
    private static final String INDEX1 = "i1";
    private static final String INDEX2 = "i2";
    private static final Map<String, ColumnSchema> COLUMNS = new HashMap<String, ColumnSchema>() {{
        put(COLUMN1, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, false, true, 8, 0, 0));
        put(COLUMN2, ColumnSchemaFactory.createColumnSchema(ColumnType.STRING, true, false, 32, 0, 0));
    }};
    private static final Map<String, IndexSchema> INDICES = new HashMap<String, IndexSchema>() {{
        put(INDEX1, IndexSchemaFactory.createIndexSchema(Lists.newArrayList(COLUMN1), false, INDEX1));
        put(INDEX2, IndexSchemaFactory.createIndexSchema(Lists.newArrayList(COLUMN1, COLUMN2), true, INDEX2));
    }};
    private static final Row row = new Row(
            new HashMap<String, ByteBuffer>() {{
                put(COLUMN1, ByteBuffer.wrap(Longs.toByteArray(123)));
                put(COLUMN2, ByteBuffer.wrap("foobar".getBytes()));
            }},
            UUID.randomUUID()
    );
    private static final byte DATA_PREFIX = new DataRow(0, null).getPrefix();
    private static final byte ASC_PREFIX = IndexRowBuilder.newBuilder(0, 0)
            .withSortOrder(SortOrder.Ascending).build().getPrefix();
    private static final byte DESC_PREFIX = IndexRowBuilder.newBuilder(0, 0)
            .withSortOrder(SortOrder.Descending).build().getPrefix();
    private MutationFactory factory;
    private long tableId;

    @Before
    public void testSetup() {
        HBaseTableFactory tableFactory = mock(HBaseTableFactory.class);
        HTableProvider provider = mock(HTableProvider.class);

        MockitoAnnotations.initMocks(this);
        MockHTable table = MockHTable.create();
        when(provider.get()).thenReturn(table);

        HBaseMetadata metadata = new HBaseMetadata(provider);
        MetadataCache cache = new MetadataCache(metadata);

        HBaseStore store = new HBaseStore(metadata, tableFactory, cache);
        factory = new MutationFactory(store);

        TableSchema schema = TableSchemaFactory.createTableSchema(COLUMNS, INDICES);

        store.createTable(TABLE, schema);
        tableId = store.getTableId(TABLE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertRejectsInvalidTableId() throws Exception {
        factory.insert(-1, row);
    }

    @Test(expected = TableNotFoundException.class)
    public void testInsertRejectsUnknownTableId() throws Exception {
        factory.insert(99, row);
    }

    @Test(expected = NullPointerException.class)
    public void testInsertRejectsNullRow() throws Exception {
        factory.insert(1, null);
    }

    @Test
    public void testInsert() throws Exception {
        List<Put> puts = factory.insert(tableId, row);
        byte[] rowCounts = countRowTypes(puts);

        assertEquals("data row count", 1, rowCounts[DATA_PREFIX]);
        assertEquals("ascending index count", 2, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 2, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 5, puts.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartialInsertRejectsInvalidTableId() throws Exception {
        factory.insert(-1, row, new ArrayList<IndexSchema>());
    }

    @Test(expected = TableNotFoundException.class)
    public void testPartialInsertRejectsUnknownTableId() throws Exception {
        factory.insert(99, row, ImmutableList.of(INDICES.get(INDEX1)));
    }

    @Test(expected = NullPointerException.class)
    public void testPartialInsertRejectsNullRow() throws Exception {
        factory.insert(1, null, new ArrayList<IndexSchema>());
    }

    @Test
    public void testPartialInsert() throws Exception {
        List<Put> puts = factory.insert(tableId, row,
                ImmutableList.of(INDICES.get(INDEX1)));
        byte[] rowCounts = countRowTypes(puts);

        assertEquals("data row count", 1, rowCounts[DATA_PREFIX]);
        assertEquals("ascending index count", 1, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 1, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 3, puts.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteIndicesRejectsInvalidTableId() throws Exception {
        factory.deleteIndices(-1, row);
    }

    @Test(expected = TableNotFoundException.class)
    public void testDeleteIndicesRejectsUnknownTableId() throws Exception {
        factory.deleteIndices(99, row);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteIndicesRejectsNullRow() throws Exception {
        factory.deleteIndices(1, null);
    }

    @Test
    public void testDeleteIndices() throws Exception {
        List<Delete> deletes = factory.deleteIndices(tableId, row);
        byte[] rowCounts = countRowTypes(deletes);

        assertEquals("ascending index count", 2, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 2, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 4, deletes.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartialDeleteIndicesRejectsInvalidTableId() throws Exception {
        factory.deleteIndices(-1, row, new ArrayList<IndexSchema>());
    }

    @Test(expected = TableNotFoundException.class)
    public void testPartialDeleteIndicesRejectsUnknownTableId() throws Exception {
        factory.deleteIndices(99, row, ImmutableList.of(INDICES.get(INDEX1)));
    }

    @Test(expected = NullPointerException.class)
    public void testPartialDeleteIndicesRejectsNullRow() throws Exception {
        factory.deleteIndices(1, null, new ArrayList<IndexSchema>());
    }

    @Test
    public void testPartialDeleteIndices() throws Exception {
        List<Delete> deletes = factory.deleteIndices(tableId, row,
                ImmutableList.of(INDICES.get(INDEX1)));
        byte[] rowCounts = countRowTypes(deletes);

        assertEquals("ascending index count", 1, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 1, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 2, deletes.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteRejectsInvalidTableId() throws Exception {
        factory.delete(-1, row);
    }

    @Test(expected = TableNotFoundException.class)
    public void testDeleteRejectsUnknownTableId() throws Exception {
        factory.delete(99, row);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteRejectsNullRow() throws Exception {
        factory.delete(1, null);
    }

    @Test
    public void testDelete() throws Exception {
        List<Delete> deletes = factory.delete(tableId, row);
        byte[] rowCounts = countRowTypes(deletes);

        assertEquals("data row count", 1, rowCounts[DATA_PREFIX]);
        assertEquals("ascending index count", 2, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 2, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 5, deletes.size());
    }

    private byte[] countRowTypes(List<? extends Mutation> mutations) {
        int numRowTypes = 9;
        byte[] rowCounts = new byte[numRowTypes];

        for (Mutation mutation : mutations) {
            rowCounts[mutation.getRow()[0]]++;
        }
        return rowCounts;
    }
}