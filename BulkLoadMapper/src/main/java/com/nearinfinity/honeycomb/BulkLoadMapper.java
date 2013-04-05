package com.nearinfinity.honeycomb;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.hbase.HBaseMetadata;
import com.nearinfinity.honeycomb.hbase.HBaseStore;
import com.nearinfinity.honeycomb.hbase.MetadataCache;
import com.nearinfinity.honeycomb.hbase.MutationFactory;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class BulkLoadMapper
        extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    static final Logger LOG = Logger.getLogger(BulkLoadMapper.class);
    private RowParser rowParser;
    private String[] columns;
    private long tableId;
    private TableSchema schema;
    private MutationFactory mutationFactory;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        char separator  = conf.get("importtsv.separator", " ").charAt(0);
        columns = conf.getStrings("honeycomb.sql.columns");
        String zkQuorum = conf.get("zk.quorum");
        String sqlTable = conf.get("honeycomb.sql.table");
        String hbaseTable  = conf.get("honeycomb.hb.table");

        // Check that necessary configuration variables are set
        checkNotNull(zkQuorum, "zk.quorum not set.  Job will fail.");
        checkNotNull(sqlTable, "honeycomb.sql.table not set.  Job will fail.");
        checkNotNull(columns, "honeycomb.sql.columns not set.  Job will fail.");
        checkNotNull(hbaseTable, "honeycomb.hb.table not set.  Job will fail.");

        final HTableInterface table = new HTable(conf, hbaseTable);
        HBaseMetadata metadata = new HBaseMetadata(new SingleHTableProvider(table));
        HBaseStore store = new HBaseStore(metadata, null, new MetadataCache(metadata));

        tableId = store.getTableId(sqlTable);
        schema = store.getSchema(sqlTable);
        mutationFactory = new MutationFactory(store);

        rowParser = new RowParser(schema, columns, separator, columns);

        checkSqlColumnsMatch(sqlTable);
    }

    private void checkSqlColumnsMatch(String sqlTable) {
        Set<String> expectedColumns = schema.getColumns().keySet();
        List<String> invalidColumns = Lists.newLinkedList();
        for (String column : columns) {
            if (!expectedColumns.contains(column)) {
                LOG.error("Found non-existent column " + column);
                invalidColumns.add(column);
            }
        }
        if (invalidColumns.size() > 0) {
            String expectedColumnString = Joiner.on(",").join(expectedColumns);
            String invalidColumnString = Joiner.on(",").join(invalidColumns);
            String message = String.format("In table %s following columns (%s)" +
                    " are not valid columns. Expected columns (%s)",
                    sqlTable, invalidColumnString, expectedColumnString);
            throw new IllegalStateException(message);
        }
    }

    @Override
    public void map(LongWritable offset, Text line, Context context)
            throws InterruptedException {
        try {
            Row row = rowParser.parseRow(line.toString());

            List<Put> puts = mutationFactory.insert(tableId, row);

            for (Put put : puts) {
                context.write(new ImmutableBytesWritable(put.getRow()), put);
            }

            context.getCounter(Counters.ROWS).increment(1);
            context.getCounter(Counters.PUTS).increment(puts.size());

        } catch (IOException e) {
            LOG.error("CSVParser unable to parse line: " + line.toString(), e);
            context.getCounter(Counters.FAILED_ROWS).increment(1);
        } catch (IllegalArgumentException e) {
            LOG.error(format("The line %s was incorrectly formatted. Error %s",
                    line.toString(), e.getMessage()));
            context.getCounter(Counters.FAILED_ROWS).increment(1);
        } catch (ParseException e) {
            LOG.error(format("Parsing failed on line %s with message %s",
                    line.toString(), e.getMessage()));
            context.getCounter(Counters.FAILED_ROWS).increment(1);
        } catch (Exception e) {
            LOG.error(format("The following error %s occurred during mapping" +
                    " for line %s", e.getMessage(), line.toString()));
            context.getCounter(Counters.FAILED_ROWS).increment(1);
        }
    }

    public enum Counters {ROWS, FAILED_ROWS, PUTS}
}