package com.nearinfinity.bulkloader;

import com.nearinfinity.hbaseclient.Index;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static java.lang.String.format;

public class SamplingMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private Random random;
    private String[] columnNames;
    private TableInfo tableInfo;
    private double samplePercent;
    private static final Log LOG = LogFactory.getLog(SamplingMapper.class);
    private List<List<String>> indexColumns;

    @Override
    protected void setup(Context context) throws IOException {
        random = new Random();
        Configuration conf = context.getConfiguration();

        samplePercent = Double.parseDouble(conf.get("sample_percent"));
        tableInfo = BulkLoader.extractTableInfo(conf);
        columnNames = conf.get("my_columns").split(",");
        indexColumns = Index.indexForTable(tableInfo.tableMetadata());
    }

    @Override
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        double coinFlip = random.nextDouble();
        if (samplePercent >= coinFlip) {
            List<Put> puts = BulkLoader.createPuts(line, tableInfo, columnNames, indexColumns);

            for (Put put : puts) {
                byte[] row = extractReduceKey(put);
                context.write(new ImmutableBytesWritable(row), put);
            }

            context.getCounter(BulkLoader.Counters.ROWS).increment(1);

        }
    }

    public static byte[] extractReduceKey(Put put) {
        byte[] rowBuffer = put.getRow();
        byte[] row;
        if (rowBuffer[0] == 3) {
            row = new byte[9];
        } else {
            row = new byte[17];
        }

        System.arraycopy(rowBuffer, 0, row, 0, row.length);
        return row;
    }
}
