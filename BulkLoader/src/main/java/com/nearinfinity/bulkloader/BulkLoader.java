package com.nearinfinity.bulkloader;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.JsonSyntaxException;
import com.nearinfinity.hbaseclient.ColumnMetadata;
import com.nearinfinity.hbaseclient.HBaseClient;
import com.nearinfinity.hbaseclient.PutListFactory;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.*;

import static java.text.MessageFormat.format;

public class BulkLoader extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(BulkLoader.class);

    public static final String TableInfoPath = "table_info";

    public enum Counters {ROWS, FAILED_ROWS}

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BulkLoader(), args);
        System.exit(exitCode);
    }

    public static List<Put> createPuts(Text line, TableInfo tableInfo, String[] columnNames, List<List<String>> indexColumns) throws IOException {
        CSVReader reader = new CSVReader(new StringReader(line.toString()));
        String[] columnData = reader.readNext();

        if (columnData.length != columnNames.length) {
            throw new IllegalStateException(format("Row has wrong number of columns. Expected {0} got {1}. Line: {2}", columnNames.length, columnData.length, line.toString()));
        }

        Map<String, byte[]> valueMap = new TreeMap<String, byte[]>();

        String name;
        ColumnMetadata meta;
        for (int i = 0; i < columnData.length; i++) {
            name = columnNames[i];
            meta = tableInfo.getColumnMetadata(name);
            try {
                byte[] value = ValueParser.parse(columnData[i], meta);
                if (value != null) {
                    valueMap.put(name, value);
                } else if (!meta.isNullable()) {
                    throw new IllegalStateException(format("No value provided for non-nullable column {0}", name));
                }
            } catch (NumberFormatException e) {
                String format = format("Number format error. Column: {0} Value: {1}", name, columnData[i]);
                LOG.error(format);
                throw new RuntimeException(format, e);
            } catch (ParseException e) {
                String format = format("Parse exception for column {0}", name);
                LOG.error(format);
                throw new RuntimeException(format, e);
            }
        }

        return PutListFactory.createDataInsertPutList(valueMap, tableInfo, indexColumns);
    }

    private static TableInfo getTableInfo(Configuration conf) throws IOException {
        String zkQuorum = conf.get("zk_quorum");
        String sqlTableName = conf.get("sql_table_name");
        String hbaseTableName = conf.get("hb_table");

        HBaseClient client = new HBaseClient(hbaseTableName, zkQuorum);

        return client.getTableInfo(sqlTableName);
    }

    public static TableInfo extractTableInfo(Configuration conf) throws IOException {
        TableInfo info = new TableInfo("Dummy", 0);
        final String data = conf.get(TableInfoPath);
        try {
            info.read(data);
        } catch (JsonSyntaxException e) {
            LOG.error(format("JSON {0} is not valid.", data));
            throw e;
        }
        return info;
    }

    public static Configuration updateConf(Configuration argConf, Map<String, String> params, String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        merge(conf, argConf);

        updateConfiguration(conf, args, params);
        return conf;
    }

    private static void merge(Configuration destConf, Configuration srcConf) {
        for (Map.Entry<String, String> e : srcConf) {
            destConf.set(e.getKey(), e.getValue());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: com.nearinfinity.bulkloader.BulkLoader [generic arguments] <input path> <MySQL table name> <SQL table columns in order>");
            return 1;
        }

        Map<String, String> params = readConfigOptions();

        Configuration argConf = getConf();
        Configuration conf = updateConf(argConf, params, args);

        LoadStrategy loadStrategy;
        loadStrategy = new HFilesLoadStrategy(conf);
        //loadStrategy = new PutsLoadStrategy(conf);
        loadStrategy.load();

        return 0;
    }

    private static void setupTableInfo(Configuration conf) throws IOException {
        TableInfo info = getTableInfo(conf);
        conf.set(TableInfoPath, info.write());
    }

    private static void updateConfiguration(Configuration conf, String[] args, Map<String, String> params)
            throws IOException {
        String inputPath = args[0];
        String sqlTable = args[1];

        conf.setIfUnset("sql_table_name", sqlTable);
        conf.setIfUnset("hb_family", params.get("hbase_family"));
        conf.setIfUnset("zk_quorum", params.get("zk_quorum"));
        if ("localhost".equalsIgnoreCase(conf.get("hbase.zookeeper.quorum"))) {
            conf.set("hbase.zookeeper.quorum", params.get("zk_quorum"));
        } else {
            conf.setIfUnset("hbase.zookeeper.quorum", params.get("zk_quorum"));
        }
        conf.setIfUnset("hb_table", params.get("hbase_table_name"));
        conf.setIfUnset("output_path", "bulk_loader_output_" + System.currentTimeMillis() + ".tmp");
        conf.setIfUnset("input_path", inputPath);
        int columnCount = setupColumns(conf, args, sqlTable);

        SamplingPartitioner.setColumnCount(conf, columnCount);
        updateDataInfo(conf, inputPath);
        setupTableInfo(conf);
    }

    private static int setupColumns(Configuration conf, String[] args, String sqlTable) throws IOException {
        TableInfo info = getTableInfo(conf);
        Set<String> columnNames = info.getColumnNames();
        List<String> columns = Lists.newLinkedList();
        boolean failEarly = false;
        for (int x = 2; x < args.length; x++) {
            String arg = args[x];
            if (!columnNames.contains(arg)) {
                LOG.error(format("Column {0} is not part of table {1}", arg, sqlTable));
                failEarly = true;
                continue;
            }
            columns.add(arg);
        }

        if (failEarly) {
            System.exit(1);
        }
        String columnsString = Joiner.on(",").join(columns);
        LOG.info(format("Columns: {0}", columnsString));
        int columnCount = columns.size();
        conf.setIfUnset("my_columns", columnsString);
        return columnCount;
    }

    private static void updateDataInfo(Configuration conf, String inputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        ContentSummary summary = fileSystem.getContentSummary(new Path(inputPath));
        long dataLength = summary.getLength();
        fileSystem.close();
        conf.setLong("data_size", dataLength);

        long hfileSize = SamplingReducer._1GB;
        long hfilesExpected = calculateExpectedHFileCount(dataLength, hfileSize);

        conf.setLong("hbase.hregion.max.filesize", hfileSize);

        LOG.info(format("HBase HFile size {0}", conf.get("hbase.hregion.max.filesize")));
        LOG.info(format("Size of data to load {0}", dataLength));
        LOG.info(format("*** Expected number hfiles created: {0} per reducer/{1} total. ***", hfilesExpected, hfilesExpected * 3));

        conf.setLong("hfiles_expected", hfilesExpected);
        long sampleSize = Math.round(dataLength * 0.20);
        double samplePercent = sampleSize / (float) dataLength;
        conf.setIfUnset("sample_size", Long.toString(sampleSize));
        conf.set("sample_percent", Double.toString(samplePercent));

        LOG.info(format("Sample size {0}, Data size {1}, Sample Percent {2}", sampleSize, dataLength, samplePercent));
    }

    public static long calculateExpectedHFileCount(long dataSize, long hfileSize) {
        long expandedSize = 62 * dataSize;
        if (hfileSize > expandedSize) {
            return 1;
        }

        long expectedHFiles = expandedSize / hfileSize;
        long hfilesPerPartition = expectedHFiles / 3;
        return Math.max(hfilesPerPartition, 1);
    }

    private static Map<String, String> readConfigOptions() throws FileNotFoundException {
        //Read config options from adapter.conf
        Scanner confFile = new Scanner(new File("/etc/mysql/adapter.conf"));
        Map<String, String> params = new TreeMap<String, String>();
        while (confFile.hasNextLine()) {
            Scanner line = new Scanner(confFile.nextLine());
            params.put(line.next(), line.next());
        }
        return params;
    }
}
