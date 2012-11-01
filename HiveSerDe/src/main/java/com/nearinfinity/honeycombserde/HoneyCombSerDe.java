package com.nearinfinity.honeycombserde;

import com.nearinfinity.hbaseclient.HBaseClient;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class HoneyCombSerDe implements SerDe {
    @Override
    public void initialize(Configuration entries, Properties properties) throws SerDeException {
        // Retrieve HoneyComb metadata for table.
        String sqlTableName = properties.getProperty(Constants.META_TABLE_NAME);
        HBaseClient client; // TODO: How to get ZK Quorum and hbase table?
        try {
            client = new HBaseClient("sql", "localhost");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        TableInfo info;
        try {
            info = client.getTableInfo(sqlTableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Set<String> columns = info.getColumnNames();
        LazySimpleSerDe.SerDeParameters parameters = LazySimpleSerDe.initSerdeParams(entries, properties, getClass().getName());
        List<String> declaredColumns = parameters.getColumnNames();
        checkColumn(columns, declaredColumns);
    }

    private void checkColumn(Set<String> columns, List<String> declaredColumns) throws SerDeException {
        if (declaredColumns.size() != columns.size()) {
            throw new SerDeException("All columns must be in HBase");
        }

        for (String column : declaredColumns) {
            if (!columns.contains(column)) {
                throw new SerDeException("Column names must match up.");
            }
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
