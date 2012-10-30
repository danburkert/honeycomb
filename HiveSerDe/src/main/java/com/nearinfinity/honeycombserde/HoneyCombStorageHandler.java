package com.nearinfinity.honeycombserde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.Map;

public class HoneyCombStorageHandler implements HiveStorageHandler {
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return HoneyCombInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HoneyCombOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return HoneyCombSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> stringStringMap) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> stringStringMap) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> stringStringMap) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setConf(Configuration entries) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Configuration getConf() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
