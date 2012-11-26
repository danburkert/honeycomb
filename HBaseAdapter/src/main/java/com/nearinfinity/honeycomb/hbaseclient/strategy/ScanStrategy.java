package com.nearinfinity.honeycomb.hbaseclient.strategy;

import com.nearinfinity.honeycomb.hbaseclient.TableInfo;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 7:51 AM
 * To change this template use File | Settings | File Templates.
 */
public interface ScanStrategy {
    public Scan getScan(TableInfo info);

    public String getTableName();
}
