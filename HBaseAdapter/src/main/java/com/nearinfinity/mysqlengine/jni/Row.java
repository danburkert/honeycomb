package com.nearinfinity.mysqlengine.jni;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 10:32 AM
 * To change this template use File | Settings | File Templates.
 */
public class Row {
    private Map<String, byte[]> values;

    public Map<String, byte[]> getValues() {
        return values;
    }

    public void setValues(Map<String, byte[]> values) {
        this.values = values;
    }
}
