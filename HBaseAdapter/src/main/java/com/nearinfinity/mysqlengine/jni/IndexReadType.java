package com.nearinfinity.mysqlengine.jni;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/15/12
 * Time: 10:55 AM
 * To change this template use File | Settings | File Templates.
 */
public enum IndexReadType {
    HA_READ_KEY_EXACT,
    HA_READ_AFTER_KEY,
    HA_READ_KEY_OR_NEXT,
    HA_READ_KEY_OR_PREV,
    HA_READ_BEFORE_KEY
}
