package com.nearinfinity.honeycomb.mysqlengine;

public enum IndexReadType {
    HA_READ_KEY_EXACT,
    HA_READ_AFTER_KEY,
    HA_READ_KEY_OR_NEXT,
    HA_READ_KEY_OR_PREV,
    HA_READ_BEFORE_KEY,
    INDEX_FIRST,
    INDEX_LAST,
    INDEX_NULL
}
