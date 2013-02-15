package com.nearinfinity.honeycomb.hbaseclient;

public interface MetricsMXBean {
    void resetAll();

    long getHBaseCalls();

    long getHBaseTime();

    long getParseResultTime();

    long getParseRowMapTime();
}
