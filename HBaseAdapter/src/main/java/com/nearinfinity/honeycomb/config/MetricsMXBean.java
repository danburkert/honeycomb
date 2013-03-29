package com.nearinfinity.honeycomb.config;

import java.util.Map;

public interface MetricsMXBean {
    void resetAll();

    long getHBaseCalls();

    long getHBaseTime();

    long getParseResultTime();

    long getParseRowMapTime();

    Map<String, Long> getStatistics();
}
