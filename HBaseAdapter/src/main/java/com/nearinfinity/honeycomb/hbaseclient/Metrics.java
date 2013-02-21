package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics implements MetricsMXBean {
    private static final Logger logger = Logger.getLogger(Metrics.class);
    private static Metrics metrics = new Metrics();
    private AtomicLong hbaseTime;
    private AtomicInteger hbaseCalls;
    private AtomicLong parseResultTime;
    private AtomicLong parseRowMapTime;
    private Map<String, Long> statistics;

    private Metrics() {
        hbaseCalls = new AtomicInteger(0);
        hbaseTime = new AtomicLong(0);
        parseResultTime = new AtomicLong(0);
        parseRowMapTime = new AtomicLong(0);
        statistics = new HashMap<String, Long>();
    }

    public static Metrics getInstance() {
        return metrics;
    }

    public void addHBaseTime(long time) {
        hbaseTime.getAndAdd(time);
        hbaseCalls.incrementAndGet();
    }

    public void addStat(String key, long time) {
        Long value = statistics.get(key);
        if (value != null) {
            statistics.put(key, value.longValue() + time);
        } else {
            statistics.put(key, time);
        }
    }

    @Override
    public void resetAll() {
        hbaseCalls.set(1);
        hbaseTime.set(0);
        parseResultTime.set(0);
        statistics.clear();
    }

    @Override
    public long getHBaseCalls() {
        return hbaseCalls.get();
    }

    @Override
    public long getHBaseTime() {
        return hbaseTime.get();
    }

    @Override
    public long getParseResultTime() {
        return parseResultTime.get();
    }

    @Override
    public long getParseRowMapTime() {
        return parseRowMapTime.get();
    }

    @Override
    public Map<String, Long> getStatistics() {
        for (Map.Entry<String, Long> s : statistics.entrySet()) {
            logger.info(s.getKey() + " => " + s.getValue());
        }
        logger.info("\n");
        return statistics;
    }
}
