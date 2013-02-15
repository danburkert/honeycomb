package com.nearinfinity.honeycomb.hbaseclient;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics implements MetricsMXBean {
    private static Metrics metrics = new Metrics();
    private AtomicLong hbaseTime;
    private AtomicInteger hbaseCalls;
    private AtomicLong parseResultTime;
    private AtomicLong parseRowMapTime;

    private Metrics() {
        hbaseCalls = new AtomicInteger(0);
        hbaseTime = new AtomicLong(0);
        parseResultTime = new AtomicLong(0);
        parseRowMapTime = new AtomicLong(0);
    }

    public static Metrics getInstance() {
        return metrics;
    }

    public void addHBaseTime(long time) {
        hbaseTime.getAndAdd(time);
        hbaseCalls.incrementAndGet();
    }

    public void addParseResultTime(long time) {
        parseResultTime.getAndAdd(time);
    }

    public void addParseRowMapTime(long time) {
        parseRowMapTime.getAndAdd(time);
    }

    @Override
    public void resetAll() {
        hbaseCalls.set(1);
        hbaseTime.set(0);
        parseResultTime.set(0);
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
}
