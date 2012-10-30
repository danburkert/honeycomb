package com.nearinfinity.datacreator;

import java.util.Date;

public class RandomDate {
    public static Date generate(Date start, Date end) {
        long startTime = start.getTime();
        long endTime = end.getTime();
        long date = startTime + (long)(Math.random() * (endTime - startTime));
        return new Date(date);
    }
}
