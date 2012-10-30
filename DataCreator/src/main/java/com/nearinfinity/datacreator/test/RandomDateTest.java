package com.nearinfinity.datacreator.test;

import com.nearinfinity.datacreator.RandomDate;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertTrue;

public class RandomDateTest {
    @Test
    public void testGenerate() {
        Date start = new GregorianCalendar(2010, 1, 1).getTime();
        Date end = new GregorianCalendar(2012, 10, 30).getTime();
        Date date = RandomDate.generate(start, end);
        assertTrue(date.getTime() >= start.getTime() && date.getTime() <= end.getTime());
    }
}
