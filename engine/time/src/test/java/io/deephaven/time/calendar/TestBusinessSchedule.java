package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;

public class TestBusinessSchedule extends BaseArrayTestCase {

    public void testBusinessSchedule() {
        final DateTime open1 = DateTimeUtils.convertDateTime("2017-03-11T10:00:00.000000000 NY");
        final DateTime close1 = DateTimeUtils.convertDateTime("2017-03-11T11:00:00.000000000 NY");
        final BusinessPeriod period1 = new BusinessPeriod(open1, close1);
        final DateTime open2 = DateTimeUtils.convertDateTime("2017-03-11T12:00:00.000000000 NY");
        final DateTime close2 = DateTimeUtils.convertDateTime("2017-03-11T17:00:00.000000000 NY");
        final BusinessPeriod period2 = new BusinessPeriod(open2, close2);

        // empty
        final BusinessSchedule empty = new BusinessSchedule();
        assertEquals(new BusinessPeriod[0], empty.getBusinessPeriods());
        assertNull(empty.getSOBD());
        assertNull(empty.getStartOfBusinessDay());
        assertNull(empty.getEOBD());
        assertNull(empty.getEndOfBusinessDay());
        assertEquals(0L, empty.getLOBD());
        assertEquals(0L, empty.getLengthOfBusinessDay());
        assertFalse(empty.isBusinessDay());
        assertFalse(empty.isBusinessTime(open1));
        assertFalse(empty.isBusinessTime(close1));
        assertEquals(0L, empty.businessTimeElapsed(open1));
        assertEquals(0L, empty.businessTimeElapsed(close1));



        // single period
        final BusinessSchedule single = new BusinessSchedule(period1);
        assertEquals(new BusinessPeriod[] {period1}, single.getBusinessPeriods());
        assertEquals(open1, single.getSOBD());
        assertEquals(open1, single.getStartOfBusinessDay());
        assertEquals(close1, single.getEOBD());
        assertEquals(close1, single.getEndOfBusinessDay());
        assertEquals(DateTimeUtils.HOUR, single.getLOBD());
        assertEquals(DateTimeUtils.HOUR, single.getLengthOfBusinessDay());
        assertTrue(single.isBusinessDay());
        assertTrue(single.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T10:00:00.000000000 NY")));
        assertTrue(single.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T10:15:00.000000000 NY")));
        assertTrue(single.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T11:00:00.000000000 NY")));
        assertFalse(single.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T11:10:00.000000000 NY")));
        assertEquals(0L, single.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                single.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR,
                single.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T13:00:00.000000000 NY")));


        // multi period
        final BusinessSchedule multi = new BusinessSchedule(period1, period2);
        assertEquals(new BusinessPeriod[] {period1, period2}, multi.getBusinessPeriods());
        assertEquals(open1, multi.getSOBD());
        assertEquals(open1, multi.getStartOfBusinessDay());
        assertEquals(close2, multi.getEOBD());
        assertEquals(close2, multi.getEndOfBusinessDay());
        assertEquals(DateTimeUtils.HOUR * 6, multi.getLOBD());
        assertEquals(DateTimeUtils.HOUR * 6, multi.getLengthOfBusinessDay());
        assertTrue(multi.isBusinessDay());
        assertTrue(multi.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T10:00:00.000000000 NY")));
        assertTrue(multi.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T10:15:00.000000000 NY")));
        assertTrue(multi.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T11:00:00.000000000 NY")));
        assertFalse(multi.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T11:10:00.000000000 NY")));
        assertTrue(multi.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T12:10:00.000000000 NY")));
        assertEquals(0L, multi.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                multi.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2,
                multi.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2,
                multi.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T13:00:00.000000000 NY")));

        final BusinessSchedule multi2 = new BusinessSchedule(period2, period1);
        assertEquals(new BusinessPeriod[] {period1, period2}, multi2.getBusinessPeriods());
        assertEquals(open1, multi2.getSOBD());
        assertEquals(open1, multi2.getStartOfBusinessDay());
        assertEquals(close2, multi2.getEOBD());
        assertEquals(close2, multi2.getEndOfBusinessDay());
        assertEquals(DateTimeUtils.HOUR * 6, multi2.getLOBD());
        assertEquals(DateTimeUtils.HOUR * 6, multi2.getLengthOfBusinessDay());
        assertTrue(multi2.isBusinessDay());
        assertTrue(multi2.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T10:00:00.000000000 NY")));
        assertTrue(multi2.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T10:15:00.000000000 NY")));
        assertTrue(multi2.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T11:00:00.000000000 NY")));
        assertFalse(multi2.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T11:10:00.000000000 NY")));
        assertTrue(multi2.isBusinessTime(DateTimeUtils.convertDateTime("2017-03-11T12:10:00.000000000 NY")));
        assertEquals(0L, multi2.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                multi2.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2,
                multi2.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2,
                multi2.businessTimeElapsed(DateTimeUtils.convertDateTime("2017-03-11T13:00:00.000000000 NY")));

    }
}
