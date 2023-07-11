/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.assertNotEquals;

public class TestBusinessSchedule extends BaseArrayTestCase {
    final Instant open1 = DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY");
    final Instant close1 = DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY");
    final BusinessPeriod<Instant> period1 = new BusinessPeriod<>(open1, close1);
    final Instant open2 = DateTimeUtils.parseInstant("2017-03-11T12:00:00.000000000 NY");
    final Instant close2 = DateTimeUtils.parseInstant("2017-03-11T17:00:00.000000000 NY");
    final BusinessPeriod<Instant> period2 = new BusinessPeriod<>(open2, close2);

    public void testEmpty() {
        final BusinessSchedule<Instant> empty = new BusinessSchedule<>();
        assertEquals(new BusinessPeriod[0], empty.periods());
        assertNull(empty.businessStart());
        assertNull(empty.businessStart());
        assertNull(empty.businessEnd());
        assertNull(empty.businessEnd());
        assertEquals(0L, empty.businessNanos());
        assertEquals(0L, empty.businessNanos());
        assertFalse(empty.isBusinessDay());
        assertFalse(empty.isBusinessTime(open1));
        assertFalse(empty.isBusinessTime(close1));
        assertEquals(0L, empty.businessNanosElapsed(open1));
        assertEquals(0L, empty.businessNanosElapsed(close1));
        assertEquals(0L, empty.businessNanosRemaining(open1));
        assertEquals(0L, empty.businessNanosRemaining(close1));
    }

    public void testSinglePeriod() {
        //noinspection unchecked,rawtypes
        final BusinessSchedule<Instant> single = new BusinessSchedule<>(new BusinessPeriod[]{period1});
        assertEquals(new BusinessPeriod[] {period1}, single.periods());
        assertEquals(open1, single.businessStart());
        assertEquals(open1, single.businessStart());
        assertEquals(close1, single.businessEnd());
        assertEquals(close1, single.businessEnd());
        assertEquals(DateTimeUtils.HOUR, single.businessNanos());
        assertEquals(DateTimeUtils.HOUR, single.businessNanos());
        assertTrue(single.isBusinessDay());
        assertTrue(single.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY")));
        assertTrue(single.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:15:00.000000000 NY")));
        assertTrue(single.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY")));
        assertFalse(single.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:10:00.000000000 NY")));
        assertEquals(0L, single.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                single.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR,
                single.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                single.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(0L,
                single.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
    }

    public void testMultiPeriod() {
        //noinspection unchecked,rawtypes
        final BusinessSchedule<Instant> multi = new BusinessSchedule<>(new BusinessPeriod[]{period1, period2});
        assertEquals(new BusinessPeriod[] {period1, period2}, multi.periods());
        assertEquals(open1, multi.businessStart());
        assertEquals(open1, multi.businessStart());
        assertEquals(close2, multi.businessEnd());
        assertEquals(close2, multi.businessEnd());
        assertEquals(DateTimeUtils.HOUR * 6, multi.businessNanos());
        assertEquals(DateTimeUtils.HOUR * 6, multi.businessNanos());
        assertTrue(multi.isBusinessDay());
        assertTrue(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY")));
        assertTrue(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:15:00.000000000 NY")));
        assertTrue(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY")));
        assertFalse(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:10:00.000000000 NY")));
        assertTrue(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T12:10:00.000000000 NY")));
        assertEquals(0L, multi.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                multi.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2,
                multi.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2,
                multi.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR*6, multi.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR*5+DateTimeUtils.MINUTE * 30,
                multi.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 4,
                multi.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));


        //noinspection unchecked,rawtypes
        final BusinessSchedule<Instant> multi2 = new BusinessSchedule<>(new BusinessPeriod[]{period2, period1});
        assertEquals(new BusinessPeriod[] {period1, period2}, multi2.periods());
        assertEquals(open1, multi2.businessStart());
        assertEquals(open1, multi2.businessStart());
        assertEquals(close2, multi2.businessEnd());
        assertEquals(close2, multi2.businessEnd());
        assertEquals(DateTimeUtils.HOUR * 6, multi2.businessNanos());
        assertEquals(DateTimeUtils.HOUR * 6, multi2.businessNanos());
        assertTrue(multi2.isBusinessDay());
        assertTrue(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY")));
        assertTrue(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:15:00.000000000 NY")));
        assertTrue(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY")));
        assertFalse(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:10:00.000000000 NY")));
        assertTrue(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T12:10:00.000000000 NY")));
        assertEquals(0L, multi2.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                multi2.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2,
                multi2.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2,
                multi2.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
    }

    public void testPeriodsOverlap() {
        try {
            //noinspection unchecked,rawtypes
            new BusinessSchedule<>(new BusinessPeriod[]{period1, period1});
            fail("Should have thrown an exception");
        }catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("overlap"));
        }
    }

    public void testToInstant() {
        final BusinessPeriod<LocalTime> p1 = new BusinessPeriod<>(LocalTime.of(1,2), LocalTime.of(3,4));
        final BusinessPeriod<LocalTime> p2 = new BusinessPeriod<>(LocalTime.of(5,6), LocalTime.of(7,8));

        //noinspection unchecked,rawtypes
        final BusinessSchedule<LocalTime> local = new BusinessSchedule<>(new BusinessPeriod[]{p1,p2});
        final LocalDate date = LocalDate.of(2017,3,11);
        final ZoneId timeZone = ZoneId.of("America/Los_Angeles");

        //noinspection unchecked,rawtypes
        final BusinessSchedule<Instant> target = new BusinessSchedule<>(new BusinessPeriod[]{BusinessPeriod.toInstant(p1, date, timeZone), BusinessPeriod.toInstant(p2,date,timeZone)});
        final BusinessSchedule<Instant> actual = BusinessSchedule.toInstant(local, date, timeZone);
        assertEquals(target, actual);
    }

    public void testEqualsHash() {
        //noinspection unchecked,rawtypes
        final BusinessSchedule<Instant> multi = new BusinessSchedule<>(new BusinessPeriod[]{period1, period2});
        assertEquals(new BusinessPeriod[] {period1, period2}, multi.periods());

        int hashTarget = 31 * Objects.hash(multi.businessStart(), multi.businessEnd(), multi.businessNanos()) + Arrays.hashCode(multi.periods());
        assertEquals(hashTarget, multi.hashCode());

        //noinspection unchecked,rawtypes
        final BusinessSchedule<Instant> multi2 = new BusinessSchedule<>(new BusinessPeriod[]{period1, period2});
        //noinspection unchecked,rawtypes
        final BusinessSchedule<Instant> multi3 = new BusinessSchedule<>(new BusinessPeriod[]{period1, new BusinessPeriod<>(open2, DateTimeUtils.parseInstant("2017-03-11T17:01:00.000000000 NY"))});
        assertEquals(multi, multi);
        assertEquals(multi, multi2);
        assertNotEquals(multi, multi3);
        assertNotEquals(multi2, multi3);
    }
}
