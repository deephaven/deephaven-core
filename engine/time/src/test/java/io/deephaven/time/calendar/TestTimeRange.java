//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;
import junit.framework.TestCase;

import java.time.*;
import java.util.Objects;

import static org.junit.Assert.assertNotEquals;

public class TestTimeRange extends BaseArrayTestCase {

    public void testTimeRangeInclusive() {
        final Instant open1 = DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY");
        final Instant close1 = DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY");

        try {
            new TimeRange<>(null, close1, true);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        try {
            new TimeRange<>(close1, null, true);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        try {
            new TimeRange<>(close1, open1, true);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("after"));
        }

        try {
            new TimeRange<>(open1, open1, true);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("same"));
        }

        TimeRange<Instant> period = new TimeRange<>(open1, close1, true);
        assertEquals(open1, period.start());
        assertEquals(close1, period.end());
        assertTrue(period.isInclusiveEnd());
        assertEquals(DateTimeUtils.HOUR + 1, period.nanos());
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR + 1), period.duration());

        assertTrue(period.contains(open1));
        assertTrue(period
                .contains(DateTimeUtils.epochNanosToInstant(DateTimeUtils.epochNanos(open1) + DateTimeUtils.MINUTE)));
        assertFalse(period
                .contains(DateTimeUtils.epochNanosToInstant(DateTimeUtils.epochNanos(open1) - DateTimeUtils.MINUTE)));
        assertTrue(period.contains(close1));
        assertTrue(period
                .contains(DateTimeUtils.epochNanosToInstant(DateTimeUtils.epochNanos(close1) - DateTimeUtils.MINUTE)));
        assertFalse(period
                .contains(DateTimeUtils.epochNanosToInstant(DateTimeUtils.epochNanos(close1) + DateTimeUtils.MINUTE)));
    }

    public void testTimeRangeExclusive() {
        final Instant open1 = DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY");
        final Instant close1 = DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY");

        try {
            new TimeRange<>(null, close1, false);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        try {
            new TimeRange<>(close1, null, false);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        try {
            new TimeRange<>(close1, open1, false);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("after"));
        }

        try {
            new TimeRange<>(open1, open1, false);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("same"));
        }

        TimeRange<Instant> period = new TimeRange<>(open1, close1, false);
        assertEquals(open1, period.start());
        assertEquals(close1, period.end());
        assertFalse(period.isInclusiveEnd());
        assertEquals(DateTimeUtils.HOUR, period.nanos());
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR), period.duration());

        assertTrue(period.contains(open1));
        assertTrue(period
                .contains(DateTimeUtils.epochNanosToInstant(DateTimeUtils.epochNanos(open1) + DateTimeUtils.MINUTE)));
        assertFalse(period
                .contains(DateTimeUtils.epochNanosToInstant(DateTimeUtils.epochNanos(open1) - DateTimeUtils.MINUTE)));
        assertTrue(period.contains(close1.minusNanos(1)));
        assertFalse(period.contains(close1));
        assertTrue(period
                .contains(DateTimeUtils.epochNanosToInstant(DateTimeUtils.epochNanos(close1) - DateTimeUtils.MINUTE)));
        assertFalse(period
                .contains(DateTimeUtils.epochNanosToInstant(DateTimeUtils.epochNanos(close1) + DateTimeUtils.MINUTE)));
    }

    public void testToInstantInclusive() {
        final LocalTime start = LocalTime.of(1, 2, 3);
        final LocalTime end = LocalTime.of(7, 8, 9);

        final TimeRange<LocalTime> local = new TimeRange<>(start, end, true);

        final LocalDate date = LocalDate.of(2017, 3, 11);
        final ZoneId timeZone = ZoneId.of("America/Los_Angeles");
        final Instant targetStart = date.atTime(start).atZone(timeZone).toInstant();
        final Instant targetEnd = date.atTime(end).atZone(timeZone).toInstant();

        final TimeRange<Instant> target = new TimeRange<>(targetStart, targetEnd, true);
        final TimeRange<Instant> rst = TimeRange.toInstant(local, date, timeZone);
        assertEquals(target, rst);
    }

    public void testToInstantExclusive() {
        final LocalTime start = LocalTime.of(1, 2, 3);
        final LocalTime end = LocalTime.of(7, 8, 9);

        final TimeRange<LocalTime> local = new TimeRange<>(start, end, false);

        final LocalDate date = LocalDate.of(2017, 3, 11);
        final ZoneId timeZone = ZoneId.of("America/Los_Angeles");
        final Instant targetStart = date.atTime(start).atZone(timeZone).toInstant();
        final Instant targetEnd = date.atTime(end).atZone(timeZone).toInstant();

        final TimeRange<Instant> target = new TimeRange<>(targetStart, targetEnd, false);
        final TimeRange<Instant> rst = TimeRange.toInstant(local, date, timeZone);
        assertEquals(target, rst);
    }

    public void testEqualsHashInclusive() {
        final LocalTime start = LocalTime.of(1, 2, 3);
        final LocalTime end = LocalTime.of(7, 8, 9);
        final TimeRange<LocalTime> p1 = new TimeRange<>(start, end, true);
        final TimeRange<LocalTime> p2 = new TimeRange<>(start, end, true);
        final TimeRange<LocalTime> p3 = new TimeRange<>(LocalTime.of(0, 1), end, true);
        final TimeRange<LocalTime> p4 = new TimeRange<>(start, LocalTime.of(8, 9), true);

        assertEquals(p1.hashCode(), Objects.hash(start, end, true));
        assertEquals(p1, p1);
        assertEquals(p1, p2);
        assertNotEquals(p1, p3);
        assertNotEquals(p1, p4);
    }

    public void testEqualsHashExclusive() {
        final LocalTime start = LocalTime.of(1, 2, 3);
        final LocalTime end = LocalTime.of(7, 8, 9);
        final TimeRange<LocalTime> p1 = new TimeRange<>(start, end, false);
        final TimeRange<LocalTime> p2 = new TimeRange<>(start, end, false);
        final TimeRange<LocalTime> p3 = new TimeRange<>(LocalTime.of(0, 1), end, false);
        final TimeRange<LocalTime> p4 = new TimeRange<>(start, LocalTime.of(8, 9), false);

        assertEquals(p1.hashCode(), Objects.hash(start, end, false));
        assertEquals(p1, p1);
        assertEquals(p1, p2);
        assertNotEquals(p1, p3);
        assertNotEquals(p1, p4);
    }

    public void testToStringInclusive() {
        final LocalTime start = LocalTime.of(1, 2, 3);
        final LocalTime end = LocalTime.of(7, 8, 9);
        final TimeRange<LocalTime> p1 = new TimeRange<>(start, end, true);
        assertEquals("TimeRange{start=01:02:03, end=07:08:09, inclusiveEnd=true}", p1.toString());
    }

    public void testToStringExclusive() {
        final LocalTime start = LocalTime.of(1, 2, 3);
        final LocalTime end = LocalTime.of(7, 8, 9);
        final TimeRange<LocalTime> p1 = new TimeRange<>(start, end, false);
        assertEquals("TimeRange{start=01:02:03, end=07:08:09, inclusiveEnd=false}", p1.toString());
    }

    public void testNanos() {
        final Instant t1 = DateTimeUtils.epochMillisToInstant(0);
        final Instant t2 = DateTimeUtils.epochMillisToInstant(1);

        final TimeRange<Instant> pInclusive = new TimeRange<>(t1, t2, true);
        final TimeRange<Instant> pExclusive = new TimeRange<>(t1, t2, false);

        final long nInclusive = pInclusive.nanos();
        final long nExclusive = pExclusive.nanos();

        assertNotEquals(nInclusive, nExclusive);
        assertEquals(DateTimeUtils.MILLI, nExclusive);
        assertEquals(DateTimeUtils.MILLI + 1, nInclusive);
    }
}
