/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;
import junit.framework.TestCase;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Objects;

import static org.junit.Assert.assertNotEquals;

public class TestBusinessPeriod extends BaseArrayTestCase {

    public void testBusinessPeriod() {
        final Instant open1 = DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY");
        final Instant close1 = DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY");

        try {
            new BusinessPeriod<>(null, close1);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        try {
            new BusinessPeriod<>(close1, null);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        try {
            new BusinessPeriod<>(close1, open1);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("after"));
        }

        BusinessPeriod<Instant> period = new BusinessPeriod<>(open1, close1);
        assertEquals(open1, period.start());
        assertEquals(close1, period.end());
        assertEquals(DateTimeUtils.HOUR, period.nanos());

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

    public void testToInstant() {
        final LocalTime start = LocalTime.of(1, 2, 3);
        final LocalTime end = LocalTime.of(7, 8, 9);

        final BusinessPeriod<LocalTime> local = new BusinessPeriod<>(start, end);

        final LocalDate date = LocalDate.of(2017, 3, 11);
        final ZoneId timeZone = ZoneId.of("America/Los_Angeles");
        final Instant targetStart = date.atTime(start).atZone(timeZone).toInstant();
        final Instant targetEnd = date.atTime(end).atZone(timeZone).toInstant();

        final BusinessPeriod<Instant> target = new BusinessPeriod<>(targetStart, targetEnd);
        final BusinessPeriod<Instant> rst = BusinessPeriod.toInstant(local, date, timeZone);
        assertEquals(target, rst);
    }

    public void testEqualsHash() {
        final LocalTime start = LocalTime.of(1, 2, 3);
        final LocalTime end = LocalTime.of(7, 8, 9);
        final BusinessPeriod<LocalTime> p1 = new BusinessPeriod<>(start, end);
        final BusinessPeriod<LocalTime> p2 = new BusinessPeriod<>(start, end);
        final BusinessPeriod<LocalTime> p3 = new BusinessPeriod<>(LocalTime.of(0, 1), end);
        final BusinessPeriod<LocalTime> p4 = new BusinessPeriod<>(start, LocalTime.of(8, 9));

        assertEquals(p1.hashCode(), Objects.hash(start, end, p1.nanos()));
        assertEquals(p1, p1);
        assertEquals(p1, p2);
        assertNotEquals(p1, p3);
        assertNotEquals(p1, p4);
    }

    public void testToString() {
        final LocalTime start = LocalTime.of(1, 2, 3);
        final LocalTime end = LocalTime.of(7, 8, 9);
        final BusinessPeriod<LocalTime> p1 = new BusinessPeriod<>(start, end);
        assertEquals("BusinessPeriod{start=01:02:03, end=07:08:09}", p1.toString());
    }
}
