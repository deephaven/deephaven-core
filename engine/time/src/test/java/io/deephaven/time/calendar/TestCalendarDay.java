//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;

import java.time.*;
import java.util.Arrays;
import java.util.List;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestCalendarDay extends BaseArrayTestCase {
    private final Instant open1 = DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY");
    private final Instant close1 = DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY");
    private final TimeRange<Instant> period1 = new TimeRange<>(open1, close1, true);
    private final Instant open2 = DateTimeUtils.parseInstant("2017-03-11T12:00:00.000000000 NY");
    private final Instant close2 = DateTimeUtils.parseInstant("2017-03-11T17:00:00.000000000 NY");
    private final TimeRange<Instant> period2 = new TimeRange<>(open2, close2, true);

    public void testEmpty() {
        final CalendarDay<Instant> empty = new CalendarDay<>();
        assertEquals(List.of(), empty.businessTimeRanges());
        assertNull(empty.businessStart());
        assertNull(empty.businessStart());
        assertNull(empty.businessEnd());
        assertNull(empty.businessEnd());
        assertTrue(empty.isInclusiveEnd());
        assertEquals(0L, empty.businessNanos());
        assertEquals(Duration.ofNanos(0), empty.businessDuration());
        assertEquals(0L, empty.businessNanos());
        assertEquals(Duration.ofNanos(0), empty.businessDuration());
        assertFalse(empty.isBusinessDay());
        assertFalse(empty.isBusinessTime(open1));
        assertFalse(empty.isBusinessTime(close1));
        assertEquals(0L, empty.businessNanosElapsed(open1));
        assertEquals(Duration.ofNanos(0), empty.businessDurationElapsed(open1));
        assertEquals(0L, empty.businessNanosElapsed(close1));
        assertEquals(Duration.ofNanos(0), empty.businessDurationElapsed(close1));
        assertEquals(0L, empty.businessNanosRemaining(open1));
        assertEquals(Duration.ofNanos(0), empty.businessDurationRemaining(open1));
        assertEquals(0L, empty.businessNanosRemaining(close1));
        assertEquals(Duration.ofNanos(0), empty.businessDurationRemaining(close1));

        assertEquals(NULL_LONG, empty.businessNanosElapsed(null));
        assertEquals(NULL_LONG, empty.businessNanosRemaining(null));
        assertNull(empty.businessDurationElapsed(null));
        assertNull(empty.businessDurationRemaining(null));
        assertFalse(empty.isBusinessTime(null));
    }

    public void testSinglePeriod() {
        final CalendarDay<Instant> single = new CalendarDay<>(new TimeRange[] {period1});
        assertEquals(List.of(period1), single.businessTimeRanges());
        assertEquals(open1, single.businessStart());
        assertEquals(open1, single.businessStart());
        assertEquals(close1, single.businessEnd());
        assertEquals(close1, single.businessEnd());
        assertTrue(single.isInclusiveEnd());
        assertEquals(DateTimeUtils.HOUR + 1, single.businessNanos());
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR + 1), single.businessDuration());
        assertEquals(DateTimeUtils.HOUR + 1, single.businessNanos());
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR + 1), single.businessDuration());
        assertTrue(single.isBusinessDay());
        assertTrue(single.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY")));
        assertTrue(single.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:15:00.000000000 NY")));
        assertTrue(single.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY")));
        assertFalse(single.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:10:00.000000000 NY")));
        assertEquals(0L, single.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(0),
                single.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                single.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.MINUTE * 30),
                single.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR + 1,
                single.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR + 1),
                single.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30 + 1,
                single.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.MINUTE * 30 + 1),
                single.businessDurationRemaining(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(0L,
                single.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(0),
                single.businessDurationRemaining(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));

        assertEquals(NULL_LONG, single.businessNanosElapsed(null));
        assertEquals(NULL_LONG, single.businessNanosRemaining(null));
        assertNull(single.businessDurationElapsed(null));
        assertNull(single.businessDurationRemaining(null));
        assertFalse(single.isBusinessTime(null));
    }

    public void testMultiPeriod() {
        final CalendarDay<Instant> multi = new CalendarDay<>(new TimeRange[] {period1, period2});
        assertEquals(List.of(period1, period2), multi.businessTimeRanges());
        assertEquals(open1, multi.businessStart());
        assertEquals(open1, multi.businessStart());
        assertEquals(close2, multi.businessEnd());
        assertEquals(close2, multi.businessEnd());
        assertTrue(multi.isInclusiveEnd());
        assertEquals(DateTimeUtils.HOUR * 6 + 2, multi.businessNanos());
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 6 + 2), multi.businessDuration());
        assertEquals(DateTimeUtils.HOUR * 6 + 2, multi.businessNanos());
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 6 + 2), multi.businessDuration());
        assertTrue(multi.isBusinessDay());
        assertTrue(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY")));
        assertTrue(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:15:00.000000000 NY")));
        assertTrue(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY")));
        assertFalse(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:10:00.000000000 NY")));
        assertTrue(multi.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T12:10:00.000000000 NY")));
        assertEquals(0L, multi.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(0L),
                multi.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                multi.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.MINUTE * 30),
                multi.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2 + 1,
                multi.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 2 + 1),
                multi.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2 + 1,
                multi.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 2 + 1),
                multi.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 6 + 2,
                multi.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 6 + 2),
                multi.businessDurationRemaining(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 5 + DateTimeUtils.MINUTE * 30 + 2,
                multi.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 5 + DateTimeUtils.MINUTE * 30 + 2),
                multi.businessDurationRemaining(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 4 + 1,
                multi.businessNanosRemaining(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 4 + 1),
                multi.businessDurationRemaining(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));

        assertEquals(NULL_LONG, multi.businessNanosElapsed(null));
        assertEquals(NULL_LONG, multi.businessNanosRemaining(null));
        assertNull(multi.businessDurationElapsed(null));
        assertNull(multi.businessDurationRemaining(null));
        assertFalse(multi.isBusinessTime(null));

        final CalendarDay<Instant> multi2 = new CalendarDay<>(new TimeRange[] {period2, period1});
        assertEquals(List.of(period1, period2), multi2.businessTimeRanges());
        assertEquals(open1, multi2.businessStart());
        assertEquals(open1, multi2.businessStart());
        assertEquals(close2, multi2.businessEnd());
        assertEquals(close2, multi2.businessEnd());
        assertTrue(multi2.isInclusiveEnd());
        assertEquals(DateTimeUtils.HOUR * 6 + 2, multi2.businessNanos());
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 6 + 2), multi2.businessDuration());
        assertEquals(DateTimeUtils.HOUR * 6 + 2, multi2.businessNanos());
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 6 + 2), multi2.businessDuration());
        assertTrue(multi2.isBusinessDay());
        assertTrue(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:00:00.000000000 NY")));
        assertTrue(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T10:15:00.000000000 NY")));
        assertTrue(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:00:00.000000000 NY")));
        assertFalse(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T11:10:00.000000000 NY")));
        assertTrue(multi2.isBusinessTime(DateTimeUtils.parseInstant("2017-03-11T12:10:00.000000000 NY")));
        assertEquals(0L, multi2.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(0),
                multi2.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.MINUTE * 30,
                multi2.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.MINUTE * 30),
                multi2.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T10:30:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2 + 1,
                multi2.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 2 + 1),
                multi2.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(DateTimeUtils.HOUR * 2 + 1,
                multi2.businessNanosElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));
        assertEquals(Duration.ofNanos(DateTimeUtils.HOUR * 2 + 1),
                multi2.businessDurationElapsed(DateTimeUtils.parseInstant("2017-03-11T13:00:00.000000000 NY")));

        assertEquals(NULL_LONG, multi2.businessNanosElapsed(null));
        assertEquals(NULL_LONG, multi2.businessNanosRemaining(null));
        assertNull(multi2.businessDurationElapsed(null));
        assertNull(multi2.businessDurationRemaining(null));
        assertFalse(multi2.isBusinessTime(null));
    }

    public void testPeriodsOverlap() {
        try {
            new CalendarDay<>(new TimeRange[] {period1, period1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("overlap"));
        }
    }

    public void testToInstant() {
        final TimeRange<LocalTime> p1 = new TimeRange<>(LocalTime.of(1, 2), LocalTime.of(3, 4), true);
        final TimeRange<LocalTime> p2 = new TimeRange<>(LocalTime.of(5, 6), LocalTime.of(7, 8), true);

        final CalendarDay<LocalTime> local = new CalendarDay<>(new TimeRange[] {p1, p2});
        final LocalDate date = LocalDate.of(2017, 3, 11);
        final ZoneId timeZone = ZoneId.of("America/Los_Angeles");

        final CalendarDay<Instant> target = new CalendarDay<>(new TimeRange[] {
                TimeRange.toInstant(p1, date, timeZone), TimeRange.toInstant(p2, date, timeZone)});
        final CalendarDay<Instant> actual = CalendarDay.toInstant(local, date, timeZone);
        assertEquals(target, actual);
    }

    public void testEqualsHash() {
        final CalendarDay<Instant> multi = new CalendarDay<>(new TimeRange[] {period1, period2});
        assertEquals(List.of(period1, period2), multi.businessTimeRanges());

        int hashTarget = Arrays.hashCode(multi.businessTimeRanges().toArray());
        assertEquals(hashTarget, multi.hashCode());

        final CalendarDay<Instant> multi2 = new CalendarDay<>(new TimeRange[] {period1, period2});
        final CalendarDay<Instant> multi3 = new CalendarDay<>(new TimeRange[] {period1,
                new TimeRange<>(open2, DateTimeUtils.parseInstant("2017-03-11T17:01:00.000000000 NY"), true)});
        assertEquals(multi, multi);
        assertEquals(multi, multi2);
        assertNotEquals(multi, multi3);
        assertNotEquals(multi2, multi3);
    }

    public void testToString() {
        final CalendarDay<Instant> multi = new CalendarDay<>(new TimeRange[] {period1, period2});
        assertEquals(
                "CalendarDay{businessTimeRanges=[TimeRange{start=2017-03-11T15:00:00Z, end=2017-03-11T16:00:00Z, inclusiveEnd=true}, TimeRange{start=2017-03-11T17:00:00Z, end=2017-03-11T22:00:00Z, inclusiveEnd=true}]}",
                multi.toString());
    }
}
