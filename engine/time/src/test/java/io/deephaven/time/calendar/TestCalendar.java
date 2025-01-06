//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;

import java.time.*;
import java.util.ArrayList;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class TestCalendar extends BaseArrayTestCase {
    protected final String name = "TEST CALENDAR";
    protected final String description = "This is a test";
    protected final ZoneId timeZone = ZoneId.of("America/Los_Angeles");

    protected Calendar calendar;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        calendar = new Calendar(name, description, timeZone);
    }

    public void testGetters() {
        assertEquals(name, calendar.name());
        assertEquals(description, calendar.description());
        assertEquals(timeZone, calendar.timeZone());
    }

    public void testToString() {
        assertEquals("Calendar{name='TEST CALENDAR', description='This is a test', timeZone=America/Los_Angeles}",
                calendar.toString());
    }

    public void testDayOfWeek() {
        assertEquals(DateTimeUtils.dayOfWeek(calendar.calendarDate()), calendar.dayOfWeek());
        assertEquals(DayOfWeek.MONDAY, calendar.dayOfWeek("2020-03-02"));
        assertEquals(DayOfWeek.MONDAY, calendar.dayOfWeek(LocalDate.of(2020, 3, 2)));
        assertEquals(DayOfWeek.MONDAY, calendar.dayOfWeek(LocalDate.of(2020, 3, 2).atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(DayOfWeek.MONDAY,
                calendar.dayOfWeek(LocalDate.of(2020, 3, 2).atTime(1, 2, 3).atZone(timeZone).toInstant()));
        assertNull(calendar.dayOfWeek((LocalDate) null));
        assertNull(calendar.dayOfWeek((String) null));
        assertNull(calendar.dayOfWeek((Instant) null));
        assertNull(calendar.dayOfWeek((ZonedDateTime) null));

        assertEquals(DateTimeUtils.dayOfWeekValue(calendar.calendarDate()), calendar.dayOfWeekValue());
        assertEquals(1, calendar.dayOfWeekValue("2020-03-02"));
        assertEquals(1, calendar.dayOfWeekValue(LocalDate.of(2020, 3, 2)));
        assertEquals(1, calendar.dayOfWeekValue(LocalDate.of(2020, 3, 2).atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(1, calendar.dayOfWeekValue(LocalDate.of(2020, 3, 2).atTime(1, 2, 3).atZone(timeZone).toInstant()));
        assertEquals(NULL_INT, calendar.dayOfWeekValue((LocalDate) null));
        assertEquals(NULL_INT, calendar.dayOfWeekValue((String) null));
        assertEquals(NULL_INT, calendar.dayOfWeekValue((Instant) null));
        assertEquals(NULL_INT, calendar.dayOfWeekValue((ZonedDateTime) null));
    }

    public void testPlusDays() {
        final ZoneId timeZone2 = ZoneId.of("America/New_York");
        final LocalDate d = LocalDate.of(2023, 2, 3);
        final String s = "2023-02-03";
        final ZonedDateTime z = d.atTime(1, 24).atZone(timeZone2);
        final Instant i = d.atTime(1, 24).atZone(timeZone2).toInstant();

        assertEquals(d, calendar.plusDays(d, 0));
        assertEquals(d.toString(), calendar.plusDays(s, 0));
        assertEquals(z.withZoneSameInstant(timeZone), calendar.plusDays(z, 0));
        assertEquals(i, calendar.plusDays(i, 0));

        final LocalDate d2 = LocalDate.of(2023, 2, 5);
        final Instant i2 = d2.atTime(1, 24).atZone(timeZone2).toInstant();
        final ZonedDateTime z2 = d2.atTime(1, 24).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d2, calendar.plusDays(d, 2));
        assertEquals(d2.toString(), calendar.plusDays(s, 2));
        assertEquals(z2, calendar.plusDays(z, 2));
        assertEquals(i2, calendar.plusDays(i, 2));
        assertEquals(2 * DateTimeUtils.DAY, DateTimeUtils.minus(i2, i));
        assertEquals(2 * DateTimeUtils.DAY, DateTimeUtils.minus(z2, z));

        final LocalDate d3 = LocalDate.of(2023, 2, 1);
        final Instant i3 = d3.atTime(1, 24).atZone(timeZone2).toInstant();
        final ZonedDateTime z3 = d3.atTime(z.toLocalTime()).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d3, calendar.plusDays(d, -2));
        assertEquals(d3.toString(), calendar.plusDays(s, -2));
        assertEquals(z3, calendar.plusDays(z, -2));
        assertEquals(i3, calendar.plusDays(i, -2));
        assertEquals(-2 * DateTimeUtils.DAY, DateTimeUtils.minus(i3, i));
        assertEquals(-2 * DateTimeUtils.DAY, DateTimeUtils.minus(z3, z));

        assertNull(calendar.plusDays((LocalDate) null, 1));
        assertNull(calendar.plusDays((String) null, 1));
        assertNull(calendar.plusDays((Instant) null, 1));
        assertNull(calendar.plusDays((ZonedDateTime) null, 1));

        assertNull(calendar.plusDays(d, NULL_INT));
        assertNull(calendar.plusDays(s, NULL_INT));
        assertNull(calendar.plusDays(i, NULL_INT));
        assertNull(calendar.plusDays(z, NULL_INT));

        // Test Daylight Savings Time
        final ZonedDateTime zDST1 = ZonedDateTime.of(2023, 11, 4, 14, 1, 2, 3, timeZone).withZoneSameInstant(timeZone2);
        final ZonedDateTime zDST2 = ZonedDateTime.of(2023, 11, 5, 14, 1, 2, 3, timeZone);
        final Instant iDST1 = ZonedDateTime.of(2023, 11, 4, 14, 1, 2, 3, timeZone).toInstant();
        final Instant iDST2 = ZonedDateTime.of(2023, 11, 5, 14, 1, 2, 3, timeZone).toInstant();

        assertEquals(zDST2, calendar.plusDays(zDST1, 1));
        assertEquals(iDST2, calendar.plusDays(iDST1, 1));
        assertEquals(DateTimeUtils.DAY + DateTimeUtils.HOUR, DateTimeUtils.minus(zDST2, zDST1));
        assertEquals(DateTimeUtils.DAY + DateTimeUtils.HOUR, DateTimeUtils.minus(iDST2, iDST1));
    }

    public void testMinusDays() {
        final ZoneId timeZone2 = ZoneId.of("America/New_York");
        final LocalDate d = LocalDate.of(2023, 2, 3);
        final String s = "2023-02-03";
        final ZonedDateTime z = d.atTime(1, 24).atZone(timeZone2);
        final Instant i = d.atTime(1, 24).atZone(timeZone2).toInstant();

        assertEquals(d, calendar.minusDays(d, 0));
        assertEquals(d.toString(), calendar.minusDays(s, 0));
        assertEquals(z.withZoneSameInstant(timeZone), calendar.minusDays(z, 0));
        assertEquals(i, calendar.minusDays(i, 0));

        final LocalDate d2 = LocalDate.of(2023, 2, 1);
        final Instant i2 = d2.atTime(1, 24).atZone(timeZone2).toInstant();
        final ZonedDateTime z2 = d2.atTime(1, 24).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d2, calendar.minusDays(d, 2));
        assertEquals(d2.toString(), calendar.minusDays(s, 2));
        assertEquals(z2, calendar.minusDays(z, 2));
        assertEquals(i2, calendar.minusDays(i, 2));

        final LocalDate d3 = LocalDate.of(2023, 2, 5);
        final Instant i3 = d3.atTime(1, 24).atZone(timeZone2).toInstant();
        final ZonedDateTime z3 = d3.atTime(z.toLocalTime()).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d3, calendar.minusDays(d, -2));
        assertEquals(d3.toString(), calendar.minusDays(s, -2));
        assertEquals(z3, calendar.minusDays(z, -2));
        assertEquals(i3, calendar.minusDays(i, -2));

        assertNull(calendar.minusDays((LocalDate) null, 1));
        assertNull(calendar.minusDays((String) null, 1));
        assertNull(calendar.minusDays((Instant) null, 1));
        assertNull(calendar.minusDays((ZonedDateTime) null, 1));

        assertNull(calendar.minusDays(d, NULL_INT));
        assertNull(calendar.minusDays(s, NULL_INT));
        assertNull(calendar.minusDays(i, NULL_INT));
        assertNull(calendar.minusDays(z, NULL_INT));

        // Test Daylight Savings Time
        final ZonedDateTime zDST1 = ZonedDateTime.of(2023, 11, 4, 14, 1, 2, 3, timeZone);
        final ZonedDateTime zDST2 = ZonedDateTime.of(2023, 11, 5, 14, 1, 2, 3, timeZone).withZoneSameInstant(timeZone2);
        final Instant iDST1 = ZonedDateTime.of(2023, 11, 4, 14, 1, 2, 3, timeZone).toInstant();
        final Instant iDST2 = ZonedDateTime.of(2023, 11, 5, 14, 1, 2, 3, timeZone).toInstant();

        assertEquals(zDST1, calendar.minusDays(zDST2, 1));
        assertEquals(iDST1, calendar.minusDays(iDST2, 1));
        assertEquals(DateTimeUtils.DAY + DateTimeUtils.HOUR, DateTimeUtils.minus(zDST2, zDST1));
        assertEquals(DateTimeUtils.DAY + DateTimeUtils.HOUR, DateTimeUtils.minus(iDST2, iDST1));
    }

    public void testCurrentDate() {
        assertEquals(calendar.calendarDate(), calendar.calendarDate());
    }

    public void testFutureDate() {
        assertEquals(calendar.plusDays(calendar.calendarDate(), 3), calendar.futureDate(3));
        assertEquals(calendar.plusDays(calendar.calendarDate(), -3), calendar.futureDate(-3));
        assertNull(calendar.futureDate(NULL_INT));
    }

    public void testPastDate() {
        assertEquals(calendar.minusDays(calendar.calendarDate(), 3), calendar.pastDate(3));
        assertEquals(calendar.minusDays(calendar.calendarDate(), -3), calendar.pastDate(-3));
        assertNull(calendar.pastDate(NULL_INT));
    }

    public void testCalendarDates() {
        final LocalDate start = LocalDate.of(2023, 2, 3);
        final LocalDate middle = LocalDate.of(2023, 2, 4);
        final LocalDate end = LocalDate.of(2023, 2, 5);

        assertEquals(new LocalDate[] {start, middle, end}, calendar.calendarDates(start, end));
        assertEquals(new String[] {start.toString(), middle.toString(), end.toString()},
                calendar.calendarDates(start.toString(), end.toString()));
        assertEquals(new LocalDate[] {start, middle, end},
                calendar.calendarDates(start.atTime(1, 24).atZone(timeZone), end.atTime(1, 24).atZone(timeZone)));
        assertEquals(new LocalDate[] {start, middle, end}, calendar.calendarDates(
                start.atTime(1, 24).atZone(timeZone).toInstant(), end.atTime(1, 24).atZone(timeZone).toInstant()));

        assertEquals(new LocalDate[] {start, middle}, calendar.calendarDates(start, end, true, false));
        assertEquals(new String[] {start.toString(), middle.toString()},
                calendar.calendarDates(start.toString(), end.toString(), true, false));
        assertEquals(new LocalDate[] {start, middle}, calendar.calendarDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone), true, false));
        assertEquals(new LocalDate[] {start, middle},
                calendar.calendarDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                        end.atTime(1, 24).atZone(timeZone).toInstant(), true, false));

        assertEquals(new LocalDate[] {middle, end}, calendar.calendarDates(start, end, false, true));
        assertEquals(new String[] {middle.toString(), end.toString()},
                calendar.calendarDates(start.toString(), end.toString(), false, true));
        assertEquals(new LocalDate[] {middle, end}, calendar.calendarDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone), false, true));
        assertEquals(new LocalDate[] {middle, end},
                calendar.calendarDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                        end.atTime(1, 24).atZone(timeZone).toInstant(), false, true));

        assertEquals(new LocalDate[] {middle}, calendar.calendarDates(start, end, false, false));
        assertEquals(new String[] {middle.toString()},
                calendar.calendarDates(start.toString(), end.toString(), false, false));
        assertEquals(new LocalDate[] {middle}, calendar.calendarDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone), false, false));
        assertEquals(new LocalDate[] {middle}, calendar.calendarDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant(), false, false));

        assertNull(calendar.calendarDates(null, end, false, false));
        assertNull(calendar.calendarDates(start, null, false, false));
        assertNull(calendar.calendarDates(null, end.toString(), false, false));
        assertNull(calendar.calendarDates(start.toString(), null, false, false));
        assertNull(calendar.calendarDates(null, end.atTime(3, 15).atZone(timeZone).toInstant(), false, false));
        assertNull(calendar.calendarDates(start.atTime(3, 15).atZone(timeZone).toInstant(), null, false, false));
        assertNull(calendar.calendarDates(null, end.atTime(3, 15).atZone(timeZone), false, false));
        assertNull(calendar.calendarDates(start.atTime(3, 15).atZone(timeZone), null, false, false));

        // end before start
        assertEquals(new LocalDate[0], calendar.calendarDates(end, start));

        // long span of dates
        final LocalDate startLong = LocalDate.of(2019, 2, 1);
        final LocalDate endLong = LocalDate.of(2023, 12, 31);
        final ArrayList<LocalDate> targetLong = new ArrayList<>();

        for (LocalDate d = startLong; !d.isAfter(endLong); d = d.plusDays(1)) {
            targetLong.add(d);
        }

        assertEquals(targetLong.toArray(LocalDate[]::new), calendar.calendarDates(startLong, endLong));
    }

    public void testNumberCalendarDates() {
        final LocalDate start = LocalDate.of(2023, 2, 3);
        final LocalDate middle = LocalDate.of(2023, 2, 4);
        final LocalDate end = LocalDate.of(2023, 2, 5);

        assertEquals(3, calendar.numberCalendarDates(start, end));
        assertEquals(3, calendar.numberCalendarDates(start.toString(), end.toString()));
        assertEquals(3,
                calendar.numberCalendarDates(start.atTime(1, 24).atZone(timeZone), end.atTime(1, 24).atZone(timeZone)));
        assertEquals(3, calendar.numberCalendarDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant()));

        assertEquals(2, calendar.numberCalendarDates(start, end, true, false));
        assertEquals(2, calendar.numberCalendarDates(start.toString(), end.toString(), true, false));
        assertEquals(2, calendar.numberCalendarDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone), true, false));
        assertEquals(2, calendar.numberCalendarDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant(), true, false));

        assertEquals(2, calendar.numberCalendarDates(start, end, false, true));
        assertEquals(2, calendar.numberCalendarDates(start.toString(), end.toString(), false, true));
        assertEquals(2, calendar.numberCalendarDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone), false, true));
        assertEquals(2, calendar.numberCalendarDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant(), false, true));

        assertEquals(1, calendar.numberCalendarDates(start, end, false, false));
        assertEquals(1, calendar.numberCalendarDates(start.toString(), end.toString(), false, false));
        assertEquals(1, calendar.numberCalendarDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone), false, false));
        assertEquals(1, calendar.numberCalendarDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant(), false, false));

        assertEquals(NULL_INT, calendar.numberCalendarDates(null, end, false, false));
        assertEquals(NULL_INT, calendar.numberCalendarDates(start, null, false, false));
        assertEquals(NULL_INT, calendar.numberCalendarDates(null, end.toString(), false, false));
        assertEquals(NULL_INT, calendar.numberCalendarDates(start.toString(), null, false, false));
        assertEquals(NULL_INT,
                calendar.numberCalendarDates(null, end.atTime(3, 15).atZone(timeZone).toInstant(), false, false));
        assertEquals(NULL_INT,
                calendar.numberCalendarDates(start.atTime(3, 15).atZone(timeZone).toInstant(), null, false, false));
        assertEquals(NULL_INT, calendar.numberCalendarDates(null, end.atTime(3, 15).atZone(timeZone), false, false));
        assertEquals(NULL_INT, calendar.numberCalendarDates(start.atTime(3, 15).atZone(timeZone), null, false, false));
    }
}
