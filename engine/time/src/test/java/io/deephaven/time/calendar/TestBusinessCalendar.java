//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.time.DateTimeUtils;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static io.deephaven.util.QueryConstants.*;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBusinessCalendar extends TestCalendar {
    private final LocalDate firstValidDate = LocalDate.of(2000, 1, 1);
    private final LocalDate lastValidDate = LocalDate.of(2050, 12, 31);
    private final TimeRange<LocalTime> period = new TimeRange<>(LocalTime.of(9, 0), LocalTime.of(12, 15), true);
    private final TimeRange<LocalTime> periodHalf = new TimeRange<>(LocalTime.of(9, 0), LocalTime.of(11, 7), true);
    private final CalendarDay<LocalTime> schedule = new CalendarDay<>(new TimeRange[] {period});
    private final Set<DayOfWeek> weekendDays = Set.of(DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY);
    private final LocalDate holidayDate1 = LocalDate.of(2023, 7, 4);
    private final LocalDate holidayDate2 = LocalDate.of(2023, 12, 25);
    private final CalendarDay<Instant> holiday = new CalendarDay<>();
    private final LocalDate halfDayDate = LocalDate.of(2023, 7, 6);
    private final CalendarDay<Instant> halfDay =
            new CalendarDay<>(new TimeRange[] {TimeRange.toInstant(periodHalf, halfDayDate, timeZone)});

    private Map<LocalDate, CalendarDay<Instant>> holidays;
    private BusinessCalendar bCalendar;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        holidays = new HashMap<>();
        holidays.put(holidayDate1, holiday);
        holidays.put(holidayDate2, holiday);
        holidays.put(halfDayDate, halfDay);

        bCalendar = new BusinessCalendar(name, description, timeZone, firstValidDate, lastValidDate, schedule,
                weekendDays, holidays);
        calendar = bCalendar;
    }

    public void testSchedulesCacheKeys() {
        final int y = 2023;
        final int m = 7;
        final int d = 11;
        final LocalDate ld = LocalDate.of(y, m, d);

        final int key = BusinessCalendar.schedulesCacheKeyFromDate(ld);
        assertEquals(key, y * 10000 + m * 100 + d);
        assertEquals(ld, BusinessCalendar.schedulesCacheDateFromKey(key));
    }

    public void testBusinessGetters() {
        assertEquals(schedule, bCalendar.standardBusinessDay());
        assertEquals(schedule.businessNanos(), bCalendar.standardBusinessNanos());
        assertEquals(schedule.businessDuration(), bCalendar.standardBusinessDuration());
        assertEquals(holidays, bCalendar.holidays());
        assertEquals(firstValidDate, bCalendar.firstValidDate());
        assertEquals(lastValidDate, bCalendar.lastValidDate());
        assertEquals(weekendDays, bCalendar.weekendDays());
    }

    public void testCalendarDay() {
        assertEquals(holiday, bCalendar.calendarDay(holidayDate1));
        assertEquals(holiday, bCalendar.calendarDay(holidayDate2));
        assertEquals(halfDay, bCalendar.calendarDay(halfDayDate));

        // TUES - Weekday
        LocalDate date = LocalDate.of(2023, 7, 11);
        assertEquals(CalendarDay.toInstant(schedule, date, timeZone), bCalendar.calendarDay(date));
        assertEquals(CalendarDay.toInstant(schedule, date, timeZone), bCalendar.calendarDay(date.toString()));
        assertEquals(CalendarDay.toInstant(schedule, date, timeZone),
                bCalendar.calendarDay(date.atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(CalendarDay.toInstant(schedule, date, timeZone),
                bCalendar.calendarDay(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // WED - Weekend
        date = LocalDate.of(2023, 7, 12);
        assertEquals(CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone),
                bCalendar.calendarDay(date));
        assertEquals(CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone),
                bCalendar.calendarDay(date.toString()));
        assertEquals(CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone),
                bCalendar.calendarDay(date.atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone),
                bCalendar.calendarDay(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // THURS - Weekend
        date = LocalDate.of(2023, 7, 13);
        assertEquals(CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone),
                bCalendar.calendarDay(date));
        assertEquals(CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone),
                bCalendar.calendarDay(date.toString()));
        assertEquals(CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone),
                bCalendar.calendarDay(date.atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone),
                bCalendar.calendarDay(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // FRI - Weekday
        date = LocalDate.of(2023, 7, 14);
        assertEquals(CalendarDay.toInstant(schedule, date, timeZone), bCalendar.calendarDay(date));
        assertEquals(CalendarDay.toInstant(schedule, date, timeZone), bCalendar.calendarDay(date.toString()));
        assertEquals(CalendarDay.toInstant(schedule, date, timeZone),
                bCalendar.calendarDay(date.atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(CalendarDay.toInstant(schedule, date, timeZone),
                bCalendar.calendarDay(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // Current date
        assertEquals(bCalendar.calendarDay(bCalendar.calendarDate()), bCalendar.calendarDay());

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            assertNotNull(bCalendar.calendarDay(d));
        }

        try {
            bCalendar.calendarDay(firstValidDate.minusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        try {
            bCalendar.calendarDay(lastValidDate.plusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        assertNull(bCalendar.calendarDay((LocalDate) null));
        assertNull(bCalendar.calendarDay((String) null));
        assertNull(bCalendar.calendarDay((ZonedDateTime) null));
        assertNull(bCalendar.calendarDay((Instant) null));
    }

    public void testIsBusinessDay() {
        assertFalse(bCalendar.isBusinessDay(holidayDate1));
        assertFalse(bCalendar.isBusinessDay(holidayDate2));
        assertTrue(bCalendar.isBusinessDay(halfDayDate));

        // TUES - Weekday
        LocalDate date = LocalDate.of(2023, 7, 11);
        assertTrue(bCalendar.isBusinessDay(date));
        assertTrue(bCalendar.isBusinessDay(date.toString()));
        assertTrue(bCalendar.isBusinessDay(date.atTime(1, 2, 3).atZone(timeZone)));
        assertTrue(bCalendar.isBusinessDay(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // WED - Weekend
        date = LocalDate.of(2023, 7, 12);
        assertFalse(bCalendar.isBusinessDay(date));
        assertFalse(bCalendar.isBusinessDay(date.toString()));
        assertFalse(bCalendar.isBusinessDay(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bCalendar.isBusinessDay(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // THURS - Weekend
        date = LocalDate.of(2023, 7, 13);
        assertFalse(bCalendar.isBusinessDay(date));
        assertFalse(bCalendar.isBusinessDay(date.toString()));
        assertFalse(bCalendar.isBusinessDay(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bCalendar.isBusinessDay(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // FRI - Weekday
        date = LocalDate.of(2023, 7, 14);
        assertTrue(bCalendar.isBusinessDay(date));
        assertTrue(bCalendar.isBusinessDay(date.toString()));
        assertTrue(bCalendar.isBusinessDay(date.atTime(1, 2, 3).atZone(timeZone)));
        assertTrue(bCalendar.isBusinessDay(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // Current date
        assertEquals(bCalendar.isBusinessDay(bCalendar.calendarDate()), bCalendar.isBusinessDay());

        // DayOfWeek

        for (DayOfWeek dow : DayOfWeek.values()) {
            assertEquals(!weekendDays.contains(dow), bCalendar.isBusinessDay(dow));
        }

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            bCalendar.isBusinessDay(d);
        }

        try {
            bCalendar.isBusinessDay(firstValidDate.minusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        try {
            bCalendar.isBusinessDay(lastValidDate.plusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        assertFalse(bCalendar.isBusinessDay((LocalDate) null));
        assertFalse(bCalendar.isBusinessDay((String) null));
        assertFalse(bCalendar.isBusinessDay((ZonedDateTime) null));
        assertFalse(bCalendar.isBusinessDay((Instant) null));
        assertFalse(bCalendar.isBusinessDay((DayOfWeek) null));
    }

    public void testIsLastBusinessDayOfMonth() {
        LocalDate date = LocalDate.of(2023, 7, 31);
        assertTrue(bCalendar.isLastBusinessDayOfMonth(date));
        assertTrue(bCalendar.isLastBusinessDayOfMonth(date.toString()));
        assertTrue(bCalendar.isLastBusinessDayOfMonth(date.atTime(1, 2, 3).atZone(timeZone)));
        assertTrue(bCalendar.isLastBusinessDayOfMonth(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // THURS - Weekend
        date = LocalDate.of(2023, 8, 31);
        assertFalse(bCalendar.isLastBusinessDayOfMonth(date));
        assertFalse(bCalendar.isLastBusinessDayOfMonth(date.toString()));
        assertFalse(bCalendar.isLastBusinessDayOfMonth(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bCalendar.isLastBusinessDayOfMonth(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // WED - Weekend
        date = LocalDate.of(2023, 8, 30);
        assertFalse(bCalendar.isLastBusinessDayOfMonth(date));
        assertFalse(bCalendar.isLastBusinessDayOfMonth(date.toString()));
        assertFalse(bCalendar.isLastBusinessDayOfMonth(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bCalendar.isLastBusinessDayOfMonth(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // TUES - Weekday
        date = LocalDate.of(2023, 8, 29);
        assertTrue(bCalendar.isLastBusinessDayOfMonth(date));
        assertTrue(bCalendar.isLastBusinessDayOfMonth(date.toString()));
        assertTrue(bCalendar.isLastBusinessDayOfMonth(date.atTime(1, 2, 3).atZone(timeZone)));
        assertTrue(bCalendar.isLastBusinessDayOfMonth(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // Current date
        assertEquals(bCalendar.isLastBusinessDayOfMonth(bCalendar.calendarDate()),
                bCalendar.isLastBusinessDayOfMonth());

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; d.isBefore(lastValidDate); d = d.plusDays(1)) {
            bCalendar.isLastBusinessDayOfMonth(d);
        }

        try {
            bCalendar.isLastBusinessDayOfMonth(firstValidDate.minusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfMonth(lastValidDate.plusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        assertFalse(bCalendar.isLastBusinessDayOfMonth((LocalDate) null));
        assertFalse(bCalendar.isLastBusinessDayOfMonth((String) null));
        assertFalse(bCalendar.isLastBusinessDayOfMonth((ZonedDateTime) null));
        assertFalse(bCalendar.isLastBusinessDayOfMonth((Instant) null));
    }

    public void testIsLastBusinessDayOfWeek() {
        // FRI
        LocalDate date = LocalDate.of(2023, 7, 28);
        assertFalse(bCalendar.isLastBusinessDayOfWeek(date));
        assertFalse(bCalendar.isLastBusinessDayOfWeek(date.toString()));
        assertFalse(bCalendar.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bCalendar.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // SAT
        date = LocalDate.of(2023, 7, 29);
        assertFalse(bCalendar.isLastBusinessDayOfWeek(date));
        assertFalse(bCalendar.isLastBusinessDayOfWeek(date.toString()));
        assertFalse(bCalendar.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bCalendar.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // SUN
        date = LocalDate.of(2023, 7, 30);
        assertTrue(bCalendar.isLastBusinessDayOfWeek(date));
        assertTrue(bCalendar.isLastBusinessDayOfWeek(date.toString()));
        assertTrue(bCalendar.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone)));
        assertTrue(bCalendar.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        final Set<DayOfWeek> wd = Set.of(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY);
        final BusinessCalendar bc = new BusinessCalendar(name, description, timeZone, firstValidDate, lastValidDate,
                schedule, wd, holidays);

        // FRI
        date = LocalDate.of(2023, 7, 28);
        assertTrue(bc.isLastBusinessDayOfWeek(date));
        assertTrue(bc.isLastBusinessDayOfWeek(date.toString()));
        assertTrue(bc.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone)));
        assertTrue(bc.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // SAT
        date = LocalDate.of(2023, 7, 29);
        assertFalse(bc.isLastBusinessDayOfWeek(date));
        assertFalse(bc.isLastBusinessDayOfWeek(date.toString()));
        assertFalse(bc.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bc.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // SUN
        date = LocalDate.of(2023, 7, 30);
        assertFalse(bc.isLastBusinessDayOfWeek(date));
        assertFalse(bc.isLastBusinessDayOfWeek(date.toString()));
        assertFalse(bc.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bc.isLastBusinessDayOfWeek(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // Current date
        assertEquals(bCalendar.isLastBusinessDayOfWeek(bCalendar.calendarDate()), bCalendar.isLastBusinessDayOfWeek());

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; d.isBefore(lastValidDate); d = d.plusDays(1)) {
            bCalendar.isLastBusinessDayOfWeek(d);
        }

        try {
            bCalendar.isLastBusinessDayOfWeek(firstValidDate.minusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfWeek(lastValidDate.plusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        assertFalse(bCalendar.isLastBusinessDayOfWeek((LocalDate) null));
        assertFalse(bCalendar.isLastBusinessDayOfWeek((String) null));
        assertFalse(bCalendar.isLastBusinessDayOfWeek((ZonedDateTime) null));
        assertFalse(bCalendar.isLastBusinessDayOfWeek((Instant) null));
    }

    public void testIsLastBusinessDayOfYear() {
        LocalDate date = LocalDate.of(2023, 12, 29);
        assertFalse(bCalendar.isLastBusinessDayOfYear(date));
        assertFalse(bCalendar.isLastBusinessDayOfYear(date.toString()));
        assertFalse(bCalendar.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bCalendar.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        date = LocalDate.of(2023, 12, 30);
        assertFalse(bCalendar.isLastBusinessDayOfYear(date));
        assertFalse(bCalendar.isLastBusinessDayOfYear(date.toString()));
        assertFalse(bCalendar.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bCalendar.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        date = LocalDate.of(2023, 12, 31);
        assertTrue(bCalendar.isLastBusinessDayOfYear(date));
        assertTrue(bCalendar.isLastBusinessDayOfYear(date.toString()));
        assertTrue(bCalendar.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone)));
        assertTrue(bCalendar.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        final Set<DayOfWeek> wd = Set.of(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY);
        final BusinessCalendar bc = new BusinessCalendar(name, description, timeZone, firstValidDate, lastValidDate,
                schedule, wd, holidays);

        date = LocalDate.of(2023, 12, 29);
        assertTrue(bc.isLastBusinessDayOfYear(date));
        assertTrue(bc.isLastBusinessDayOfYear(date.toString()));
        assertTrue(bc.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone)));
        assertTrue(bc.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        date = LocalDate.of(2023, 12, 30);
        assertFalse(bc.isLastBusinessDayOfYear(date));
        assertFalse(bc.isLastBusinessDayOfYear(date.toString()));
        assertFalse(bc.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bc.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        date = LocalDate.of(2023, 12, 31);
        assertFalse(bc.isLastBusinessDayOfYear(date));
        assertFalse(bc.isLastBusinessDayOfYear(date.toString()));
        assertFalse(bc.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone)));
        assertFalse(bc.isLastBusinessDayOfYear(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // Current date
        assertEquals(bCalendar.isLastBusinessDayOfYear(bCalendar.calendarDate()), bCalendar.isLastBusinessDayOfYear());

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; d.isBefore(lastValidDate); d = d.plusDays(1)) {
            bCalendar.isLastBusinessDayOfYear(d);
        }

        try {
            bCalendar.isLastBusinessDayOfYear(firstValidDate.minusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfYear(lastValidDate.plusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        assertFalse(bCalendar.isLastBusinessDayOfYear((LocalDate) null));
        assertFalse(bCalendar.isLastBusinessDayOfYear((String) null));
        assertFalse(bCalendar.isLastBusinessDayOfYear((ZonedDateTime) null));
        assertFalse(bCalendar.isLastBusinessDayOfYear((Instant) null));
    }

    public void testIsBusinessTime() {
        // Normal bus day
        LocalDate date = LocalDate.of(2023, 7, 11);
        ZonedDateTime tNotIn = date.atTime(8, 2, 3).atZone(timeZone);
        ZonedDateTime tIn = date.atTime(10, 2, 3).atZone(timeZone);
        assertFalse(bCalendar.isBusinessTime(tNotIn));
        assertFalse(bCalendar.isBusinessTime(tNotIn.toInstant()));
        assertTrue(bCalendar.isBusinessTime(tIn));
        assertTrue(bCalendar.isBusinessTime(tIn.toInstant()));

        // Weekend day -- THURS
        date = LocalDate.of(2023, 7, 13);
        tNotIn = date.atTime(8, 2, 3).atZone(timeZone);
        tIn = date.atTime(10, 2, 3).atZone(timeZone);
        assertFalse(bCalendar.isBusinessTime(tNotIn));
        assertFalse(bCalendar.isBusinessTime(tNotIn.toInstant()));
        assertFalse(bCalendar.isBusinessTime(tIn));
        assertFalse(bCalendar.isBusinessTime(tIn.toInstant()));

        // Holiday
        date = holidayDate1;
        tNotIn = date.atTime(8, 2, 3).atZone(timeZone);
        tIn = date.atTime(10, 2, 3).atZone(timeZone);
        assertFalse(bCalendar.isBusinessTime(tNotIn));
        assertFalse(bCalendar.isBusinessTime(tNotIn.toInstant()));
        assertFalse(bCalendar.isBusinessTime(tIn));
        assertFalse(bCalendar.isBusinessTime(tIn.toInstant()));

        // Half day
        date = halfDayDate;
        tNotIn = date.atTime(8, 2, 3).atZone(timeZone);
        tIn = date.atTime(10, 2, 3).atZone(timeZone);
        assertFalse(bCalendar.isBusinessTime(tNotIn));
        assertFalse(bCalendar.isBusinessTime(tNotIn.toInstant()));
        assertTrue(bCalendar.isBusinessTime(tIn));
        assertTrue(bCalendar.isBusinessTime(tIn.toInstant()));

        // Current date
        assertEquals(bCalendar.isBusinessTime(DateTimeUtils.now()), bCalendar.isBusinessTime());

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            bCalendar.isBusinessTime(d.atTime(12, 34).atZone(timeZone));
        }

        assertFalse(bCalendar.isBusinessTime((ZonedDateTime) null));
        assertFalse(bCalendar.isBusinessTime((Instant) null));
    }

    public void testFractionStandardBusinessDay() {
        // Normal bus day
        LocalDate date = LocalDate.of(2023, 7, 11);
        assertEquals(1.0, bCalendar.fractionStandardBusinessDay(date));
        assertEquals(1.0, bCalendar.fractionStandardBusinessDay(date.toString()));
        assertEquals(1.0, bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone)));
        assertEquals(1.0, bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone).toInstant()));

        // Weekend day -- THURS
        date = LocalDate.of(2023, 7, 13);
        assertEquals(0.0, bCalendar.fractionStandardBusinessDay(date));
        assertEquals(0.0, bCalendar.fractionStandardBusinessDay(date.toString()));
        assertEquals(0.0, bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone)));
        assertEquals(0.0, bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone).toInstant()));

        // Holiday
        date = holidayDate1;
        assertEquals(0.0, bCalendar.fractionStandardBusinessDay(date));
        assertEquals(0.0, bCalendar.fractionStandardBusinessDay(date.toString()));
        assertEquals(0.0, bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone)));
        assertEquals(0.0, bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone).toInstant()));

        // Half day
        date = halfDayDate;
        assertEquals((double) halfDay.businessNanos() / (double) schedule.businessNanos(),
                bCalendar.fractionStandardBusinessDay(date));
        assertEquals((double) halfDay.businessNanos() / (double) schedule.businessNanos(),
                bCalendar.fractionStandardBusinessDay(date.toString()));
        assertEquals((double) halfDay.businessNanos() / (double) schedule.businessNanos(),
                bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone)));
        assertEquals((double) halfDay.businessNanos() / (double) schedule.businessNanos(),
                bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone).toInstant()));

        // Current date
        assertEquals(bCalendar.fractionStandardBusinessDay(bCalendar.calendarDate()),
                bCalendar.fractionStandardBusinessDay());

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            bCalendar.fractionStandardBusinessDay(d);
        }

        assertEquals(NULL_DOUBLE, bCalendar.fractionStandardBusinessDay((LocalDate) null));
        assertEquals(NULL_DOUBLE, bCalendar.fractionStandardBusinessDay((String) null));
        assertEquals(NULL_DOUBLE, bCalendar.fractionStandardBusinessDay((ZonedDateTime) null));
        assertEquals(NULL_DOUBLE, bCalendar.fractionStandardBusinessDay((Instant) null));
    }


    public void testFractionBusinessDayComplete() {
        // Normal bus day
        LocalDate date = LocalDate.of(2023, 7, 11);
        ZonedDateTime tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        ZonedDateTime tIn = date.atTime(10, 2, 3).atZone(timeZone);
        ZonedDateTime tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(0.0, bCalendar.fractionBusinessDayComplete(tBefore));
        assertEquals(0.0, bCalendar.fractionBusinessDayComplete(tBefore.toInstant()));
        assertEquals((double) bCalendar.calendarDay(date).businessNanosElapsed(tIn.toInstant())
                / (double) schedule.businessNanos(), bCalendar.fractionBusinessDayComplete(tIn));
        assertEquals((double) bCalendar.calendarDay(date).businessNanosElapsed(tIn.toInstant())
                / (double) schedule.businessNanos(), bCalendar.fractionBusinessDayComplete(tIn.toInstant()));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter.toInstant()));

        // Weekend day -- THURS
        date = LocalDate.of(2023, 7, 13);
        tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        tIn = date.atTime(10, 2, 3).atZone(timeZone);
        tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tBefore));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tBefore.toInstant()));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tIn));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tIn.toInstant()));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter.toInstant()));

        // Holiday
        date = holidayDate1;
        tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        tIn = date.atTime(10, 2, 3).atZone(timeZone);
        tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tBefore));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tBefore.toInstant()));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tIn));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tIn.toInstant()));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter.toInstant()));

        // Half day
        date = halfDayDate;
        tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        tIn = date.atTime(10, 2, 3).atZone(timeZone);
        tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(0.0, bCalendar.fractionBusinessDayComplete(tBefore));
        assertEquals(0.0, bCalendar.fractionBusinessDayComplete(tBefore.toInstant()));
        assertEquals((double) bCalendar.calendarDay(date).businessNanosElapsed(tIn.toInstant())
                / (double) halfDay.businessNanos(), bCalendar.fractionBusinessDayComplete(tIn));
        assertEquals((double) bCalendar.calendarDay(date).businessNanosElapsed(tIn.toInstant())
                / (double) halfDay.businessNanos(), bCalendar.fractionBusinessDayComplete(tIn.toInstant()));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter.toInstant()));

        // Current date
        assertEquals(bCalendar.fractionBusinessDayComplete(DateTimeUtils.now()),
                bCalendar.fractionBusinessDayComplete(), 1e-5);

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            bCalendar.fractionBusinessDayComplete(d.atTime(12, 34).atZone(timeZone));
        }

        assertEquals(NULL_DOUBLE, bCalendar.fractionBusinessDayComplete((ZonedDateTime) null));
        assertEquals(NULL_DOUBLE, bCalendar.fractionBusinessDayComplete((Instant) null));
    }

    public void testFractionBusinessDayRemaining() {
        // Normal bus day
        LocalDate date = LocalDate.of(2023, 7, 11);
        ZonedDateTime tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        ZonedDateTime tIn = date.atTime(10, 2, 3).atZone(timeZone);
        ZonedDateTime tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(1.0, bCalendar.fractionBusinessDayRemaining(tBefore));
        assertEquals(1.0, bCalendar.fractionBusinessDayRemaining(tBefore.toInstant()));
        assertEquals(1 - (double) bCalendar.calendarDay(date).businessNanosElapsed(tIn.toInstant())
                / (double) schedule.businessNanos(), bCalendar.fractionBusinessDayRemaining(tIn));
        assertEquals(1 - (double) bCalendar.calendarDay(date).businessNanosElapsed(tIn.toInstant())
                / (double) schedule.businessNanos(), bCalendar.fractionBusinessDayRemaining(tIn.toInstant()));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter.toInstant()));

        // Weekend day -- THURS
        date = LocalDate.of(2023, 7, 13);
        tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        tIn = date.atTime(10, 2, 3).atZone(timeZone);
        tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tBefore));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tBefore.toInstant()));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tIn));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tIn.toInstant()));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter.toInstant()));

        // Holiday
        date = holidayDate1;
        tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        tIn = date.atTime(10, 2, 3).atZone(timeZone);
        tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tBefore));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tBefore.toInstant()));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tIn));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tIn.toInstant()));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter.toInstant()));

        // Half day
        date = halfDayDate;
        tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        tIn = date.atTime(10, 2, 3).atZone(timeZone);
        tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(1.0, bCalendar.fractionBusinessDayRemaining(tBefore));
        assertEquals(1.0, bCalendar.fractionBusinessDayRemaining(tBefore.toInstant()));
        assertEquals(1 - (double) bCalendar.calendarDay(date).businessNanosElapsed(tIn.toInstant())
                / (double) halfDay.businessNanos(), bCalendar.fractionBusinessDayRemaining(tIn));
        assertEquals(1 - (double) bCalendar.calendarDay(date).businessNanosElapsed(tIn.toInstant())
                / (double) halfDay.businessNanos(), bCalendar.fractionBusinessDayRemaining(tIn.toInstant()));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter.toInstant()));

        // Current date
        assertEquals(bCalendar.fractionBusinessDayRemaining(DateTimeUtils.now()),
                bCalendar.fractionBusinessDayRemaining(), 1e-5);

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            bCalendar.fractionBusinessDayRemaining(d.atTime(12, 34).atZone(timeZone));
        }

        assertEquals(NULL_DOUBLE, bCalendar.fractionBusinessDayRemaining((ZonedDateTime) null));
        assertEquals(NULL_DOUBLE, bCalendar.fractionBusinessDayRemaining((Instant) null));
    }

    public void testBusinessDates() {

        final LocalDate start = LocalDate.of(2023, 7, 3);
        final LocalDate end = LocalDate.of(2023, 7, 15);

        // final LocalDate[] nonBus = {
        // holidayDate1, // Holiday 2023-07-04
        // LocalDate.of(2023, 7, 5), // WED
        // // halfDayDate, // Half Day 2023-07-06 --> is a business day
        // LocalDate.of(2023, 7, 12), // WED
        // LocalDate.of(2023, 7, 13), // THURS
        // } ;

        final LocalDate[] bus = {
                LocalDate.of(2023, 7, 3),
                LocalDate.of(2023, 7, 6),
                LocalDate.of(2023, 7, 7),
                LocalDate.of(2023, 7, 8),
                LocalDate.of(2023, 7, 9),
                LocalDate.of(2023, 7, 10),
                LocalDate.of(2023, 7, 11),
                LocalDate.of(2023, 7, 14),
                LocalDate.of(2023, 7, 15),
        };

        assertEquals(bus, bCalendar.businessDates(start, end));
        assertEquals(Arrays.stream(bus).map(DateTimeUtils::formatDate).toArray(String[]::new),
                bCalendar.businessDates(start.toString(), end.toString()));
        assertEquals(bus,
                bCalendar.businessDates(start.atTime(1, 24).atZone(timeZone), end.atTime(1, 24).atZone(timeZone)));
        assertEquals(bus, bCalendar.businessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant()));

        assertEquals(Arrays.copyOfRange(bus, 0, bus.length - 1), bCalendar.businessDates(start, end, true, false));
        assertEquals(
                Arrays.stream(Arrays.copyOfRange(bus, 0, bus.length - 1)).map(DateTimeUtils::formatDate)
                        .toArray(String[]::new),
                bCalendar.businessDates(start.toString(), end.toString(), true, false));
        assertEquals(Arrays.copyOfRange(bus, 0, bus.length - 1), bCalendar
                .businessDates(start.atTime(1, 24).atZone(timeZone), end.atTime(1, 24).atZone(timeZone), true, false));
        assertEquals(Arrays.copyOfRange(bus, 0, bus.length - 1),
                bCalendar.businessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                        end.atTime(1, 24).atZone(timeZone).toInstant(), true, false));

        assertEquals(Arrays.copyOfRange(bus, 1, bus.length), bCalendar.businessDates(start, end, false, true));
        assertEquals(
                Arrays.stream(Arrays.copyOfRange(bus, 1, bus.length)).map(DateTimeUtils::formatDate)
                        .toArray(String[]::new),
                bCalendar.businessDates(start.toString(), end.toString(), false, true));
        assertEquals(Arrays.copyOfRange(bus, 1, bus.length), bCalendar
                .businessDates(start.atTime(1, 24).atZone(timeZone), end.atTime(1, 24).atZone(timeZone), false, true));
        assertEquals(Arrays.copyOfRange(bus, 1, bus.length),
                bCalendar.businessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                        end.atTime(1, 24).atZone(timeZone).toInstant(), false, true));

        assertEquals(Arrays.copyOfRange(bus, 1, bus.length - 1), bCalendar.businessDates(start, end, false, false));
        assertEquals(
                Arrays.stream(Arrays.copyOfRange(bus, 1, bus.length - 1)).map(DateTimeUtils::formatDate)
                        .toArray(String[]::new),
                bCalendar.businessDates(start.toString(), end.toString(), false, false));
        assertEquals(Arrays.copyOfRange(bus, 1, bus.length - 1), bCalendar
                .businessDates(start.atTime(1, 24).atZone(timeZone), end.atTime(1, 24).atZone(timeZone), false, false));
        assertEquals(Arrays.copyOfRange(bus, 1, bus.length - 1),
                bCalendar.businessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                        end.atTime(1, 24).atZone(timeZone).toInstant(), false, false));

        assertNull(bCalendar.businessDates(null, end, true, true));
        assertNull(bCalendar.businessDates(start, null, true, true));
        assertNull(bCalendar.businessDates(null, end.toString(), true, true));
        assertNull(bCalendar.businessDates(start.toString(), null, true, true));
        assertNull(bCalendar.businessDates(null, end.atTime(1, 2).atZone(timeZone).toInstant(), true, true));
        assertNull(bCalendar.businessDates(start.atTime(1, 2).atZone(timeZone).toInstant(), null, true, true));
        assertNull(bCalendar.businessDates(null, end.atTime(1, 2).atZone(timeZone), true, true));
        assertNull(bCalendar.businessDates(start.atTime(1, 2).atZone(timeZone), null, true, true));

        // end before start
        assertEquals(new LocalDate[0], bCalendar.businessDates(end, start));

        // long span of dates
        final LocalDate startLong = LocalDate.of(2019, 2, 1);
        final LocalDate endLong = LocalDate.of(2023, 12, 31);
        final ArrayList<LocalDate> targetLong = new ArrayList<>();

        for (LocalDate d = startLong; !d.isAfter(endLong); d = d.plusDays(1)) {
            if (bCalendar.isBusinessDay(d)) {
                targetLong.add(d);
            }
        }

        assertEquals(targetLong.toArray(LocalDate[]::new), bCalendar.businessDates(startLong, endLong));
    }

    public void testNumberBusinessDates() {

        final LocalDate start = LocalDate.of(2023, 7, 3);
        final LocalDate end = LocalDate.of(2023, 7, 15);

        // final LocalDate[] nonBus = {
        // holidayDate1, // Holiday 2023-07-04
        // LocalDate.of(2023, 7, 5), // WED
        // // halfDayDate, // Half Day 2023-07-06 --> is a business day
        // LocalDate.of(2023, 7, 12), // WED
        // LocalDate.of(2023, 7, 13), // THURS
        // } ;

        final LocalDate[] bus = {
                LocalDate.of(2023, 7, 3),
                LocalDate.of(2023, 7, 6),
                LocalDate.of(2023, 7, 7),
                LocalDate.of(2023, 7, 8),
                LocalDate.of(2023, 7, 9),
                LocalDate.of(2023, 7, 10),
                LocalDate.of(2023, 7, 11),
                LocalDate.of(2023, 7, 14),
                LocalDate.of(2023, 7, 15),
        };

        assertEquals(bus.length, bCalendar.numberBusinessDates(start, end));
        assertEquals(bus.length, bCalendar.numberBusinessDates(start.toString(), end.toString()));
        assertEquals(bus.length, bCalendar.numberBusinessDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone)));
        assertEquals(bus.length, bCalendar.numberBusinessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant()));

        assertEquals(bus.length - 1, bCalendar.numberBusinessDates(start, end, true, false));
        assertEquals(bus.length - 1, bCalendar.numberBusinessDates(start.toString(), end.toString(), true, false));
        assertEquals(bus.length - 1, bCalendar.numberBusinessDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone), true, false));
        assertEquals(bus.length - 1, bCalendar.numberBusinessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant(), true, false));

        assertEquals(bus.length - 1, bCalendar.numberBusinessDates(start, end, false, true));
        assertEquals(bus.length - 1, bCalendar.numberBusinessDates(start.toString(), end.toString(), false, true));
        assertEquals(bus.length - 1, bCalendar.numberBusinessDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone), false, true));
        assertEquals(bus.length - 1, bCalendar.numberBusinessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant(), false, true));

        assertEquals(bus.length - 2, bCalendar.numberBusinessDates(start, end, false, false));
        assertEquals(bus.length - 2, bCalendar.numberBusinessDates(start.toString(), end.toString(), false, false));
        assertEquals(bus.length - 2, bCalendar.numberBusinessDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone), false, false));
        assertEquals(bus.length - 2, bCalendar.numberBusinessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant(), false, false));


        assertEquals(NULL_INT, bCalendar.numberBusinessDates(null, end, true, true));
        assertEquals(NULL_INT, bCalendar.numberBusinessDates(start, null, true, true));
        assertEquals(NULL_INT, bCalendar.numberBusinessDates(null, end.toString(), true, true));
        assertEquals(NULL_INT, bCalendar.numberBusinessDates(start.toString(), null, true, true));
        assertEquals(NULL_INT,
                bCalendar.numberBusinessDates(null, end.atTime(1, 2).atZone(timeZone).toInstant(), true, true));
        assertEquals(NULL_INT,
                bCalendar.numberBusinessDates(start.atTime(1, 2).atZone(timeZone).toInstant(), null, true, true));
        assertEquals(NULL_INT, bCalendar.numberBusinessDates(null, end.atTime(1, 2).atZone(timeZone), true, true));
        assertEquals(NULL_INT, bCalendar.numberBusinessDates(start.atTime(1, 2).atZone(timeZone), null, true, true));

        // end before start
        assertEquals(0, bCalendar.numberBusinessDates(end, start));

        // long span of dates
        final LocalDate startLong = LocalDate.of(2019, 2, 1);
        final LocalDate endLong = LocalDate.of(2023, 12, 31);
        final ArrayList<LocalDate> targetLong = new ArrayList<>();

        for (LocalDate d = startLong; !d.isAfter(endLong); d = d.plusDays(1)) {
            if (bCalendar.isBusinessDay(d)) {
                targetLong.add(d);
            }
        }

        assertEquals(targetLong.size(), bCalendar.numberBusinessDates(startLong, endLong));
    }

    public void testBusinessDatesValidateCacheIteration() {
        // Construct a very simple calendar for counting business days that is easy to reason about

        final LocalDate firstValidDateLocal = LocalDate.of(2000, 1, 1);
        final LocalDate lastValidDateLocal = LocalDate.of(2030, 12, 31);
        final Set<DayOfWeek> weekendDaysLocal = Set.of(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY);
        final Map<LocalDate, CalendarDay<Instant>> holidaysLocal = new HashMap<>();
        final BusinessCalendar bc = new BusinessCalendar("Test", "Test", timeZone, firstValidDateLocal,
                lastValidDateLocal, schedule, weekendDaysLocal, holidaysLocal);

        // short period -- no caching case

        LocalDate start = LocalDate.of(2024, 5, 15);
        LocalDate end = LocalDate.of(2024, 5, 29);

        LocalDate[] target = {
                LocalDate.of(2024, 5, 15),
                LocalDate.of(2024, 5, 16),
                LocalDate.of(2024, 5, 17),
                LocalDate.of(2024, 5, 20),
                LocalDate.of(2024, 5, 21),
                LocalDate.of(2024, 5, 22),
                LocalDate.of(2024, 5, 23),
                LocalDate.of(2024, 5, 24),
                LocalDate.of(2024, 5, 27),
                LocalDate.of(2024, 5, 28),
                LocalDate.of(2024, 5, 29),
        };

        assertEquals(target, bc.businessDates(start, end, true, true));
        assertEquals(target.length, bc.numberBusinessDates(start, end, true, true));

        target = new LocalDate[] {
                LocalDate.of(2024, 5, 16),
                LocalDate.of(2024, 5, 17),
                LocalDate.of(2024, 5, 20),
                LocalDate.of(2024, 5, 21),
                LocalDate.of(2024, 5, 22),
                LocalDate.of(2024, 5, 23),
                LocalDate.of(2024, 5, 24),
                LocalDate.of(2024, 5, 27),
                LocalDate.of(2024, 5, 28),
        };

        assertEquals(target, bc.businessDates(start, end, false, false));
        assertEquals(target.length, bc.numberBusinessDates(start, end, false, false));

        // long period -- caching case

        start = LocalDate.of(2024, 5, 15);
        end = LocalDate.of(2025, 5, 29);

        final ArrayList<LocalDate> targetList = new ArrayList<>();
        LocalDate d = start;

        while (!d.isAfter(end)) {
            if (!weekendDaysLocal.contains(d.getDayOfWeek())) {
                targetList.add(d);
            }
            d = d.plusDays(1);
        }

        target = targetList.toArray(new LocalDate[0]);

        assertEquals(target, bc.businessDates(start, end, true, true));
        assertEquals(target.length, bc.numberBusinessDates(start, end, true, true));

        targetList.remove(0);
        targetList.remove(targetList.size() - 1);
        target = targetList.toArray(new LocalDate[0]);

        assertEquals(target, bc.businessDates(start, end, false, false));
        assertEquals(target.length, bc.numberBusinessDates(start, end, false, false));
    }

    public void testNonBusinessDatesValidateCacheIteration() {
        // Construct a very simple calendar for counting business days that is easy to reason about

        final LocalDate firstValidDateLocal = LocalDate.of(2000, 1, 1);
        final LocalDate lastValidDateLocal = LocalDate.of(2030, 12, 31);
        final Set<DayOfWeek> weekendDaysLocal = Set.of(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY);
        final Map<LocalDate, CalendarDay<Instant>> holidaysLocal = new HashMap<>();
        final BusinessCalendar bc = new BusinessCalendar("Test", "Test", timeZone, firstValidDateLocal,
                lastValidDateLocal, schedule, weekendDaysLocal, holidaysLocal);

        // short period -- no caching case

        LocalDate start = LocalDate.of(2024, 5, 12);
        LocalDate end = LocalDate.of(2024, 5, 26);

        LocalDate[] target = {
                LocalDate.of(2024, 5, 12),
                LocalDate.of(2024, 5, 18),
                LocalDate.of(2024, 5, 19),
                LocalDate.of(2024, 5, 25),
                LocalDate.of(2024, 5, 26),
        };

        assertEquals(target, bc.nonBusinessDates(start, end, true, true));
        assertEquals(target.length, bc.numberNonBusinessDates(start, end, true, true));

        target = new LocalDate[] {
                LocalDate.of(2024, 5, 18),
                LocalDate.of(2024, 5, 19),
                LocalDate.of(2024, 5, 25),
        };

        assertEquals(target, bc.nonBusinessDates(start, end, false, false));
        assertEquals(target.length, bc.numberNonBusinessDates(start, end, false, false));

        // long period -- caching case

        start = LocalDate.of(2024, 5, 19);
        end = LocalDate.of(2025, 5, 17);

        final ArrayList<LocalDate> targetList = new ArrayList<>();
        LocalDate d = start;

        while (!d.isAfter(end)) {
            if (weekendDaysLocal.contains(d.getDayOfWeek())) {
                targetList.add(d);
            }
            d = d.plusDays(1);
        }

        target = targetList.toArray(new LocalDate[0]);

        assertEquals(target, bc.nonBusinessDates(start, end, true, true));
        assertEquals(target.length, bc.numberNonBusinessDates(start, end, true, true));

        targetList.remove(0);
        targetList.remove(targetList.size() - 1);
        target = targetList.toArray(new LocalDate[0]);

        assertEquals(target, bc.nonBusinessDates(start, end, false, false));
        assertEquals(target.length, bc.numberNonBusinessDates(start, end, false, false));
    }

    public void testNonBusinessDates() {

        final LocalDate start = LocalDate.of(2023, 7, 3);
        final LocalDate end = LocalDate.of(2023, 7, 15);

        final LocalDate[] nonBus = {
                holidayDate1, // Holiday 2023-07-04
                LocalDate.of(2023, 7, 5), // WED
                // halfDayDate, // Half Day 2023-07-06 --> is a business day
                LocalDate.of(2023, 7, 12), // WED
                LocalDate.of(2023, 7, 13), // THURS
        };

        // final LocalDate[] bus = {
        // LocalDate.of(2023,7,3),
        // LocalDate.of(2023,7,6),
        // LocalDate.of(2023,7,7),
        // LocalDate.of(2023,7,8),
        // LocalDate.of(2023,7,9),
        // LocalDate.of(2023,7,10),
        // LocalDate.of(2023,7,11),
        // LocalDate.of(2023,7,14),
        // LocalDate.of(2023,7,15),
        // };

        assertEquals(nonBus, bCalendar.nonBusinessDates(start, end));
        assertEquals(Arrays.stream(nonBus).map(DateTimeUtils::formatDate).toArray(String[]::new),
                bCalendar.nonBusinessDates(start.toString(), end.toString()));
        assertEquals(nonBus,
                bCalendar.nonBusinessDates(start.atTime(1, 24).atZone(timeZone), end.atTime(1, 24).atZone(timeZone)));
        assertEquals(nonBus, bCalendar.nonBusinessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant()));

        assertEquals(Arrays.copyOfRange(nonBus, 0, nonBus.length - 1),
                bCalendar.nonBusinessDates(nonBus[0], nonBus[nonBus.length - 1], true, false));
        assertEquals(
                Arrays.stream(Arrays.copyOfRange(nonBus, 0, nonBus.length - 1)).map(DateTimeUtils::formatDate)
                        .toArray(String[]::new),
                bCalendar.nonBusinessDates(nonBus[0].toString(), nonBus[nonBus.length - 1].toString(), true, false));
        assertEquals(Arrays.copyOfRange(nonBus, 0, nonBus.length - 1),
                bCalendar.nonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone),
                        nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone), true, false));
        assertEquals(Arrays.copyOfRange(nonBus, 0, nonBus.length - 1),
                bCalendar.nonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone).toInstant(),
                        nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone).toInstant(), true, false));

        assertEquals(Arrays.copyOfRange(nonBus, 1, nonBus.length),
                bCalendar.nonBusinessDates(nonBus[0], nonBus[nonBus.length - 1], false, true));
        assertEquals(
                Arrays.stream(Arrays.copyOfRange(nonBus, 1, nonBus.length)).map(DateTimeUtils::formatDate)
                        .toArray(String[]::new),
                bCalendar.nonBusinessDates(nonBus[0].toString(), nonBus[nonBus.length - 1].toString(), false, true));
        assertEquals(Arrays.copyOfRange(nonBus, 1, nonBus.length),
                bCalendar.nonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone),
                        nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone), false, true));
        assertEquals(Arrays.copyOfRange(nonBus, 1, nonBus.length),
                bCalendar.nonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone).toInstant(),
                        nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone).toInstant(), false, true));

        assertEquals(Arrays.copyOfRange(nonBus, 1, nonBus.length - 1),
                bCalendar.nonBusinessDates(nonBus[0], nonBus[nonBus.length - 1], false, false));
        assertEquals(
                Arrays.stream(Arrays.copyOfRange(nonBus, 1, nonBus.length - 1)).map(DateTimeUtils::formatDate)
                        .toArray(String[]::new),
                bCalendar.nonBusinessDates(nonBus[0].toString(), nonBus[nonBus.length - 1].toString(), false, false));
        assertEquals(Arrays.copyOfRange(nonBus, 1, nonBus.length - 1),
                bCalendar.nonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone),
                        nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone), false, false));
        assertEquals(Arrays.copyOfRange(nonBus, 1, nonBus.length - 1),
                bCalendar.nonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone).toInstant(),
                        nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone).toInstant(), false, false));

        assertNull(bCalendar.nonBusinessDates(null, end, true, true));
        assertNull(bCalendar.nonBusinessDates(start, null, true, true));
        assertNull(bCalendar.nonBusinessDates(null, end.toString(), true, true));
        assertNull(bCalendar.nonBusinessDates(start.toString(), null, true, true));
        assertNull(bCalendar.nonBusinessDates(null, end.atTime(1, 2).atZone(timeZone).toInstant(), true, true));
        assertNull(bCalendar.nonBusinessDates(start.atTime(1, 2).atZone(timeZone).toInstant(), null, true, true));
        assertNull(bCalendar.nonBusinessDates(null, end.atTime(1, 2).atZone(timeZone), true, true));
        assertNull(bCalendar.nonBusinessDates(start.atTime(1, 2).atZone(timeZone), null, true, true));

        // end before start
        assertEquals(new LocalDate[0], bCalendar.nonBusinessDates(end, start));

        // long span of dates
        final LocalDate startLong = LocalDate.of(2019, 2, 1);
        final LocalDate endLong = LocalDate.of(2023, 12, 31);
        final ArrayList<LocalDate> targetLong = new ArrayList<>();

        for (LocalDate d = startLong; !d.isAfter(endLong); d = d.plusDays(1)) {
            if (!bCalendar.isBusinessDay(d)) {
                targetLong.add(d);
            }
        }

        assertEquals(targetLong.toArray(LocalDate[]::new), bCalendar.nonBusinessDates(startLong, endLong));
    }

    public void testNumberNonBusinessDates() {

        final LocalDate start = LocalDate.of(2023, 7, 3);
        final LocalDate end = LocalDate.of(2023, 7, 15);

        final LocalDate[] nonBus = {
                holidayDate1, // Holiday 2023-07-04
                LocalDate.of(2023, 7, 5), // WED
                // halfDayDate, // Half Day 2023-07-06 --> is a business day
                LocalDate.of(2023, 7, 12), // WED
                LocalDate.of(2023, 7, 13), // THURS
        };

        // final LocalDate[] bus = {
        // LocalDate.of(2023,7,3),
        // LocalDate.of(2023,7,6),
        // LocalDate.of(2023,7,7),
        // LocalDate.of(2023,7,8),
        // LocalDate.of(2023,7,9),
        // LocalDate.of(2023,7,10),
        // LocalDate.of(2023,7,11),
        // LocalDate.of(2023,7,14),
        // LocalDate.of(2023,7,15),
        // };

        assertEquals(0, bCalendar.numberNonBusinessDates(end, start));
        assertEquals(nonBus.length, bCalendar.numberNonBusinessDates(start, end));
        assertEquals(nonBus.length, bCalendar.numberNonBusinessDates(start.toString(), end.toString()));
        assertEquals(nonBus.length, bCalendar.numberNonBusinessDates(start.atTime(1, 24).atZone(timeZone),
                end.atTime(1, 24).atZone(timeZone)));
        assertEquals(nonBus.length, bCalendar.numberNonBusinessDates(start.atTime(1, 24).atZone(timeZone).toInstant(),
                end.atTime(1, 24).atZone(timeZone).toInstant()));

        assertEquals(nonBus.length - 1,
                bCalendar.numberNonBusinessDates(nonBus[0], nonBus[nonBus.length - 1], true, false));
        assertEquals(nonBus.length - 1, bCalendar.numberNonBusinessDates(nonBus[0].toString(),
                nonBus[nonBus.length - 1].toString(), true, false));
        assertEquals(nonBus.length - 1, bCalendar.numberNonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone),
                nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone), true, false));
        assertEquals(nonBus.length - 1,
                bCalendar.numberNonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone).toInstant(),
                        nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone).toInstant(), true, false));

        assertEquals(nonBus.length - 1,
                bCalendar.numberNonBusinessDates(nonBus[0], nonBus[nonBus.length - 1], false, true));
        assertEquals(nonBus.length - 1, bCalendar.numberNonBusinessDates(nonBus[0].toString(),
                nonBus[nonBus.length - 1].toString(), false, true));
        assertEquals(nonBus.length - 1, bCalendar.numberNonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone),
                nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone), false, true));
        assertEquals(nonBus.length - 1,
                bCalendar.numberNonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone).toInstant(),
                        nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone).toInstant(), false, true));

        assertEquals(nonBus.length - 2,
                bCalendar.numberNonBusinessDates(nonBus[0], nonBus[nonBus.length - 1], false, false));
        assertEquals(nonBus.length - 2, bCalendar.numberNonBusinessDates(nonBus[0].toString(),
                nonBus[nonBus.length - 1].toString(), false, false));
        assertEquals(nonBus.length - 2, bCalendar.numberNonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone),
                nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone), false, false));
        assertEquals(nonBus.length - 2,
                bCalendar.numberNonBusinessDates(nonBus[0].atTime(1, 24).atZone(timeZone).toInstant(),
                        nonBus[nonBus.length - 1].atTime(1, 24).atZone(timeZone).toInstant(), false, false));

        assertEquals(NULL_INT, bCalendar.numberNonBusinessDates(null, end, true, true));
        assertEquals(NULL_INT, bCalendar.numberNonBusinessDates(start, null, true, true));
        assertEquals(NULL_INT, bCalendar.numberNonBusinessDates(null, end.toString(), true, true));
        assertEquals(NULL_INT, bCalendar.numberNonBusinessDates(start.toString(), null, true, true));
        assertEquals(NULL_INT,
                bCalendar.numberNonBusinessDates(null, end.atTime(1, 2).atZone(timeZone).toInstant(), true, true));
        assertEquals(NULL_INT,
                bCalendar.numberNonBusinessDates(start.atTime(1, 2).atZone(timeZone).toInstant(), null, true, true));
        assertEquals(NULL_INT, bCalendar.numberNonBusinessDates(null, end.atTime(1, 2).atZone(timeZone), true, true));
        assertEquals(NULL_INT, bCalendar.numberNonBusinessDates(start.atTime(1, 2).atZone(timeZone), null, true, true));

        final LocalDate startLong = LocalDate.of(2023, 7, 3);
        final LocalDate endLong = LocalDate.of(2025, 7, 15);
        final LocalDate[] nonBusLong = bCalendar.nonBusinessDates(startLong, endLong);
        assertEquals(nonBusLong.length, bCalendar.numberNonBusinessDates(startLong, endLong));
    }

    public void testDiffBusinessNanos() {
        // Same day
        final ZonedDateTime zdt1 = LocalDate.of(2023, 7, 3).atTime(9, 27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023, 7, 3).atTime(12, 10).atZone(timeZone);

        assertEquals(zdt1.until(zdt2, ChronoUnit.NANOS), bCalendar.diffBusinessNanos(zdt1, zdt2));
        assertEquals(-zdt1.until(zdt2, ChronoUnit.NANOS), bCalendar.diffBusinessNanos(zdt2, zdt1));
        assertEquals(zdt1.until(zdt2, ChronoUnit.NANOS),
                bCalendar.diffBusinessNanos(zdt1.toInstant(), zdt2.toInstant()));
        assertEquals(-zdt1.until(zdt2, ChronoUnit.NANOS),
                bCalendar.diffBusinessNanos(zdt2.toInstant(), zdt1.toInstant()));

        // Multiple holidays
        final ZonedDateTime zdt3 = LocalDate.of(2023, 7, 8).atTime(12, 54).atZone(timeZone);
        final long target = schedule.businessNanosRemaining(zdt1.toLocalTime()) // 2023-07-03
                // 2023-07-04 holiday
                // 2023-07-05 weekend WED
                + halfDay.businessNanos() // 2023-07-06 half day
                + schedule.businessNanos() // normal day
                + schedule.businessNanosElapsed(zdt3.toLocalTime());

        assertEquals(target, bCalendar.diffBusinessNanos(zdt1, zdt3));
        assertEquals(-target, bCalendar.diffBusinessNanos(zdt3, zdt1));
        assertEquals(target, bCalendar.diffBusinessNanos(zdt1.toInstant(), zdt3.toInstant()));
        assertEquals(-target, bCalendar.diffBusinessNanos(zdt3.toInstant(), zdt1.toInstant()));

        assertEquals(NULL_LONG, bCalendar.diffBusinessNanos(zdt1, null));
        assertEquals(NULL_LONG, bCalendar.diffBusinessNanos(null, zdt3));
        assertEquals(NULL_LONG, bCalendar.diffBusinessNanos(zdt1.toInstant(), null));
        assertEquals(NULL_LONG, bCalendar.diffBusinessNanos(null, zdt3.toInstant()));
    }

    public void testDiffNonBusinessNanos() {
        // Same day
        final ZonedDateTime zdt1 = LocalDate.of(2023, 7, 3).atTime(6, 27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023, 7, 3).atTime(15, 10).atZone(timeZone);

        assertEquals(DateTimeUtils.diffNanos(zdt1, zdt2) - bCalendar.diffBusinessNanos(zdt1, zdt2),
                bCalendar.diffNonBusinessNanos(zdt1, zdt2));
        assertEquals(-bCalendar.diffNonBusinessNanos(zdt1, zdt2), bCalendar.diffNonBusinessNanos(zdt2, zdt1));
        assertEquals(DateTimeUtils.diffNanos(zdt1, zdt2) - bCalendar.diffBusinessNanos(zdt1, zdt2),
                bCalendar.diffNonBusinessNanos(zdt1.toInstant(), zdt2.toInstant()));
        assertEquals(-bCalendar.diffNonBusinessNanos(zdt1, zdt2),
                bCalendar.diffNonBusinessNanos(zdt2.toInstant(), zdt1.toInstant()));

        // Multiple holidays
        final ZonedDateTime zdt3 = LocalDate.of(2023, 7, 8).atTime(12, 54).atZone(timeZone);

        assertEquals(DateTimeUtils.diffNanos(zdt1, zdt3) - bCalendar.diffBusinessNanos(zdt1, zdt3),
                bCalendar.diffNonBusinessNanos(zdt1, zdt3));
        assertEquals(-bCalendar.diffNonBusinessNanos(zdt1, zdt3), bCalendar.diffNonBusinessNanos(zdt3, zdt1));
        assertEquals(DateTimeUtils.diffNanos(zdt1, zdt3) - bCalendar.diffBusinessNanos(zdt1, zdt3),
                bCalendar.diffNonBusinessNanos(zdt1.toInstant(), zdt3.toInstant()));
        assertEquals(-bCalendar.diffNonBusinessNanos(zdt1, zdt3),
                bCalendar.diffNonBusinessNanos(zdt3.toInstant(), zdt1.toInstant()));

        assertEquals(NULL_LONG, bCalendar.diffNonBusinessNanos(zdt1, null));
        assertEquals(NULL_LONG, bCalendar.diffNonBusinessNanos(null, zdt3));
        assertEquals(NULL_LONG, bCalendar.diffNonBusinessNanos(zdt1.toInstant(), null));
        assertEquals(NULL_LONG, bCalendar.diffNonBusinessNanos(null, zdt3.toInstant()));
    }

    public void testDiffBusinessDuration() {
        // Same day
        final ZonedDateTime zdt1 = LocalDate.of(2023, 7, 3).atTime(9, 27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023, 7, 3).atTime(12, 10).atZone(timeZone);

        assertEquals(Duration.ofNanos(zdt1.until(zdt2, ChronoUnit.NANOS)), bCalendar.diffBusinessDuration(zdt1, zdt2));
        assertEquals(Duration.ofNanos(-zdt1.until(zdt2, ChronoUnit.NANOS)), bCalendar.diffBusinessDuration(zdt2, zdt1));
        assertEquals(Duration.ofNanos(zdt1.until(zdt2, ChronoUnit.NANOS)),
                bCalendar.diffBusinessDuration(zdt1.toInstant(), zdt2.toInstant()));
        assertEquals(Duration.ofNanos(-zdt1.until(zdt2, ChronoUnit.NANOS)),
                bCalendar.diffBusinessDuration(zdt2.toInstant(), zdt1.toInstant()));

        // Multiple holidays
        final ZonedDateTime zdt3 = LocalDate.of(2023, 7, 8).atTime(12, 54).atZone(timeZone);
        final long target = schedule.businessNanosRemaining(zdt1.toLocalTime()) // 2023-07-03
                // 2023-07-04 holiday
                // 2023-07-05 weekend WED
                + halfDay.businessNanos() // 2023-07-06 half day
                + schedule.businessNanos() // normal day
                + schedule.businessNanosElapsed(zdt3.toLocalTime());

        assertEquals(Duration.ofNanos(target), bCalendar.diffBusinessDuration(zdt1, zdt3));
        assertEquals(Duration.ofNanos(-target), bCalendar.diffBusinessDuration(zdt3, zdt1));
        assertEquals(Duration.ofNanos(target), bCalendar.diffBusinessDuration(zdt1.toInstant(), zdt3.toInstant()));
        assertEquals(Duration.ofNanos(-target), bCalendar.diffBusinessDuration(zdt3.toInstant(), zdt1.toInstant()));

        assertNull(bCalendar.diffBusinessDuration(zdt1, null));
        assertNull(bCalendar.diffBusinessDuration(null, zdt3));
        assertNull(bCalendar.diffBusinessDuration(zdt1.toInstant(), null));
        assertNull(bCalendar.diffBusinessDuration(null, zdt3.toInstant()));
    }

    public void testDiffNonBusinessDuration() {
        // Same day
        final ZonedDateTime zdt1 = LocalDate.of(2023, 7, 3).atTime(6, 27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023, 7, 3).atTime(15, 10).atZone(timeZone);

        assertEquals(Duration.ofNanos(DateTimeUtils.diffNanos(zdt1, zdt2) - bCalendar.diffBusinessNanos(zdt1, zdt2)),
                bCalendar.diffNonBusinessDuration(zdt1, zdt2));
        assertEquals(Duration.ofNanos(-bCalendar.diffNonBusinessNanos(zdt1, zdt2)),
                bCalendar.diffNonBusinessDuration(zdt2, zdt1));
        assertEquals(Duration.ofNanos(DateTimeUtils.diffNanos(zdt1, zdt2) - bCalendar.diffBusinessNanos(zdt1, zdt2)),
                bCalendar.diffNonBusinessDuration(zdt1.toInstant(), zdt2.toInstant()));
        assertEquals(Duration.ofNanos(-bCalendar.diffNonBusinessNanos(zdt1, zdt2)),
                bCalendar.diffNonBusinessDuration(zdt2.toInstant(), zdt1.toInstant()));

        // Multiple holidays
        final ZonedDateTime zdt3 = LocalDate.of(2023, 7, 8).atTime(12, 54).atZone(timeZone);

        assertEquals(Duration.ofNanos(DateTimeUtils.diffNanos(zdt1, zdt3) - bCalendar.diffBusinessNanos(zdt1, zdt3)),
                bCalendar.diffNonBusinessDuration(zdt1, zdt3));
        assertEquals(Duration.ofNanos(-bCalendar.diffNonBusinessNanos(zdt1, zdt3)),
                bCalendar.diffNonBusinessDuration(zdt3, zdt1));
        assertEquals(Duration.ofNanos(DateTimeUtils.diffNanos(zdt1, zdt3) - bCalendar.diffBusinessNanos(zdt1, zdt3)),
                bCalendar.diffNonBusinessDuration(zdt1.toInstant(), zdt3.toInstant()));
        assertEquals(Duration.ofNanos(-bCalendar.diffNonBusinessNanos(zdt1, zdt3)),
                bCalendar.diffNonBusinessDuration(zdt3.toInstant(), zdt1.toInstant()));

        assertNull(bCalendar.diffNonBusinessDuration(zdt1, null));
        assertNull(bCalendar.diffNonBusinessDuration(null, zdt3));
        assertNull(bCalendar.diffNonBusinessDuration(zdt1.toInstant(), null));
        assertNull(bCalendar.diffNonBusinessDuration(null, zdt3.toInstant()));

    }

    public void testDiffBusinessDays() {
        final ZonedDateTime zdt1 = LocalDate.of(2023, 7, 3).atTime(6, 27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023, 7, 10).atTime(15, 10).atZone(timeZone);

        assertEquals(bCalendar.diffBusinessNanos(zdt1, zdt2) / (double) schedule.businessNanos(),
                bCalendar.diffBusinessDays(zdt1, zdt2));
        assertEquals(bCalendar.diffBusinessNanos(zdt1, zdt2) / (double) schedule.businessNanos(),
                bCalendar.diffBusinessDays(zdt1.toInstant(), zdt2.toInstant()));

        assertEquals(NULL_DOUBLE, bCalendar.diffBusinessDays(zdt1, null));
        assertEquals(NULL_DOUBLE, bCalendar.diffBusinessDays(null, zdt2));
        assertEquals(NULL_DOUBLE, bCalendar.diffBusinessDays(zdt1.toInstant(), null));
        assertEquals(NULL_DOUBLE, bCalendar.diffBusinessDays(null, zdt2.toInstant()));
    }

    public void testDiffBusinessYears() {
        final ZonedDateTime zdt1 = LocalDate.of(2023, 1, 1).atTime(6, 27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023, 12, 31).atTime(15, 10).atZone(timeZone);

        assertEquals(1.0, bCalendar.diffBusinessYears(zdt1, zdt2));
        assertEquals(1.0, bCalendar.diffBusinessYears(zdt1.toInstant(), zdt2.toInstant()));

        final ZonedDateTime zdt3 = LocalDate.of(2024, 12, 31).atTime(15, 10).atZone(timeZone);

        assertEquals(2.0, bCalendar.diffBusinessYears(zdt1, zdt3));
        assertEquals(2.0, bCalendar.diffBusinessYears(zdt1.toInstant(), zdt3.toInstant()));

        final ZonedDateTime zdt4 = LocalDate.of(2025, 12, 31).atTime(15, 10).atZone(timeZone);

        assertEquals(3.0, bCalendar.diffBusinessYears(zdt1, zdt4));
        assertEquals(3.0, bCalendar.diffBusinessYears(zdt1.toInstant(), zdt4.toInstant()));

        final ZonedDateTime zdt5 = LocalDate.of(2022, 12, 1).atTime(15, 10).atZone(timeZone);
        final ZonedDateTime zdt6 = LocalDate.of(2026, 1, 31).atTime(15, 10).atZone(timeZone);
        final long length2022 = bCalendar.diffBusinessNanos(
                LocalDate.of(2022, 1, 1).atTime(0, 0).atZone(timeZone),
                LocalDate.of(2022, 12, 31).atTime(23, 59).atZone(timeZone));
        final long length2026 = bCalendar.diffBusinessNanos(
                LocalDate.of(2026, 1, 1).atTime(0, 0).atZone(timeZone),
                LocalDate.of(2026, 12, 31).atTime(23, 59).atZone(timeZone));
        final double start = bCalendar.diffBusinessNanos(zdt5, zdt1) / (double) length2022;
        final double end = bCalendar.diffBusinessNanos(zdt4, zdt6) / (double) length2026;

        assertEquals(3.0 + start + end, bCalendar.diffBusinessYears(zdt5, zdt6), 1e-5);
        assertEquals(3.0 + start + end, bCalendar.diffBusinessYears(zdt5.toInstant(), zdt6.toInstant()), 1e-5);

        assertEquals(NULL_DOUBLE, bCalendar.diffBusinessYears(zdt1, null));
        assertEquals(NULL_DOUBLE, bCalendar.diffBusinessYears(null, zdt2));
        assertEquals(NULL_DOUBLE, bCalendar.diffBusinessYears(zdt1.toInstant(), null));
        assertEquals(NULL_DOUBLE, bCalendar.diffBusinessYears(null, zdt2.toInstant()));
    }

    public void testPlusBusinessDays() {
        // 2023-07-03 normal day
        // 2023-07-04 holiday
        // 2023-07-05 weekend
        // 2023-07-06 half day
        // 2023-07-07 normal day
        // 2023-07-08 normal day
        // 2023-07-09 normal day
        // 2023-07-10 normal day
        // 2023-07-11 normal day
        // 2023-07-12 weekend
        // 2023-07-13 weekend
        // 2023-07-14 normal day
        // 2023-07-15 normal day

        final ZoneId timeZone2 = ZoneId.of("America/New_York");
        LocalDate d = LocalDate.of(2023, 7, 3);
        String s = d.toString();
        ZonedDateTime z = d.atTime(6, 24).atZone(timeZone2);
        Instant i = z.toInstant();

        assertEquals(d, bCalendar.plusBusinessDays(d, 0));
        assertEquals(d.toString(), bCalendar.plusBusinessDays(s, 0));
        assertEquals(z.withZoneSameInstant(timeZone), bCalendar.plusBusinessDays(z, 0));
        assertEquals(i, bCalendar.plusBusinessDays(i, 0));

        final LocalDate d2 = LocalDate.of(2023, 7, 6);
        final Instant i2 = d2.atTime(6, 24).atZone(timeZone2).toInstant();
        final ZonedDateTime z2 = d2.atTime(6, 24).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d2, bCalendar.plusBusinessDays(d, 1));
        assertEquals(d2.toString(), bCalendar.plusBusinessDays(s, 1));
        assertEquals(z2, bCalendar.plusBusinessDays(z, 1));
        assertEquals(i2, bCalendar.plusBusinessDays(i, 1));

        final LocalDate d3 = LocalDate.of(2023, 7, 7);
        final Instant i3 = d3.atTime(6, 24).atZone(timeZone2).toInstant();
        final ZonedDateTime z3 = d3.atTime(6, 24).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d3, bCalendar.plusBusinessDays(d, 2));
        assertEquals(d3.toString(), bCalendar.plusBusinessDays(s, 2));
        assertEquals(z3, bCalendar.plusBusinessDays(z, 2));
        assertEquals(i3, bCalendar.plusBusinessDays(i, 2));

        final LocalDate d4 = LocalDate.of(2023, 7, 14);
        final Instant i4 = d4.atTime(6, 24).atZone(timeZone2).toInstant();
        final ZonedDateTime z4 = d4.atTime(6, 24).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d4, bCalendar.plusBusinessDays(d, 7));
        assertEquals(d4.toString(), bCalendar.plusBusinessDays(s, 7));
        assertEquals(z4, bCalendar.plusBusinessDays(z, 7));
        assertEquals(i4, bCalendar.plusBusinessDays(i, 7));

        d = LocalDate.of(2023, 7, 14);
        s = d.toString();
        z = d.atTime(6, 25).atZone(timeZone2);
        i = z.toInstant();

        assertEquals(d, bCalendar.plusBusinessDays(d, 0));
        assertEquals(d.toString(), bCalendar.plusBusinessDays(s, 0));
        assertEquals(z.withZoneSameInstant(timeZone), bCalendar.plusBusinessDays(z, 0));
        assertEquals(i, bCalendar.plusBusinessDays(i, 0));

        final LocalDate d5 = LocalDate.of(2023, 7, 11);
        final Instant i5 = d5.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z5 = d5.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d5, bCalendar.plusBusinessDays(d, -1));
        assertEquals(d5.toString(), bCalendar.plusBusinessDays(s, -1));
        assertEquals(z5, bCalendar.plusBusinessDays(z, -1));
        assertEquals(i5, bCalendar.plusBusinessDays(i, -1));

        final LocalDate d6 = LocalDate.of(2023, 7, 10);
        final Instant i6 = d6.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z6 = d6.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d6, bCalendar.plusBusinessDays(d, -2));
        assertEquals(d6.toString(), bCalendar.plusBusinessDays(s, -2));
        assertEquals(z6, bCalendar.plusBusinessDays(z, -2));
        assertEquals(i6, bCalendar.plusBusinessDays(i, -2));

        final LocalDate d7 = LocalDate.of(2023, 7, 3);
        final Instant i7 = d7.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z7 = d7.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d7, bCalendar.plusBusinessDays(d, -7));
        assertEquals(d7.toString(), bCalendar.plusBusinessDays(s, -7));
        assertEquals(z7, bCalendar.plusBusinessDays(z, -7));
        assertEquals(i7, bCalendar.plusBusinessDays(i, -7));

        d = LocalDate.of(2023, 7, 4);
        s = d.toString();
        z = d.atTime(6, 24).atZone(timeZone2);
        i = z.toInstant();

        assertNull(bCalendar.plusBusinessDays(d, 0));
        assertNull(bCalendar.plusBusinessDays(s, 0));
        assertNull(bCalendar.plusBusinessDays(z.withZoneSameInstant(timeZone), 0));
        assertNull(bCalendar.plusBusinessDays(i, 0));

        assertNull(bCalendar.plusBusinessDays((LocalDate) null, 1));
        assertNull(bCalendar.plusBusinessDays((String) null, 1));
        assertNull(bCalendar.plusBusinessDays((ZonedDateTime) null, 1));
        assertNull(bCalendar.plusBusinessDays((Instant) null, 1));

        assertNull(bCalendar.plusBusinessDays(d, NULL_INT));
        assertNull(bCalendar.plusBusinessDays(s, NULL_INT));
        assertNull(bCalendar.plusBusinessDays(z.withZoneSameInstant(timeZone), NULL_INT));
        assertNull(bCalendar.plusBusinessDays(i, NULL_INT));
    }

    public void testMinusBusinessDays() {
        // 2023-07-03 normal day
        // 2023-07-04 holiday
        // 2023-07-05 weekend
        // 2023-07-06 half day
        // 2023-07-07 normal day
        // 2023-07-08 normal day
        // 2023-07-09 normal day
        // 2023-07-10 normal day
        // 2023-07-11 normal day
        // 2023-07-12 weekend
        // 2023-07-13 weekend
        // 2023-07-14 normal day
        // 2023-07-15 normal day

        final ZoneId timeZone2 = ZoneId.of("America/New_York");
        LocalDate d = LocalDate.of(2023, 7, 3);
        String s = d.toString();
        ZonedDateTime z = d.atTime(6, 25).atZone(timeZone2);
        Instant i = z.toInstant();

        assertEquals(d, bCalendar.minusBusinessDays(d, 0));
        assertEquals(d.toString(), bCalendar.minusBusinessDays(s, 0));
        assertEquals(z.withZoneSameInstant(timeZone), bCalendar.minusBusinessDays(z, 0));
        assertEquals(i, bCalendar.minusBusinessDays(i, 0));

        final LocalDate d1 = LocalDate.of(2023, 7, 6);
        final Instant i1 = d1.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z1 = d1.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d1, bCalendar.minusBusinessDays(d, -1));
        assertEquals(d1.toString(), bCalendar.minusBusinessDays(s, -1));
        assertEquals(z1, bCalendar.minusBusinessDays(z, -1));
        assertEquals(i1, bCalendar.minusBusinessDays(i, -1));

        final LocalDate d2 = LocalDate.of(2023, 7, 7);
        final Instant i2 = d2.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z2 = d2.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d2, bCalendar.minusBusinessDays(d, -2));
        assertEquals(d2.toString(), bCalendar.minusBusinessDays(s, -2));
        assertEquals(z2, bCalendar.minusBusinessDays(z, -2));
        assertEquals(i2, bCalendar.minusBusinessDays(i, -2));

        final LocalDate d3 = LocalDate.of(2023, 7, 14);
        final Instant i3 = d3.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z3 = d3.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d3, bCalendar.minusBusinessDays(d, -7));
        assertEquals(d3.toString(), bCalendar.minusBusinessDays(s, -7));
        assertEquals(z3, bCalendar.minusBusinessDays(z, -7));
        assertEquals(i3, bCalendar.minusBusinessDays(i, -7));

        d = LocalDate.of(2023, 7, 14);
        s = d.toString();
        z = d.atTime(6, 25).atZone(timeZone2);
        i = z.toInstant();

        assertEquals(d, bCalendar.minusBusinessDays(d, 0));
        assertEquals(d.toString(), bCalendar.minusBusinessDays(s, 0));
        assertEquals(z.withZoneSameInstant(timeZone), bCalendar.minusBusinessDays(z, 0));
        assertEquals(i, bCalendar.minusBusinessDays(i, 0));

        final LocalDate d4 = LocalDate.of(2023, 7, 11);
        final Instant i4 = d4.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z4 = d4.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d4, bCalendar.minusBusinessDays(d, 1));
        assertEquals(d4.toString(), bCalendar.minusBusinessDays(s, 1));
        assertEquals(z4, bCalendar.minusBusinessDays(z, 1));
        assertEquals(i4, bCalendar.minusBusinessDays(i, 1));

        final LocalDate d5 = LocalDate.of(2023, 7, 10);
        final Instant i5 = d5.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z5 = d5.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d5, bCalendar.minusBusinessDays(d, 2));
        assertEquals(d5.toString(), bCalendar.minusBusinessDays(s, 2));
        assertEquals(z5, bCalendar.minusBusinessDays(z, 2));
        assertEquals(i5, bCalendar.minusBusinessDays(i, 2));

        final LocalDate d6 = LocalDate.of(2023, 7, 3);
        final Instant i6 = d6.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z6 = d6.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d6, bCalendar.minusBusinessDays(d, 7));
        assertEquals(d6.toString(), bCalendar.minusBusinessDays(s, 7));
        assertEquals(z6, bCalendar.minusBusinessDays(z, 7));
        assertEquals(i6, bCalendar.minusBusinessDays(i, 7));

        d = LocalDate.of(2023, 7, 4);
        s = d.toString();
        z = d.atTime(6, 24).atZone(timeZone2);
        i = z.toInstant();

        assertNull(bCalendar.minusBusinessDays(d, 0));
        assertNull(bCalendar.minusBusinessDays(s, 0));
        assertNull(bCalendar.minusBusinessDays(z, 0));
        assertNull(bCalendar.minusBusinessDays(i, 0));

        assertNull(bCalendar.minusBusinessDays((LocalDate) null, 1));
        assertNull(bCalendar.minusBusinessDays((String) null, 1));
        assertNull(bCalendar.minusBusinessDays((ZonedDateTime) null, 1));
        assertNull(bCalendar.minusBusinessDays((Instant) null, 1));

        assertNull(bCalendar.minusBusinessDays(d, NULL_INT));
        assertNull(bCalendar.minusBusinessDays(s, NULL_INT));
        assertNull(bCalendar.minusBusinessDays(z.withZoneSameInstant(timeZone), NULL_INT));
        assertNull(bCalendar.minusBusinessDays(i, NULL_INT));
    }

    public void testPlusNonBusinessDays() {
        // 2023-07-03 normal day
        // 2023-07-04 holiday
        // 2023-07-05 weekend
        // 2023-07-06 half day
        // 2023-07-07 normal day
        // 2023-07-08 normal day
        // 2023-07-09 normal day
        // 2023-07-10 normal day
        // 2023-07-11 normal day
        // 2023-07-12 weekend
        // 2023-07-13 weekend
        // 2023-07-14 normal day
        // 2023-07-15 normal day

        final ZoneId timeZone2 = ZoneId.of("America/New_York");
        LocalDate d = LocalDate.of(2023, 7, 2);
        String s = d.toString();
        ZonedDateTime z = d.atTime(6, 25).atZone(timeZone2);
        Instant i = z.toInstant();

        assertNull(bCalendar.plusNonBusinessDays(d, 0));
        assertNull(bCalendar.plusNonBusinessDays(s, 0));
        assertNull(bCalendar.plusNonBusinessDays(z, 0));
        assertNull(bCalendar.plusNonBusinessDays(i, 0));

        final LocalDate d1 = LocalDate.of(2023, 7, 4);
        final Instant i1 = d1.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z1 = d1.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d1, bCalendar.plusNonBusinessDays(d, 1));
        assertEquals(d1.toString(), bCalendar.plusNonBusinessDays(s, 1));
        assertEquals(z1, bCalendar.plusNonBusinessDays(z, 1));
        assertEquals(i1, bCalendar.plusNonBusinessDays(i, 1));

        final LocalDate d2 = LocalDate.of(2023, 7, 5);
        final Instant i2 = d2.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z2 = d2.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d2, bCalendar.plusNonBusinessDays(d, 2));
        assertEquals(d2.toString(), bCalendar.plusNonBusinessDays(s, 2));
        assertEquals(z2, bCalendar.plusNonBusinessDays(z, 2));
        assertEquals(i2, bCalendar.plusNonBusinessDays(i, 2));

        final LocalDate d3 = LocalDate.of(2023, 7, 13);
        final Instant i3 = d3.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z3 = d3.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d3, bCalendar.plusNonBusinessDays(d, 4));
        assertEquals(d3.toString(), bCalendar.plusNonBusinessDays(s, 4));
        assertEquals(z3, bCalendar.plusNonBusinessDays(z, 4));
        assertEquals(i3, bCalendar.plusNonBusinessDays(i, 4));

        d = LocalDate.of(2023, 7, 14);
        s = d.toString();
        z = d.atTime(6, 25).atZone(timeZone2);
        i = z.toInstant();

        assertNull(bCalendar.plusNonBusinessDays(d, 0));
        assertNull(bCalendar.plusNonBusinessDays(s, 0));
        assertNull(bCalendar.plusNonBusinessDays(z, 0));
        assertNull(bCalendar.plusNonBusinessDays(i, 0));

        final LocalDate d4 = LocalDate.of(2023, 7, 13);
        final Instant i4 = d4.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z4 = d4.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d4, bCalendar.plusNonBusinessDays(d, -1));
        assertEquals(d4.toString(), bCalendar.plusNonBusinessDays(s, -1));
        assertEquals(z4, bCalendar.plusNonBusinessDays(z, -1));
        assertEquals(i4, bCalendar.plusNonBusinessDays(i, -1));

        final LocalDate d5 = LocalDate.of(2023, 7, 12);
        final Instant i5 = d5.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z5 = d5.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d5, bCalendar.plusNonBusinessDays(d, -2));
        assertEquals(d5.toString(), bCalendar.plusNonBusinessDays(s, -2));
        assertEquals(z5, bCalendar.plusNonBusinessDays(z, -2));
        assertEquals(i5, bCalendar.plusNonBusinessDays(i, -2));

        final LocalDate d6 = LocalDate.of(2023, 7, 4);
        final Instant i6 = d6.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z6 = d6.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d6, bCalendar.plusNonBusinessDays(d, -4));
        assertEquals(d6.toString(), bCalendar.plusNonBusinessDays(s, -4));
        assertEquals(z6, bCalendar.plusNonBusinessDays(z, -4));
        assertEquals(i6, bCalendar.plusNonBusinessDays(i, -4));

        d = LocalDate.of(2023, 7, 2);
        s = d.toString();
        z = d.atTime(6, 25).atZone(timeZone2);
        i = z.toInstant();

        assertNull(bCalendar.plusNonBusinessDays(d, 0));
        assertNull(bCalendar.plusNonBusinessDays(s, 0));
        assertNull(bCalendar.plusNonBusinessDays(z, 0));
        assertNull(bCalendar.plusNonBusinessDays(i, 0));

        assertNull(bCalendar.plusNonBusinessDays((LocalDate) null, 1));
        assertNull(bCalendar.plusNonBusinessDays((String) null, 1));
        assertNull(bCalendar.plusNonBusinessDays((ZonedDateTime) null, 1));
        assertNull(bCalendar.plusNonBusinessDays((Instant) null, 1));

        assertNull(bCalendar.plusNonBusinessDays(d, NULL_INT));
        assertNull(bCalendar.plusNonBusinessDays(s, NULL_INT));
        assertNull(bCalendar.plusNonBusinessDays(z.withZoneSameInstant(timeZone), NULL_INT));
        assertNull(bCalendar.plusNonBusinessDays(i, NULL_INT));
    }

    public void testMinusNonBusinessDays() {
        // 2023-07-03 normal day
        // 2023-07-04 holiday
        // 2023-07-05 weekend
        // 2023-07-06 half day
        // 2023-07-07 normal day
        // 2023-07-08 normal day
        // 2023-07-09 normal day
        // 2023-07-10 normal day
        // 2023-07-11 normal day
        // 2023-07-12 weekend
        // 2023-07-13 weekend
        // 2023-07-14 normal day
        // 2023-07-15 normal day

        final ZoneId timeZone2 = ZoneId.of("America/New_York");
        LocalDate d = LocalDate.of(2023, 7, 2);
        String s = d.toString();
        ZonedDateTime z = d.atTime(6, 25).atZone(timeZone2);
        Instant i = z.toInstant();

        assertNull(bCalendar.minusNonBusinessDays(d, 0));
        assertNull(bCalendar.minusNonBusinessDays(s, 0));
        assertNull(bCalendar.minusNonBusinessDays(z, 0));
        assertNull(bCalendar.minusNonBusinessDays(i, 0));

        final LocalDate d1 = LocalDate.of(2023, 7, 4);
        final Instant i1 = d1.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z1 = d1.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d1, bCalendar.minusNonBusinessDays(d, -1));
        assertEquals(d1.toString(), bCalendar.minusNonBusinessDays(s, -1));
        assertEquals(z1, bCalendar.minusNonBusinessDays(z, -1));
        assertEquals(i1, bCalendar.minusNonBusinessDays(i, -1));

        final LocalDate d2 = LocalDate.of(2023, 7, 5);
        final Instant i2 = d2.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z2 = d2.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d2, bCalendar.minusNonBusinessDays(d, -2));
        assertEquals(d2.toString(), bCalendar.minusNonBusinessDays(s, -2));
        assertEquals(z2, bCalendar.minusNonBusinessDays(z, -2));
        assertEquals(i2, bCalendar.minusNonBusinessDays(i, -2));

        final LocalDate d3 = LocalDate.of(2023, 7, 13);
        final Instant i3 = d3.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z3 = d3.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d3, bCalendar.minusNonBusinessDays(d, -4));
        assertEquals(d3.toString(), bCalendar.minusNonBusinessDays(s, -4));
        assertEquals(z3, bCalendar.minusNonBusinessDays(z, -4));
        assertEquals(i3, bCalendar.minusNonBusinessDays(i, -4));

        d = LocalDate.of(2023, 7, 14);
        s = d.toString();
        z = d.atTime(6, 25).atZone(timeZone2);
        i = z.toInstant();

        assertNull(bCalendar.minusNonBusinessDays(d, 0));
        assertNull(bCalendar.minusNonBusinessDays(s, 0));
        assertNull(bCalendar.minusNonBusinessDays(z, 0));
        assertNull(bCalendar.minusNonBusinessDays(i, 0));

        final LocalDate d4 = LocalDate.of(2023, 7, 13);
        final Instant i4 = d4.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z4 = d4.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d4, bCalendar.minusNonBusinessDays(d, 1));
        assertEquals(d4.toString(), bCalendar.minusNonBusinessDays(s, 1));
        assertEquals(z4, bCalendar.minusNonBusinessDays(z, 1));
        assertEquals(i4, bCalendar.minusNonBusinessDays(i, 1));

        final LocalDate d5 = LocalDate.of(2023, 7, 12);
        final Instant i5 = d5.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z5 = d5.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d5, bCalendar.minusNonBusinessDays(d, 2));
        assertEquals(d5.toString(), bCalendar.minusNonBusinessDays(s, 2));
        assertEquals(z5, bCalendar.minusNonBusinessDays(z, 2));
        assertEquals(i5, bCalendar.minusNonBusinessDays(i, 2));

        final LocalDate d6 = LocalDate.of(2023, 7, 4);
        final Instant i6 = d6.atTime(6, 25).atZone(timeZone2).toInstant();
        final ZonedDateTime z6 = d6.atTime(6, 25).atZone(timeZone2).withZoneSameInstant(timeZone);
        assertEquals(d6, bCalendar.minusNonBusinessDays(d, 4));
        assertEquals(d6.toString(), bCalendar.minusNonBusinessDays(s, 4));
        assertEquals(z6, bCalendar.minusNonBusinessDays(z, 4));
        assertEquals(i6, bCalendar.minusNonBusinessDays(i, 4));

        d = LocalDate.of(2023, 7, 2);
        s = d.toString();
        z = d.atTime(6, 24).atZone(timeZone2);
        i = z.toInstant();

        assertNull(bCalendar.minusNonBusinessDays(d, 0));
        assertNull(bCalendar.minusNonBusinessDays(s, 0));
        assertNull(bCalendar.minusNonBusinessDays(z, 0));
        assertNull(bCalendar.minusNonBusinessDays(i, 0));

        assertNull(bCalendar.minusNonBusinessDays((LocalDate) null, 1));
        assertNull(bCalendar.minusNonBusinessDays((String) null, 1));
        assertNull(bCalendar.minusNonBusinessDays((ZonedDateTime) null, 1));
        assertNull(bCalendar.minusNonBusinessDays((Instant) null, 1));

        assertNull(bCalendar.minusNonBusinessDays(d, NULL_INT));
        assertNull(bCalendar.minusNonBusinessDays(s, NULL_INT));
        assertNull(bCalendar.minusNonBusinessDays(z.withZoneSameInstant(timeZone), NULL_INT));
        assertNull(bCalendar.minusNonBusinessDays(i, NULL_INT));
    }

    public void testFutureBusinessDate() {
        assertEquals(bCalendar.plusBusinessDays(bCalendar.calendarDate(), 3),
                bCalendar.futureBusinessDate(3));
        assertEquals(bCalendar.plusBusinessDays(bCalendar.calendarDate(), -3),
                bCalendar.futureBusinessDate(-3));
        assertNull(bCalendar.futureBusinessDate(NULL_INT));
    }

    public void testPastBusinessDate() {
        assertEquals(bCalendar.minusBusinessDays(bCalendar.calendarDate(), 3),
                bCalendar.pastBusinessDate(3));
        assertEquals(bCalendar.minusBusinessDays(bCalendar.calendarDate(), -3),
                bCalendar.pastBusinessDate(-3));
        assertNull(bCalendar.pastBusinessDate(NULL_INT));
    }

    public void testFutureNonBusinessDate() {
        assertEquals(bCalendar.plusNonBusinessDays(bCalendar.calendarDate(), 3),
                bCalendar.futureNonBusinessDate(3));
        assertEquals(bCalendar.plusNonBusinessDays(bCalendar.calendarDate(), -3),
                bCalendar.futureNonBusinessDate(-3));
        assertNull(bCalendar.futureNonBusinessDate(NULL_INT));
    }

    public void testPastNonBusinessDate() {
        assertEquals(bCalendar.minusNonBusinessDays(bCalendar.calendarDate(), 3),
                bCalendar.pastNonBusinessDate(3));
        assertEquals(bCalendar.minusNonBusinessDays(bCalendar.calendarDate(), -3),
                bCalendar.pastNonBusinessDate(-3));
        assertNull(bCalendar.pastNonBusinessDate(NULL_INT));
    }

    public void testClearCache() {
        final LocalDate start = LocalDate.of(2023, 7, 3);
        final LocalDate end = LocalDate.of(2025, 7, 10);

        final CalendarDay<Instant> v1 = bCalendar.calendarDay();
        final CalendarDay<Instant> v2 = bCalendar.calendarDay();
        assertEquals(v1, v2);
        final int i1 = bCalendar.numberBusinessDates(start, end);
        final int i2 = bCalendar.numberBusinessDates(start, end);
        assertEquals(i1, i2);

        bCalendar.clearCache();

        final CalendarDay<Instant> v3 = bCalendar.calendarDay();
        final CalendarDay<Instant> v4 = bCalendar.calendarDay();
        assertEquals(v3, v4);
        final int i3 = bCalendar.numberBusinessDates(start, end);
        final int i4 = bCalendar.numberBusinessDates(start, end);
        assertEquals(i3, i4);
    }

}
