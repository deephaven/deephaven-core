/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.time.DateTimeUtils;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings({"ConstantConditions", "unchecked", "rawtypes"})
public class TestBusinessCalendar extends TestCalendar {
    private final LocalDate firstValidDate = LocalDate.of(2000, 1, 1);
    private final LocalDate lastValidDate = LocalDate.of(2050, 12, 31);
    private final BusinessPeriod<LocalTime> period = new BusinessPeriod<>(LocalTime.of(9, 0), LocalTime.of(12, 15));
    private final BusinessPeriod<LocalTime> periodHalf = new BusinessPeriod<>(LocalTime.of(9, 0), LocalTime.of(11, 7));
    private final BusinessSchedule<LocalTime> schedule = new BusinessSchedule<>(new BusinessPeriod[]{period});
    private final Set<DayOfWeek> weekendDays = Set.of(DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY);
    private final LocalDate holidayDate1 = LocalDate.of(2023, 7, 4);
    private final LocalDate holidayDate2 = LocalDate.of(2023, 12, 25);
    private final BusinessSchedule<Instant> holiday = new BusinessSchedule<>();
    private final LocalDate halfDayDate = LocalDate.of(2023, 7, 6);
    private final BusinessSchedule<Instant> halfDay = new BusinessSchedule<>(new BusinessPeriod[]{BusinessPeriod.toInstant(periodHalf, halfDayDate, timeZone)});

    private Map<LocalDate, BusinessSchedule<Instant>> holidays;
    private BusinessCalendar bCalendar;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        holidays = new HashMap<>();
        holidays.put(holidayDate1, holiday);
        holidays.put(holidayDate2, holiday);
        holidays.put(halfDayDate, halfDay);

        bCalendar = new BusinessCalendar(name, description, timeZone, firstValidDate, lastValidDate, schedule, weekendDays, holidays);
        calendar = bCalendar;
    }

    public void testBusinessGetters() {
        assertEquals(schedule, bCalendar.standardBusinessSchedule());
        assertEquals(schedule.businessNanos(), bCalendar.standardBusinessNanos());
        assertEquals(holidays, bCalendar.holidays());
        //TODO: implement
//        assertEquals(firstValidDate, bCalendar.firstValidDate());
//        assertEquals(lastValidDate, bCalendar.lastValidDate());
    }

    public void testBusinessSchedule() {
        assertEquals(holiday, bCalendar.businessSchedule(holidayDate1));
        assertEquals(holiday, bCalendar.businessSchedule(holidayDate2));
        assertEquals(halfDay, bCalendar.businessSchedule(halfDayDate));

        // TUES - Weekday
        LocalDate date = LocalDate.of(2023, 7, 11);
        assertEquals(BusinessSchedule.toInstant(schedule, date, timeZone), bCalendar.businessSchedule(date));
        assertEquals(BusinessSchedule.toInstant(schedule, date, timeZone), bCalendar.businessSchedule(date.toString()));
        assertEquals(BusinessSchedule.toInstant(schedule, date, timeZone), bCalendar.businessSchedule(date.atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(BusinessSchedule.toInstant(schedule, date, timeZone), bCalendar.businessSchedule(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // WED - Weekend
        date = LocalDate.of(2023, 7, 12);
        assertEquals(BusinessSchedule.toInstant(BusinessSchedule.HOLIDAY, date, timeZone), bCalendar.businessSchedule(date));
        assertEquals(BusinessSchedule.toInstant(BusinessSchedule.HOLIDAY, date, timeZone), bCalendar.businessSchedule(date.toString()));
        assertEquals(BusinessSchedule.toInstant(BusinessSchedule.HOLIDAY, date, timeZone), bCalendar.businessSchedule(date.atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(BusinessSchedule.toInstant(BusinessSchedule.HOLIDAY, date, timeZone), bCalendar.businessSchedule(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // THURS - Weekend
        date = LocalDate.of(2023, 7, 13);
        assertEquals(BusinessSchedule.toInstant(BusinessSchedule.HOLIDAY, date, timeZone), bCalendar.businessSchedule(date));
        assertEquals(BusinessSchedule.toInstant(BusinessSchedule.HOLIDAY, date, timeZone), bCalendar.businessSchedule(date.toString()));
        assertEquals(BusinessSchedule.toInstant(BusinessSchedule.HOLIDAY, date, timeZone), bCalendar.businessSchedule(date.atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(BusinessSchedule.toInstant(BusinessSchedule.HOLIDAY, date, timeZone), bCalendar.businessSchedule(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // FRI - Weekday
        date = LocalDate.of(2023, 7, 14);
        assertEquals(BusinessSchedule.toInstant(schedule, date, timeZone), bCalendar.businessSchedule(date));
        assertEquals(BusinessSchedule.toInstant(schedule, date, timeZone), bCalendar.businessSchedule(date.toString()));
        assertEquals(BusinessSchedule.toInstant(schedule, date, timeZone), bCalendar.businessSchedule(date.atTime(1, 2, 3).atZone(timeZone)));
        assertEquals(BusinessSchedule.toInstant(schedule, date, timeZone), bCalendar.businessSchedule(date.atTime(1, 2, 3).atZone(timeZone).toInstant()));

        // Current date
        assertEquals(bCalendar.businessSchedule(bCalendar.currentDate()), bCalendar.businessSchedule());

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            assertNotNull(bCalendar.businessSchedule(d));
        }

        try {
            bCalendar.businessSchedule(firstValidDate.minusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        try {
            bCalendar.businessSchedule(lastValidDate.plusDays(1));
            fail("should throw an exception");
        } catch (BusinessCalendar.InvalidDateException ignored) {
        }

        try {
            bCalendar.businessSchedule((LocalDate) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.businessSchedule((String) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.businessSchedule((ZonedDateTime) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.businessSchedule((Instant) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }
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
        assertEquals(bCalendar.isBusinessDay(bCalendar.currentDate()), bCalendar.isBusinessDay());

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

        try {
            bCalendar.isBusinessDay((LocalDate) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isBusinessDay((String) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isBusinessDay((ZonedDateTime) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isBusinessDay((Instant) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isBusinessDay((DayOfWeek) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }
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
        assertEquals(bCalendar.isLastBusinessDayOfMonth(bCalendar.currentDate()), bCalendar.isLastBusinessDayOfMonth());

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

        try {
            bCalendar.isLastBusinessDayOfMonth((LocalDate) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfMonth((String) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfMonth((ZonedDateTime) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfMonth((Instant) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }
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
        final BusinessCalendar bc = new BusinessCalendar(name, description, timeZone, firstValidDate, lastValidDate, schedule, wd, holidays);

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
        assertEquals(bCalendar.isLastBusinessDayOfWeek(bCalendar.currentDate()), bCalendar.isLastBusinessDayOfWeek());

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

        try {
            bCalendar.isLastBusinessDayOfWeek((LocalDate) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfWeek((String) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfWeek((ZonedDateTime) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfWeek((Instant) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }
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
        final BusinessCalendar bc = new BusinessCalendar(name, description, timeZone, firstValidDate, lastValidDate, schedule, wd, holidays);

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
        assertEquals(bCalendar.isLastBusinessDayOfYear(bCalendar.currentDate()), bCalendar.isLastBusinessDayOfYear());

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

        try {
            bCalendar.isLastBusinessDayOfYear((LocalDate) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfYear((String) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfYear((ZonedDateTime) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isLastBusinessDayOfYear((Instant) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }
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

        try {
            bCalendar.isBusinessTime((ZonedDateTime) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.isBusinessTime((Instant) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }
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
        assertEquals((double) halfDay.businessNanos() / (double) schedule.businessNanos(), bCalendar.fractionStandardBusinessDay(date));
        assertEquals((double) halfDay.businessNanos() / (double) schedule.businessNanos(), bCalendar.fractionStandardBusinessDay(date.toString()));
        assertEquals((double) halfDay.businessNanos() / (double) schedule.businessNanos(), bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone)));
        assertEquals((double) halfDay.businessNanos() / (double) schedule.businessNanos(), bCalendar.fractionStandardBusinessDay(date.atTime(11, 23).atZone(timeZone).toInstant()));

        // Current date
        assertEquals(bCalendar.fractionStandardBusinessDay(bCalendar.currentDate()), bCalendar.fractionStandardBusinessDay());

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            bCalendar.fractionStandardBusinessDay(d);
        }

        try {
            bCalendar.fractionStandardBusinessDay((LocalDate) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.fractionStandardBusinessDay((String) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.fractionStandardBusinessDay((ZonedDateTime) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.fractionStandardBusinessDay((Instant) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }
    }


    public void testFractionBusinessDayComplete() {
        // Normal bus day
        LocalDate date = LocalDate.of(2023, 7, 11);
        ZonedDateTime tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        ZonedDateTime tIn = date.atTime(10, 2, 3).atZone(timeZone);
        ZonedDateTime tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(0.0, bCalendar.fractionBusinessDayComplete(tBefore));
        assertEquals(0.0, bCalendar.fractionBusinessDayComplete(tBefore.toInstant()));
        assertEquals((double) bCalendar.businessSchedule(date).businessNanosElapsed(tIn.toInstant()) / (double) schedule.businessNanos(), bCalendar.fractionBusinessDayComplete(tIn));
        assertEquals((double) bCalendar.businessSchedule(date).businessNanosElapsed(tIn.toInstant()) / (double) schedule.businessNanos(), bCalendar.fractionBusinessDayComplete(tIn.toInstant()));
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
        assertEquals((double) bCalendar.businessSchedule(date).businessNanosElapsed(tIn.toInstant()) / (double) halfDay.businessNanos(), bCalendar.fractionBusinessDayComplete(tIn));
        assertEquals((double) bCalendar.businessSchedule(date).businessNanosElapsed(tIn.toInstant()) / (double) halfDay.businessNanos(), bCalendar.fractionBusinessDayComplete(tIn.toInstant()));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter));
        assertEquals(1.0, bCalendar.fractionBusinessDayComplete(tAfter.toInstant()));

        // Current date
        assertEquals(bCalendar.fractionBusinessDayComplete(DateTimeUtils.now()), bCalendar.fractionBusinessDayComplete(), 1e-5);

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            bCalendar.fractionBusinessDayComplete(d.atTime(12, 34).atZone(timeZone));
        }

        try {
            bCalendar.fractionBusinessDayComplete((ZonedDateTime) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.fractionBusinessDayComplete((Instant) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }
    }

    public void testFractionBusinessDayRemaining() {
        // Normal bus day
        LocalDate date = LocalDate.of(2023, 7, 11);
        ZonedDateTime tBefore = date.atTime(8, 2, 3).atZone(timeZone);
        ZonedDateTime tIn = date.atTime(10, 2, 3).atZone(timeZone);
        ZonedDateTime tAfter = date.atTime(22, 2, 3).atZone(timeZone);
        assertEquals(1.0, bCalendar.fractionBusinessDayRemaining(tBefore));
        assertEquals(1.0, bCalendar.fractionBusinessDayRemaining(tBefore.toInstant()));
        assertEquals(1 - (double) bCalendar.businessSchedule(date).businessNanosElapsed(tIn.toInstant()) / (double) schedule.businessNanos(), bCalendar.fractionBusinessDayRemaining(tIn));
        assertEquals(1 - (double) bCalendar.businessSchedule(date).businessNanosElapsed(tIn.toInstant()) / (double) schedule.businessNanos(), bCalendar.fractionBusinessDayRemaining(tIn.toInstant()));
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
        assertEquals(1 - (double) bCalendar.businessSchedule(date).businessNanosElapsed(tIn.toInstant()) / (double) halfDay.businessNanos(), bCalendar.fractionBusinessDayRemaining(tIn));
        assertEquals(1 - (double) bCalendar.businessSchedule(date).businessNanosElapsed(tIn.toInstant()) / (double) halfDay.businessNanos(), bCalendar.fractionBusinessDayRemaining(tIn.toInstant()));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter));
        assertEquals(0.0, bCalendar.fractionBusinessDayRemaining(tAfter.toInstant()));

        // Current date
        assertEquals(bCalendar.fractionBusinessDayRemaining(DateTimeUtils.now()), bCalendar.fractionBusinessDayRemaining(), 1e-5);

        // Check that all dates in the range work
        for (LocalDate d = firstValidDate; !d.isAfter(lastValidDate); d = d.plusDays(1)) {
            bCalendar.fractionBusinessDayRemaining(d.atTime(12, 34).atZone(timeZone));
        }

        try {
            bCalendar.fractionBusinessDayRemaining((ZonedDateTime) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }

        try {
            bCalendar.fractionBusinessDayRemaining((Instant) null);
            fail("should throw an exception");
        } catch (RequirementFailure ignored) {
        }
    }

    public void testBusinessDates() {

        final LocalDate start = LocalDate.of(2023,7,3);
        final LocalDate end = LocalDate.of(2023,7,15);

//        final LocalDate[] nonBus = {
//                holidayDate1, // Holiday 2023-07-04
//                LocalDate.of(2023, 7, 5), // WED
//                // halfDayDate, // Half Day 2023-07-06 --> is a business day
//                LocalDate.of(2023, 7, 12), // WED
//                LocalDate.of(2023, 7, 13), // THURS
//        } ;

        final LocalDate[] bus = {
                LocalDate.of(2023,7,3),
                LocalDate.of(2023,7,6),
                LocalDate.of(2023,7,7),
                LocalDate.of(2023,7,8),
                LocalDate.of(2023,7,9),
                LocalDate.of(2023,7,10),
                LocalDate.of(2023,7,11),
                LocalDate.of(2023,7,14),
                LocalDate.of(2023,7,15),
        };

        assertEquals(bus,bCalendar.businessDates(start, end));
        assertEquals(bus,bCalendar.businessDates(start.toString(), end.toString()));
        assertEquals(bus,bCalendar.businessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone)));
        assertEquals(bus,bCalendar.businessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant()));

        assertEquals(Arrays.copyOfRange(bus,0,bus.length-1),bCalendar.businessDates(start, end, true, false));
        assertEquals(Arrays.copyOfRange(bus,0,bus.length-1),bCalendar.businessDates(start.toString(), end.toString(), true, false));
        assertEquals(Arrays.copyOfRange(bus,0,bus.length-1),bCalendar.businessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), true, false));
        assertEquals(Arrays.copyOfRange(bus,0,bus.length-1),bCalendar.businessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), true, false));

        assertEquals(Arrays.copyOfRange(bus,1,bus.length),bCalendar.businessDates(start, end, false, true));
        assertEquals(Arrays.copyOfRange(bus,1,bus.length),bCalendar.businessDates(start.toString(), end.toString(), false, true));
        assertEquals(Arrays.copyOfRange(bus,1,bus.length),bCalendar.businessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), false, true));
        assertEquals(Arrays.copyOfRange(bus,1,bus.length),bCalendar.businessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), false, true));

        assertEquals(Arrays.copyOfRange(bus,1,bus.length-1),bCalendar.businessDates(start, end, false, false));
        assertEquals(Arrays.copyOfRange(bus,1,bus.length-1),bCalendar.businessDates(start.toString(), end.toString(), false, false));
        assertEquals(Arrays.copyOfRange(bus,1,bus.length-1),bCalendar.businessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), false, false));
        assertEquals(Arrays.copyOfRange(bus,1,bus.length-1),bCalendar.businessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), false, false));
    }

    public void testNumberBusinessDates() {

        final LocalDate start = LocalDate.of(2023,7,3);
        final LocalDate end = LocalDate.of(2023,7,15);

//        final LocalDate[] nonBus = {
//                holidayDate1, // Holiday 2023-07-04
//                LocalDate.of(2023, 7, 5), // WED
//                // halfDayDate, // Half Day 2023-07-06 --> is a business day
//                LocalDate.of(2023, 7, 12), // WED
//                LocalDate.of(2023, 7, 13), // THURS
//        } ;

        final LocalDate[] bus = {
                LocalDate.of(2023,7,3),
                LocalDate.of(2023,7,6),
                LocalDate.of(2023,7,7),
                LocalDate.of(2023,7,8),
                LocalDate.of(2023,7,9),
                LocalDate.of(2023,7,10),
                LocalDate.of(2023,7,11),
                LocalDate.of(2023,7,14),
                LocalDate.of(2023,7,15),
        };

        assertEquals(bus.length,bCalendar.numberBusinessDates(start, end));
        assertEquals(bus.length,bCalendar.numberBusinessDates(start.toString(), end.toString()));
        assertEquals(bus.length,bCalendar.numberBusinessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone)));
        assertEquals(bus.length,bCalendar.numberBusinessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant()));

        assertEquals(bus.length-1,bCalendar.numberBusinessDates(start, end, true, false));
        assertEquals(bus.length-1,bCalendar.numberBusinessDates(start.toString(), end.toString(), true, false));
        assertEquals(bus.length-1,bCalendar.numberBusinessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), true, false));
        assertEquals(bus.length-1,bCalendar.numberBusinessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), true, false));

        assertEquals(bus.length-1,bCalendar.numberBusinessDates(start, end, false, true));
        assertEquals(bus.length-1,bCalendar.numberBusinessDates(start.toString(), end.toString(), false, true));
        assertEquals(bus.length-1,bCalendar.numberBusinessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), false, true));
        assertEquals(bus.length-1,bCalendar.numberBusinessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), false, true));

        assertEquals(bus.length-2,bCalendar.numberBusinessDates(start, end, false, false));
        assertEquals(bus.length-2,bCalendar.numberBusinessDates(start.toString(), end.toString(), false, false));
        assertEquals(bus.length-2,bCalendar.numberBusinessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), false, false));
        assertEquals(bus.length-2,bCalendar.numberBusinessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), false, false));
    }

    public void testNonBusinessDates() {

        final LocalDate start = LocalDate.of(2023,7,3);
        final LocalDate end = LocalDate.of(2023,7,15);

        final LocalDate[] nonBus = {
                holidayDate1, // Holiday 2023-07-04
                LocalDate.of(2023, 7, 5), // WED
                // halfDayDate, // Half Day 2023-07-06 --> is a business day
                LocalDate.of(2023, 7, 12), // WED
                LocalDate.of(2023, 7, 13), // THURS
        } ;

//        final LocalDate[] bus = {
//                LocalDate.of(2023,7,3),
//                LocalDate.of(2023,7,6),
//                LocalDate.of(2023,7,7),
//                LocalDate.of(2023,7,8),
//                LocalDate.of(2023,7,9),
//                LocalDate.of(2023,7,10),
//                LocalDate.of(2023,7,11),
//                LocalDate.of(2023,7,14),
//                LocalDate.of(2023,7,15),
//        };

        assertEquals(nonBus,bCalendar.nonBusinessDates(start, end));
        assertEquals(nonBus,bCalendar.nonBusinessDates(start.toString(), end.toString()));
        assertEquals(nonBus,bCalendar.nonBusinessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone)));
        assertEquals(nonBus,bCalendar.nonBusinessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant()));

        assertEquals(Arrays.copyOfRange(nonBus,0,nonBus.length-1),bCalendar.nonBusinessDates(nonBus[0], nonBus[nonBus.length-1], true, false));
        assertEquals(Arrays.copyOfRange(nonBus,0,nonBus.length-1),bCalendar.nonBusinessDates(nonBus[0].toString(), nonBus[nonBus.length-1].toString(), true, false));
        assertEquals(Arrays.copyOfRange(nonBus,0,nonBus.length-1),bCalendar.nonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone), true, false));
        assertEquals(Arrays.copyOfRange(nonBus,0,nonBus.length-1),bCalendar.nonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone).toInstant(), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone).toInstant(), true, false));

        assertEquals(Arrays.copyOfRange(nonBus,1,nonBus.length),bCalendar.nonBusinessDates(nonBus[0], nonBus[nonBus.length-1], false, true));
        assertEquals(Arrays.copyOfRange(nonBus,1,nonBus.length),bCalendar.nonBusinessDates(nonBus[0].toString(), nonBus[nonBus.length-1].toString(), false, true));
        assertEquals(Arrays.copyOfRange(nonBus,1,nonBus.length),bCalendar.nonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone), false, true));
        assertEquals(Arrays.copyOfRange(nonBus,1,nonBus.length),bCalendar.nonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone).toInstant(), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone).toInstant(), false, true));

        assertEquals(Arrays.copyOfRange(nonBus,1,nonBus.length-1),bCalendar.nonBusinessDates(nonBus[0], nonBus[nonBus.length-1], false, false));
        assertEquals(Arrays.copyOfRange(nonBus,1,nonBus.length-1),bCalendar.nonBusinessDates(nonBus[0].toString(), nonBus[nonBus.length-1].toString(), false, false));
        assertEquals(Arrays.copyOfRange(nonBus,1,nonBus.length-1),bCalendar.nonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone), false, false));
        assertEquals(Arrays.copyOfRange(nonBus,1,nonBus.length-1),bCalendar.nonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone).toInstant(), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone).toInstant(), false, false));
    }

    public void testNumberNonBusinessDates() {

        final LocalDate start = LocalDate.of(2023,7,3);
        final LocalDate end = LocalDate.of(2023,7,15);

        final LocalDate[] nonBus = {
                holidayDate1, // Holiday 2023-07-04
                LocalDate.of(2023, 7, 5), // WED
                // halfDayDate, // Half Day 2023-07-06 --> is a business day
                LocalDate.of(2023, 7, 12), // WED
                LocalDate.of(2023, 7, 13), // THURS
        } ;

//        final LocalDate[] bus = {
//                LocalDate.of(2023,7,3),
//                LocalDate.of(2023,7,6),
//                LocalDate.of(2023,7,7),
//                LocalDate.of(2023,7,8),
//                LocalDate.of(2023,7,9),
//                LocalDate.of(2023,7,10),
//                LocalDate.of(2023,7,11),
//                LocalDate.of(2023,7,14),
//                LocalDate.of(2023,7,15),
//        };

        assertEquals(nonBus.length,bCalendar.numberNonBusinessDates(start, end));
        assertEquals(nonBus.length,bCalendar.numberNonBusinessDates(start.toString(), end.toString()));
        assertEquals(nonBus.length,bCalendar.numberNonBusinessDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone)));
        assertEquals(nonBus.length,bCalendar.numberNonBusinessDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant()));

        assertEquals(nonBus.length-1,bCalendar.numberNonBusinessDates(nonBus[0], nonBus[nonBus.length-1], true, false));
        assertEquals(nonBus.length-1,bCalendar.numberNonBusinessDates(nonBus[0].toString(), nonBus[nonBus.length-1].toString(), true, false));
        assertEquals(nonBus.length-1,bCalendar.numberNonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone), true, false));
        assertEquals(nonBus.length-1,bCalendar.numberNonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone).toInstant(), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone).toInstant(), true, false));

        assertEquals(nonBus.length-1,bCalendar.numberNonBusinessDates(nonBus[0], nonBus[nonBus.length-1], false, true));
        assertEquals(nonBus.length-1,bCalendar.numberNonBusinessDates(nonBus[0].toString(), nonBus[nonBus.length-1].toString(), false, true));
        assertEquals(nonBus.length-1,bCalendar.numberNonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone), false, true));
        assertEquals(nonBus.length-1,bCalendar.numberNonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone).toInstant(), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone).toInstant(), false, true));

        assertEquals(nonBus.length-2,bCalendar.numberNonBusinessDates(nonBus[0], nonBus[nonBus.length-1], false, false));
        assertEquals(nonBus.length-2,bCalendar.numberNonBusinessDates(nonBus[0].toString(), nonBus[nonBus.length-1].toString(), false, false));
        assertEquals(nonBus.length-2,bCalendar.numberNonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone), false, false));
        assertEquals(nonBus.length-2,bCalendar.numberNonBusinessDates(nonBus[0].atTime(1,24).atZone(timeZone).toInstant(), nonBus[nonBus.length-1].atTime(1,24).atZone(timeZone).toInstant(), false, false));
    }

    public void testDiffBusinessNanos() {
        // Same day
        final ZonedDateTime zdt1 = LocalDate.of(2023,7,3).atTime(9,27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023,7,3).atTime(12,10).atZone(timeZone);

        assertEquals(zdt1.until(zdt2, ChronoUnit.NANOS), bCalendar.diffBusinessNanos(zdt1,zdt2));
        assertEquals(-zdt1.until(zdt2, ChronoUnit.NANOS), bCalendar.diffBusinessNanos(zdt2,zdt1));
        assertEquals(zdt1.until(zdt2, ChronoUnit.NANOS), bCalendar.diffBusinessNanos(zdt1.toInstant(),zdt2.toInstant()));
        assertEquals(-zdt1.until(zdt2, ChronoUnit.NANOS), bCalendar.diffBusinessNanos(zdt2.toInstant(),zdt1.toInstant()));

        // Multiple holidays
        final ZonedDateTime zdt3 = LocalDate.of(2023,7,8).atTime(12,54).atZone(timeZone);
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
    }

    public void testDiffNonBusinessNanos() {
        // Same day
        final ZonedDateTime zdt1 = LocalDate.of(2023,7,3).atTime(6,27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023,7,3).atTime(15,10).atZone(timeZone);

        assertEquals(DateTimeUtils.diffNanos(zdt1, zdt2) - bCalendar.diffBusinessNanos(zdt1,zdt2), bCalendar.diffNonBusinessNanos(zdt1,zdt2));
        assertEquals(- bCalendar.diffNonBusinessNanos(zdt1,zdt2), bCalendar.diffNonBusinessNanos(zdt2,zdt1));
        assertEquals(DateTimeUtils.diffNanos(zdt1, zdt2) - bCalendar.diffBusinessNanos(zdt1,zdt2), bCalendar.diffNonBusinessNanos(zdt1.toInstant(),zdt2.toInstant()));
        assertEquals(- bCalendar.diffNonBusinessNanos(zdt1,zdt2), bCalendar.diffNonBusinessNanos(zdt2.toInstant(),zdt1.toInstant()));

        // Multiple holidays
        final ZonedDateTime zdt3 = LocalDate.of(2023,7,8).atTime(12,54).atZone(timeZone);
        final long target = DateTimeUtils.diffNanos(zdt1, zdt2) - bCalendar.diffBusinessNanos(zdt1, zdt3);

        assertEquals(DateTimeUtils.diffNanos(zdt1, zdt3) - bCalendar.diffBusinessNanos(zdt1, zdt3), bCalendar.diffNonBusinessNanos(zdt1,zdt3));
        assertEquals(- bCalendar.diffNonBusinessNanos(zdt1,zdt3), bCalendar.diffNonBusinessNanos(zdt3,zdt1));
        assertEquals(DateTimeUtils.diffNanos(zdt1, zdt3) - bCalendar.diffBusinessNanos(zdt1, zdt3), bCalendar.diffNonBusinessNanos(zdt1.toInstant(),zdt3.toInstant()));
        assertEquals(- bCalendar.diffNonBusinessNanos(zdt1,zdt3), bCalendar.diffNonBusinessNanos(zdt3.toInstant(),zdt1.toInstant()));
    }

    public void testDiffBusinessDays() {
        final ZonedDateTime zdt1 = LocalDate.of(2023,7,3).atTime(6,27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023,7,10).atTime(15,10).atZone(timeZone);

        assertEquals(bCalendar.diffBusinessNanos(zdt1, zdt2)/(double)schedule.businessNanos(), bCalendar.diffBusinessDays(zdt1, zdt2));
        assertEquals(bCalendar.diffBusinessNanos(zdt1, zdt2)/(double)schedule.businessNanos(), bCalendar.diffBusinessDays(zdt1.toInstant(), zdt2.toInstant()));
    }

    public void testDiffBusinessYears() {
        final ZonedDateTime zdt1 = LocalDate.of(2023,1,1).atTime(6,27).atZone(timeZone);
        final ZonedDateTime zdt2 = LocalDate.of(2023,12,31).atTime(15,10).atZone(timeZone);

        assertEquals(1.0, bCalendar.diffBusinessYears(zdt1, zdt2));
        assertEquals(1.0, bCalendar.diffBusinessYears(zdt1.toInstant(), zdt2.toInstant()));

        final ZonedDateTime zdt3 = LocalDate.of(2024,12,31).atTime(15,10).atZone(timeZone);

        assertEquals(2.0, bCalendar.diffBusinessYears(zdt1, zdt3));
        assertEquals(2.0, bCalendar.diffBusinessYears(zdt1.toInstant(), zdt3.toInstant()));

        final ZonedDateTime zdt4 = LocalDate.of(2025,12,31).atTime(15,10).atZone(timeZone);

        assertEquals(3.0, bCalendar.diffBusinessYears(zdt1, zdt4));
        assertEquals(3.0, bCalendar.diffBusinessYears(zdt1.toInstant(), zdt4.toInstant()));

        final ZonedDateTime zdt5 = LocalDate.of(2022,12,1).atTime(15,10).atZone(timeZone);
        final ZonedDateTime zdt6 = LocalDate.of(2026,1,31).atTime(15,10).atZone(timeZone);
        final long length2022 = bCalendar.diffBusinessNanos(
                LocalDate.of(2022,1,1).atTime(0,0).atZone(timeZone),
                LocalDate.of(2022,12,31).atTime(23,59).atZone(timeZone)
        );
        final long length2026 = bCalendar.diffBusinessNanos(
                LocalDate.of(2026,1,1).atTime(0,0).atZone(timeZone),
                LocalDate.of(2026,12,31).atTime(23,59).atZone(timeZone)
        );
        final double start = bCalendar.diffBusinessNanos(zdt5, zdt1) / (double)length2022;
        final double end = bCalendar.diffBusinessNanos(zdt4, zdt6) / (double) length2026;

        assertEquals(3.0+start+end, bCalendar.diffBusinessYears(zdt5, zdt6), 1e-5);
        assertEquals(3.0+start+end, bCalendar.diffBusinessYears(zdt5.toInstant(), zdt6.toInstant()), 1e-5);
    }


    public void testFail() {
        fail();
    }


//    *** rename and implement

//    private static final ZoneId TZ_NY = ZoneId.of("America/New_York");
//    private static final ZoneId TZ_JP = ZoneId.of("Asia/Tokyo");
//    private static final ZoneId TZ_UTC = ZoneId.of("UTC");
//
//    private final BusinessCalendar USNYSE = Calendars.calendar("USNYSE");
//    private final BusinessCalendar JPOSE = Calendars.calendar("JPOSE");
//    private final BusinessCalendar UTC = Calendars.calendar("UTC");
//
//    private final String curDay = "2017-09-27";
//    private File testCal;
//    private BusinessCalendar test;
//
//    @Override
//    public void setUp() throws Exception {
//        super.setUp();
//        testCal = File.createTempFile("Test", ".calendar");
//        final FileWriter fw = new FileWriter(testCal);
//        fw.write("<!--\n" +
//                "  ~ Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
//                "  -->\n" +
//                "\n" +
//                "<calendar>\n" +
//                "    <name>TEST</name>\n" +
//                "    <timeZone>NY</timeZone>\n" +
//                "    <language>en</language>\n" +
//                "    <country>US</country>\n" +
//                "    <default>\n" +
//                "        <businessPeriod>09:30,16:00</businessPeriod>\n" +
//                "        <weekend>Saturday</weekend>\n" +
//                "        <weekend>Sunday</weekend>\n" +
//                "    </default>\n" +
//                "</calendar>");
//        fw.flush();
//        fw.close();
//
//
//        test = BusinessCalendarParser.loadBusinessCalendar(testCal);
////        test = new BusinessCalendarParser(BusinessCalendarParser.parseBusinessCalendarInputs(testCal)) {
////            @Override
////            public String currentDay() {
////                return curDay;
////            }
////        };
//    }
//
//    @Override
//    public void tearDown() throws Exception {
//        assertTrue(testCal.delete());
//        super.tearDown();
//    }
//
//    public void testName() {
//        Calendar cal = Calendars.calendar("USNYSE");
//        assertEquals(cal.name(), "USNYSE");
//    }

//    public void testNextDay() {
////        assertEquals("2017-09-28", test.futureDate());
//        assertEquals("2017-09-29", test.futureDate(2));
//        assertEquals("2017-10-11", test.futureDate(14));
//
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        String day2 = "2016-09-02";
//        assertEquals(USNYSE.futureDate(day1, 2), day2);
//        assertEquals(JPOSE.futureDate(day1, 2), day2);
//        assertEquals(USNYSE.futureDate(day2, -2), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(JPOSE.futureDate(day2, -2), DateTimeUtils.formatDate(day1, TZ_JP));
//
//        assertEquals(USNYSE.futureDate(day1, 0), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(JPOSE.futureDate(day1, 0), DateTimeUtils.formatDate(day1, TZ_JP));
//
//        // leap day
//        day1 = DateTimeUtils.parseInstant("2016-02-28T01:00:00.000000000 NY");
//        day2 = "2016-02-29";
//        assertEquals(USNYSE.futureDate(day1), day2);
//        assertEquals(JPOSE.futureDate(day1), day2);
//
//        // new year
//        day1 = DateTimeUtils.parseInstant("2013-12-31T01:00:00.000000000 NY");
//        day2 = "2014-01-05";
//        assertEquals(USNYSE.futureDate(day1, 5), day2);
//        assertEquals(JPOSE.futureDate(day1, 5), day2);
//        assertEquals(USNYSE.futureDate(day2, -5), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(JPOSE.futureDate(day2, -5), DateTimeUtils.formatDate(day1, TZ_JP));
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = DateTimeUtils.parseInstant("2017-03-12T01:00:00.000000000 NY");
//        day2 = "2017-03-13";
//        assertEquals(USNYSE.futureDate(day1), day2);
//        assertEquals(JPOSE.futureDate(day1), day2);
//
//        // outside calendar range
//        day1 = DateTimeUtils.parseInstant("2069-12-31T01:00:00.000000000 NY");
//        day2 = "2070-01-01";
//        assertEquals(USNYSE.futureDate(day1), day2);
//        assertEquals(JPOSE.futureDate(day1), day2);
//
//        day1 = null;
//        assertNull(USNYSE.futureDate(day1));
//        assertNull(JPOSE.futureDate(day1));
//    }
//
//    public void testNextDayString() {
//        String day1 = "2016-08-31";
//        String day2 = "2016-09-04";
//        assertEquals(USNYSE.futureDate(day1, 4), day2);
//        assertEquals(JPOSE.futureDate(day1, 4), day2);
//        assertEquals(USNYSE.futureDate(day2, -4), day1);
//        assertEquals(JPOSE.futureDate(day2, -4), day1);
//
//        assertEquals(USNYSE.futureDate(day1, 0), day1);
//        assertEquals(JPOSE.futureDate(day1, 0), day1);
//
//        // leap day
//        day1 = "2016-02-28";
//        day2 = "2016-02-29";
//        assertEquals(USNYSE.futureDate(day1), day2);
//        assertEquals(JPOSE.futureDate(day1), day2);
//
//        // new year
//        day1 = "2013-12-31";
//        day2 = "2014-01-01";
//        assertEquals(USNYSE.futureDate(day1), day2);
//        assertEquals(JPOSE.futureDate(day1), day2);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = "2017-03-12";
//        day2 = "2017-03-15";
//        assertEquals(USNYSE.futureDate(day1, 3), day2);
//        assertEquals(JPOSE.futureDate(day1, 3), day2);
//        assertEquals(USNYSE.futureDate(day2, -3), day1);
//        assertEquals(JPOSE.futureDate(day2, -3), day1);
//
//        day1 = null;
//        assertNull(USNYSE.futureDate(day1));
//        assertNull(JPOSE.futureDate(day1));
//
//
//        day1 = "2014-03-10";
//        day2 = "2017-03-13";
//        assertEquals(USNYSE.futureDate(day1, 1099), day2);
//
//        // incorrectly formatted days
//        try {
//            USNYSE.futureDate("2018-02-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//        try {
//            USNYSE.futureDate("20193-02-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testPreviousDay() {
//        assertEquals("2017-09-26", test.pastDate());
//        assertEquals("2017-09-25", test.pastDate(2));
//        assertEquals("2017-09-13", test.pastDate(14));
//
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        Instant day2 = DateTimeUtils.parseInstant("2016-09-01T01:00:00.000000000 NY");
//        assertEquals(USNYSE.pastDate(day2), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(JPOSE.pastDate(day2), DateTimeUtils.formatDate(day1, TZ_JP));
//
//        assertEquals(USNYSE.pastDate(day1, 0), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(JPOSE.pastDate(day1, 0), DateTimeUtils.formatDate(day1, TZ_JP));
//
//        // leap day
//        day1 = DateTimeUtils.parseInstant("2016-02-29T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2016-03-01T01:00:00.000000000 NY");
//        assertEquals(USNYSE.pastDate(day2), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(JPOSE.pastDate(day2), DateTimeUtils.formatDate(day1, TZ_JP));
//
//        // new year
//        day1 = DateTimeUtils.parseInstant("2013-12-29T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2014-01-01T01:00:00.000000000 NY");
//        assertEquals(USNYSE.pastDate(day2, 3), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(JPOSE.pastDate(day2, 3), DateTimeUtils.formatDate(day1, TZ_JP));
//        assertEquals(USNYSE.pastDate(day1, -3), DateTimeUtils.formatDate(day2, TZ_NY));
//        assertEquals(JPOSE.pastDate(day1, -3), DateTimeUtils.formatDate(day2, TZ_JP));
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2017-03-13T01:00:00.000000000 NY");
//        assertEquals(USNYSE.pastDate(day2, 2), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(JPOSE.pastDate(day2, 2), DateTimeUtils.formatDate(day1, TZ_JP));
//        assertEquals(USNYSE.pastDate(day1, -2), DateTimeUtils.formatDate(day2, TZ_NY));
//        assertEquals(JPOSE.pastDate(day1, -2), DateTimeUtils.formatDate(day2, TZ_JP));
//
//        day1 = null;
//        assertNull(USNYSE.pastDate(day1));
//        assertNull(JPOSE.pastDate(day1));
//    }
//
//    public void testPreviousDayString() {
//        String day1 = "2016-08-30";
//        String day2 = "2016-09-01";
//        assertEquals(USNYSE.pastDate(day2, 2), day1);
//        assertEquals(JPOSE.pastDate(day2, 2), day1);
//        assertEquals(USNYSE.pastDate(day1, -2), day2);
//        assertEquals(JPOSE.pastDate(day1, -2), day2);
//
//        assertEquals(USNYSE.pastDate(day1, 0), day1);
//        assertEquals(JPOSE.pastDate(day1, 0), day1);
//
//        // leap day
//        day1 = "2016-02-29";
//        day2 = "2016-03-01";
//        assertEquals(USNYSE.pastDate(day2), day1);
//        assertEquals(JPOSE.pastDate(day2), day1);
//
//        // new year
//        day1 = "2013-12-31";
//        day2 = "2014-01-01";
//        assertEquals(USNYSE.pastDate(day2), day1);
//        assertEquals(JPOSE.pastDate(day2), day1);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = "2017-03-10";
//        day2 = "2017-03-13";
//        assertEquals(USNYSE.pastDate(day2, 3), day1);
//        assertEquals(JPOSE.pastDate(day2, 3), day1);
//        assertEquals(USNYSE.pastDate(day1, -3), day2);
//        assertEquals(JPOSE.pastDate(day1, -3), day2);
//
//        day1 = null;
//        assertNull(USNYSE.pastDate(day1));
//        assertNull(JPOSE.pastDate(day1));
//
//        day1 = "2014-03-10";
//        day2 = "2017-03-13";
//        assertEquals(USNYSE.pastDate(day2, 1099), day1);
//
//        // incorrectly formatted days
//        try {
//            USNYSE.pastDate("2018-02-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//        try {
//            USNYSE.pastDate("20193-02-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testDateRange() {
//        // day light savings
//        Instant startDate = DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY");
//        Instant endDate = DateTimeUtils.parseInstant("2017-03-14T01:00:00.000000000 NY");
//
//        String[] goodResults = new String[] {
//                "2017-03-11",
//                "2017-03-12",
//                "2017-03-13",
//                "2017-03-14"
//        };
//
//        String[] results = USNYSE.calendarDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        boolean answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//
//        startDate = DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 JP");
//        endDate = DateTimeUtils.parseInstant("2017-03-14T01:00:00.000000000 JP");
//        results = JPOSE.calendarDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//
//        startDate = null;
//        assertEquals(USNYSE.calendarDates(startDate, endDate).length, 0);
//        assertEquals(JPOSE.calendarDates(startDate, endDate).length, 0);
//    }
//
//    public void testDateStringRange() {
//        String startDate = "2014-02-18";
//        String endDate = "2014-03-05";
//        String[] goodResults = new String[] {
//                "2014-02-18", "2014-02-19", "2014-02-20", "2014-02-21", "2014-02-22", "2014-02-23",
//                "2014-02-24", "2014-02-25", "2014-02-26", "2014-02-27", "2014-02-28",
//                "2014-03-01", "2014-03-02", "2014-03-03", "2014-03-04", "2014-03-05"
//        };
//
//        String[] results = USNYSE.calendarDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        boolean answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//        results = JPOSE.calendarDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//        startDate = "2020-01-01";
//        endDate = "2020-01-20";
//
//        results = USNYSE.calendarDates(startDate, endDate);
//        assertEquals(results.length, 20);
//
//        results = JPOSE.calendarDates(startDate, endDate);
//        assertEquals(results.length, 20);
//
//        startDate = null;
//        assertEquals(USNYSE.calendarDates(startDate, endDate).length, 0);
//        assertEquals(JPOSE.calendarDates(startDate, endDate).length, 0);
//
//
//        // incorrectly formatted days
//        assertEquals(new String[0], USNYSE.calendarDates("2018-02-31", "2019-02-31"));
//    }
//
//    public void testNumberOfDays() {
//        Instant startDate = DateTimeUtils.parseInstant("2014-02-18T01:00:00.000000000 NY");
//        Instant endDate = DateTimeUtils.parseInstant("2014-03-05T01:00:00.000000000 NY");
//
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), 15);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 11);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, false), 11);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, true), 12);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 4);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, false), 4);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, true), 4);
//
//
//        startDate = DateTimeUtils.parseInstant("2020-01-01T01:00:00.000000000 NY");
//        endDate = DateTimeUtils.parseInstant("2020-01-20T01:00:00.000000000 NY");
//
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), 19);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 12);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, true), 12);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 7);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, true), 8);
//
//        startDate = endDate;
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), 0);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 0);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, true), 0);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 0);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, true), 1);
//
//        startDate = DateTimeUtils.parseInstant("2020-01-02T01:00:00.000000000 NY");
//        endDate = startDate;
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), 0);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 0);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, true), 1);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 0);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, true), 0);
//
//        startDate = null;
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, true), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, true), QueryConstants.NULL_INT);
//
//        startDate = DateTimeUtils.parseInstant("2014-02-18T01:00:00.000000000 NY");
//        endDate = null;
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, true), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, true), QueryConstants.NULL_INT);
//
//        startDate = DateTimeUtils.parseInstant("2014-02-18T01:00:00.000000000 NY");
//        endDate = DateTimeUtils.parseInstant("2017-02-18T01:00:00.000000000 NY");
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), 1096);
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate, true), 1097);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 758);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, true), 758);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 338);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, true), 339);
//    }
//
//    public void testNumberOfDaysString() {
//        String startDate = "2014-02-18";
//        String endDate = "2014-03-05";
//
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), 15);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 11);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 4);
//
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate, false), 15);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, false), 11);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, false), 4);
//
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate, true), 16);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, true), 12);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, true), 4);
//
//        assertEquals(USNYSE.numberCalendarDates(endDate, startDate), -15);
//        assertEquals(USNYSE.numberOfBusinessDays(endDate, startDate), -11);
//        assertEquals(USNYSE.numberOfNonBusinessDays(endDate, startDate), -4);
//
//        assertEquals(USNYSE.numberCalendarDates(endDate, startDate, false), -15);
//        assertEquals(USNYSE.numberBusinessDates(endDate, startDate, false), -11);
//        assertEquals(USNYSE.numberNonBusinessDates(endDate, startDate, false), -4);
//
//        assertEquals(USNYSE.numberCalendarDates(endDate, startDate, true), -16);
//        assertEquals(USNYSE.numberBusinessDates(endDate, startDate, true), -12);
//        assertEquals(USNYSE.numberNonBusinessDates(endDate, startDate, true), -4);
//
//        endDate = startDate;
//
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), 0);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 0);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 0);
//
//
//        startDate = "2020-01-01";
//        endDate = "2020-01-20";
//
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), 19);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 12);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 7);
//
//        startDate = null;
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
//
//        startDate = "2014-02-18";
//        endDate = null;
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
//
//
//
//        startDate = "2014-02-18";
//        endDate = "2017-02-18";
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate), 1096);
//        assertEquals(USNYSE.numberCalendarDates(startDate, endDate, true), 1097);
//        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 758);
//        assertEquals(USNYSE.numberBusinessDates(startDate, endDate, true), 758);
//        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 338);
//        assertEquals(USNYSE.numberNonBusinessDates(startDate, endDate, true), 339);
//
//
//        // incorrectly formatted days
//        try {
//            USNYSE.numberCalendarDates("2018-02-31", "2019-02-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testIsBusinessDay() {
//        assertTrue(test.isBusinessDay());
//
//        Instant businessDay = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        Instant halfDay = DateTimeUtils.parseInstant("2014-07-03T01:00:00.000000000 NY");
//        Instant holiday = DateTimeUtils.parseInstant("2002-01-01T01:00:00.000000000 NY");
//        Instant holiday2 = DateTimeUtils.parseInstant("2002-01-21T01:00:00.000000000 NY");
//
//        assertTrue(USNYSE.isBusinessDay(businessDay));
//        assertTrue(USNYSE.isBusinessDay(halfDay));
//        assertFalse(USNYSE.isBusinessDay(holiday));
//        assertFalse(USNYSE.isBusinessDay(holiday2));
//
//        businessDay = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 JP");
//        halfDay = DateTimeUtils.parseInstant("2006-01-04T01:00:00.000000000 JP");
//        holiday = DateTimeUtils.parseInstant("2006-01-02T01:00:00.000000000 JP");
//        holiday2 = DateTimeUtils.parseInstant("2007-12-23T01:00:00.000000000 JP");
//
//        assertTrue(JPOSE.isBusinessDay(businessDay));
//        assertTrue(JPOSE.isBusinessDay(halfDay));
//        assertFalse(JPOSE.isBusinessDay(holiday));
//        assertFalse(JPOSE.isBusinessDay(holiday2));
//
//
//        businessDay = null;
//        // noinspection ConstantConditions
//        assertFalse(USNYSE.isBusinessDay(businessDay));
//        // noinspection ConstantConditions
//        assertFalse(JPOSE.isBusinessDay(businessDay));
//    }
//
//    public void testIsBusinessTime() {
//        Instant businessDayNotTime = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        Instant halfDayTime = DateTimeUtils.parseInstant("2014-07-03T12:00:00.000000000 NY");
//        Instant holiday = DateTimeUtils.parseInstant("2002-01-01T01:00:00.000000000 NY");
//        Instant holiday2 = DateTimeUtils.parseInstant("2002-01-21T01:00:00.000000000 NY");
//
//        assertFalse(USNYSE.isBusinessTime(businessDayNotTime));
//        assertTrue(USNYSE.isBusinessTime(halfDayTime));
//        assertFalse(USNYSE.isBusinessTime(holiday));
//        assertFalse(USNYSE.isBusinessTime(holiday2));
//
//        Instant businessDayTime = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 JP");
//        halfDayTime = DateTimeUtils.parseInstant("2006-01-04T11:00:00.000000000 JP");
//        holiday = DateTimeUtils.parseInstant("2006-01-02T01:00:00.000000000 JP");
//        holiday2 = DateTimeUtils.parseInstant("2007-12-23T01:00:00.000000000 JP");
//
//        assertFalse(JPOSE.isBusinessTime(businessDayTime));
//        assertTrue(JPOSE.isBusinessTime(halfDayTime));
//        assertFalse(JPOSE.isBusinessTime(holiday));
//        assertFalse(JPOSE.isBusinessTime(holiday2));
//
//
//        holiday = null;
//        // noinspection ConstantConditions
//        assertFalse(USNYSE.isBusinessTime(holiday));
//        // noinspection ConstantConditions
//        assertFalse(JPOSE.isBusinessTime(holiday));
//    }
//
//    public void testIsBusinessDayString() {
//        String businessDay = "2016-08-31";
//        String halfDay = "2014-07-03";
//        String holiday = "2002-01-01";
//        String holiday2 = "2002-01-21";
//
//        assertTrue(USNYSE.isBusinessDay(businessDay));
//        assertTrue(USNYSE.isBusinessDay(halfDay));
//        assertFalse(USNYSE.isBusinessDay(holiday));
//        assertFalse(USNYSE.isBusinessDay(holiday2));
//
//        businessDay = "2016-08-31";
//        halfDay = "2006-01-04";
//        holiday = "2007-09-17";
//        holiday2 = "2006-02-11";
//
//        assertTrue(JPOSE.isBusinessDay(businessDay));
//        assertTrue(JPOSE.isBusinessDay(halfDay));
//        assertFalse(JPOSE.isBusinessDay(holiday));
//        assertFalse(JPOSE.isBusinessDay(holiday2));
//
//        businessDay = null;
//        assertFalse(JPOSE.isBusinessDay(businessDay));
//        assertFalse(JPOSE.isBusinessDay(businessDay));
//
//
//        // incorrectly formatted days
//        try {
//            USNYSE.isBusinessDay("2018-09-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testNextBusinessDay() {
//        assertEquals("2017-09-28", test.futureBusinessDate());
//        assertEquals("2017-09-29", test.futureBusinessDate(2));
//        assertEquals("2017-10-17", test.futureBusinessDate(14));
//
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        Instant day1JP = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 JP");
//        String day2 = "2016-09-01";
//        assertNull(USNYSE.futureBusinessDate((Instant) null));
//        assertEquals(USNYSE.futureBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureBusinessDate(day1JP), day2);
//
//        assertNull(USNYSE.futureBusinessDate((Instant) null, 2));
//        assertEquals(USNYSE.futureBusinessDate(day1, 2), "2016-09-02");
//        assertEquals(JPOSE.futureBusinessDate(day1JP, 2), "2016-09-02");
//
//        assertEquals(USNYSE.futureBusinessDate(DateTimeUtils.parseInstant("2016-09-02T01:00:00.000000000 NY"), -2),
//                "2016-08-31");
//        assertEquals(JPOSE.futureBusinessDate(DateTimeUtils.parseInstant("2016-09-02T01:00:00.000000000 JP"), -2),
//                "2016-08-31");
//
//        assertEquals(USNYSE.futureBusinessDate(DateTimeUtils.parseInstant("2016-08-30T01:00:00.000000000 NY"), 0),
//                "2016-08-30");
//        assertNull(USNYSE.futureBusinessDate(DateTimeUtils.parseInstant("2016-08-28T01:00:00.000000000 NY"), 0));
//
//        // leap day
//        day1 = DateTimeUtils.parseInstant("2016-02-28T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2016-02-28T01:00:00.000000000 JP");
//        day2 = "2016-02-29";
//        assertEquals(USNYSE.futureBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureBusinessDate(day1JP), day2);
//
//        // new year
//        day1 = DateTimeUtils.parseInstant("2013-12-31T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2013-12-31T01:00:00.000000000 JP");
//        day2 = "2014-01-02";
//        assertEquals(USNYSE.futureBusinessDate(day1), day2);
//
//        day2 = "2014-01-01";
//        assertEquals(JPOSE.futureBusinessDate(day1JP), day2);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        // Japan doesn't observe day light savings
//        day1 = DateTimeUtils.parseInstant("2017-03-12T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2017-03-12T01:00:00.000000000 JP");
//        day2 = "2017-03-13";
//        assertEquals(USNYSE.futureBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureBusinessDate(day1JP), day2);
//
//        // outside calendar range, so no day off for new years, but weekend should still be off
//        day1 = DateTimeUtils.parseInstant("2069-12-31T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2069-12-31T01:00:00.000000000 JP");
//        day2 = "2070-01-01";
//        assertEquals(USNYSE.futureBusinessDate(day1).compareTo(day2), 0);
//        assertEquals(JPOSE.futureBusinessDate(day1JP), day2);
//
//        day1 = DateTimeUtils.parseInstant("2070-01-03T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2070-01-03T01:00:00.000000000 JP");
//        day2 = "2070-01-06";
//        assertEquals(USNYSE.futureBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureBusinessDate(day1JP), day2);
//
//        day1 = null;
//        assertNull(USNYSE.futureBusinessDate(day1));
//        assertNull(JPOSE.futureBusinessDate(day1));
//    }
//
//    public void testNextBusinessDayString() {
//        String day1 = "2016-08-31";
//        String day2 = "2016-09-01";
//        assertNull(USNYSE.futureBusinessDate((String) null));
//        assertEquals(USNYSE.futureBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureBusinessDate(day1), day2);
//
//        assertNull(USNYSE.futureBusinessDate((String) null, 2));
//        assertEquals(USNYSE.futureBusinessDate(day1, 2), "2016-09-02");
//        assertEquals(JPOSE.futureBusinessDate(day1, 2), "2016-09-02");
//
//        assertEquals(USNYSE.futureBusinessDate("2016-09-02", -2), "2016-08-31");
//        assertEquals(JPOSE.futureBusinessDate("2016-09-02", -2), "2016-08-31");
//
//        assertEquals(USNYSE.futureBusinessDate("2016-08-30", 0), "2016-08-30");
//        assertNull(USNYSE.futureBusinessDate("2016-08-28", 0));
//
//        // leap day
//        day1 = "2016-02-28";
//        day2 = "2016-02-29";
//        assertEquals(USNYSE.futureBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureBusinessDate(day1), day2);
//
//        // new year
//        day1 = "2013-12-31";
//        day2 = "2014-01-02";
//        assertEquals(USNYSE.futureBusinessDate(day1), day2);
//
//        day1 = "2007-01-01";
//        day2 = "2007-01-04";
//        assertEquals(JPOSE.futureBusinessDate(day1), day2);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = "2017-03-12";
//        day2 = "2017-03-13";
//        assertEquals(USNYSE.futureBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureBusinessDate(day1), day2);
//
//        day1 = null;
//        assertNull(USNYSE.futureBusinessDate(day1));
//        assertNull(JPOSE.futureBusinessDate(day1));
//
//        // incorrectly formatted days
//        try {
//            USNYSE.futureBusinessDate("2018-09-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testNextBusinessSchedule() {
//        assertEquals(test.nextBusinessSchedule(curDay), test.nextBusinessSchedule());
//        assertEquals(test.nextBusinessSchedule(curDay, 2), test.nextBusinessSchedule(2));
//
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        Instant day1JP = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 JP");
//        String day2 = "2016-09-01";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1).getSOBD(), TZ_NY), day2);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1JP).getSOBD(), TZ_JP), day2);
//
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1, 2).getSOBD(), TZ_NY), "2016-09-02");
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1JP, 2).getSOBD(), TZ_JP), "2016-09-02");
//
//        assertEquals(DateTimeUtils.formatDate(
//                USNYSE.nextBusinessSchedule(DateTimeUtils.parseInstant("2016-09-02T01:00:00.000000000 NY"), -2)
//                        .getSOBD(),
//                TZ_NY), "2016-08-31");
//        assertEquals(DateTimeUtils.formatDate(
//                JPOSE.nextBusinessSchedule(DateTimeUtils.parseInstant("2016-09-02T01:00:00.000000000 JP"), -2)
//                        .getSOBD(),
//                TZ_JP), "2016-08-31");
//
//        assertEquals(DateTimeUtils.formatDate(
//                USNYSE.nextBusinessSchedule(DateTimeUtils.parseInstant("2016-08-30T01:00:00.000000000 NY"), 0)
//                        .getSOBD(),
//                TZ_NY), "2016-08-30");
//
//        // leap day
//        day1 = DateTimeUtils.parseInstant("2016-02-28T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2016-02-28T01:00:00.000000000 JP");
//        day2 = "2016-02-29";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1).getSOBD(), TZ_NY), day2);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1JP).getSOBD(), TZ_JP), day2);
//
//        // new year
//        day1 = DateTimeUtils.parseInstant("2013-12-31T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2013-12-31T01:00:00.000000000 JP");
//        day2 = "2014-01-03";
//        assertEquals(
//                DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(USNYSE.futureBusinessDate(day1)).getSOBD(), TZ_NY),
//                day2);
//
//        day2 = "2014-01-01";
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1JP).getSOBD(), TZ_JP), day2);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        // Japan doesn't observe day light savings
//        day1 = DateTimeUtils.parseInstant("2017-03-12T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2017-03-12T01:00:00.000000000 JP");
//        day2 = "2017-03-13";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1).getSOBD(), TZ_NY), day2);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1JP).getSOBD(), TZ_JP), day2);
//
//        // outside calendar range, so no day off for new years, but weekend should still be off
//        day1 = DateTimeUtils.parseInstant("2069-12-31T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2069-12-31T01:00:00.000000000 JP");
//        day2 = "2070-01-01";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1).getSOBD(), TZ_NY).compareTo(day2), 0);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1JP).getSOBD(), TZ_JP), day2);
//
//        day1 = DateTimeUtils.parseInstant("2070-01-05T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2070-01-05T01:00:00.000000000 JP");
//        day2 = "2070-01-06";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1).getSOBD(), TZ_NY), day2);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1JP).getSOBD(), TZ_JP), day2);
//
//        day1 = null;
//        assertNull(USNYSE.nextBusinessSchedule(day1));
//        assertNull(JPOSE.nextBusinessSchedule(day1));
//
//
//        // holiday
//        final BusinessSchedule holiday = USNYSE.businessSchedule("2017-12-25");
//        assertEquals(0, holiday.periods().length);
//        assertEquals(0, holiday.businessNanos());
//        try {
//            // noinspection ResultOfMethodCallIgnored
//            holiday.businessEnd();
//            fail("Expected an exception!");
//        } catch (UnsupportedOperationException e) {
//            // pass
//        }
//        try {
//            // noinspection ResultOfMethodCallIgnored
//            holiday.businessStart();
//            fail("Expected an exception!");
//        } catch (UnsupportedOperationException e) {
//            // pass
//        }
//    }
//
//    public void testNextBusinessScheduleString() {
//        String day1 = "2016-08-31";
//        String day2 = "2016-09-01";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1).getSOBD(), TZ_NY), day2);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1).getSOBD(), TZ_JP), day2);
//
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1, 2).getSOBD(), TZ_NY), "2016-09-02");
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1, 2).getSOBD(), TZ_JP), "2016-09-02");
//
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule("2016-09-02", -2).getSOBD(), TZ_NY),
//                "2016-08-31");
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule("2016-09-02", -2).getSOBD(), TZ_JP),
//                "2016-08-31");
//
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule("2016-08-30", 0).getSOBD(), TZ_NY),
//                "2016-08-30");
//        assertNull(USNYSE.nextBusinessSchedule((String) null, 0));
//
//        // leap day
//        day1 = "2016-02-28";
//        day2 = "2016-02-29";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1).getSOBD(), TZ_NY), day2);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1).getSOBD(), TZ_JP), day2);
//
//        // new year
//        day1 = "2014-01-01";
//        day2 = "2014-01-02";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1).getSOBD(), TZ_NY), day2);
//
//        day1 = "2007-01-03";
//        day2 = "2007-01-04";
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1).getSOBD(), TZ_JP), day2);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = "2017-03-12";
//        day2 = "2017-03-13";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.nextBusinessSchedule(day1).getSOBD(), TZ_NY), day2);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.nextBusinessSchedule(day1).getSOBD(), TZ_JP), day2);
//
//        day1 = null;
//        assertNull(USNYSE.nextBusinessSchedule(day1));
//        assertNull(JPOSE.nextBusinessSchedule(day1));
//    }
//
//    public void testNextNonBusinessDay() {
//        assertEquals("2017-09-30", test.futureNonBusinessDate());
//        assertEquals("2017-10-01", test.futureNonBusinessDate(2));
//        assertEquals("2017-10-08", test.futureNonBusinessDate(4));
//
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        Instant day1JP = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 JP");
//        String day2 = "2016-09-03";
//        assertNull(USNYSE.futureNonBusinessDate((Instant) null));
//        assertEquals(USNYSE.futureNonBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureNonBusinessDate(day1JP), day2);
//
//        assertNull(USNYSE.futureNonBusinessDate((Instant) null, 2));
//        assertEquals(USNYSE.futureNonBusinessDate(day1, 2), "2016-09-04");
//        assertEquals(JPOSE.futureNonBusinessDate(day1JP, 2), "2016-09-04");
//
//        assertEquals(USNYSE.futureNonBusinessDate(DateTimeUtils.parseInstant("2016-09-04T01:00:00.000000000 NY"), -2),
//                "2016-08-28");
//        assertEquals(JPOSE.futureNonBusinessDate(DateTimeUtils.parseInstant("2016-09-04T01:00:00.000000000 JP"), -2),
//                "2016-08-28");
//
//        assertNull(USNYSE.futureNonBusinessDate(DateTimeUtils.parseInstant("2016-08-30T01:00:00.000000000 NY"), 0));
//        assertEquals(USNYSE.futureNonBusinessDate(DateTimeUtils.parseInstant("2016-08-28T01:00:00.000000000 NY"), 0),
//                "2016-08-28");
//
//        // leap day
//        day1 = DateTimeUtils.parseInstant("2016-02-28T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2016-02-28T01:00:00.000000000 JP");
//        day2 = "2016-03-05";
//        assertEquals(USNYSE.futureNonBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureNonBusinessDate(day1JP), day2);
//
//        // new year
//        day1 = DateTimeUtils.parseInstant("2013-12-31T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2013-12-31T01:00:00.000000000 JP");
//        day2 = "2014-01-01";
//        assertEquals(USNYSE.futureNonBusinessDate(day1), day2);
//
//        day2 = "2014-01-04";
//        assertEquals(JPOSE.futureNonBusinessDate(day1JP), day2);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = DateTimeUtils.parseInstant("2017-03-12T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2017-03-12T01:00:00.000000000 JP");
//        day2 = "2017-03-18";
//        assertEquals(USNYSE.futureNonBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureNonBusinessDate(day1JP), day2);
//
//        // outside calendar range, so no day off for new years, but weekend should still be off
//        day1 = DateTimeUtils.parseInstant("2069-12-31T01:00:00.000000000 NY");
//        day1JP = DateTimeUtils.parseInstant("2069-12-31T01:00:00.000000000 JP");
//        day2 = "2070-01-04";
//        assertEquals(USNYSE.futureNonBusinessDate(day1).compareTo(day2), 0);
//        assertEquals(JPOSE.futureNonBusinessDate(day1JP), day2);
//
//        day1 = null;
//        assertNull(USNYSE.futureNonBusinessDate(day1));
//        assertNull(JPOSE.futureNonBusinessDate(day1));
//    }
//
//    public void testNextNonBusinessDayString() {
//        String day1 = "2016-08-31";
//        String day2 = "2016-09-03";
//        assertNull(USNYSE.futureNonBusinessDate((String) null));
//        assertEquals(USNYSE.futureNonBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureNonBusinessDate(day1), day2);
//
//        assertNull(USNYSE.futureNonBusinessDate((String) null, 2));
//        assertEquals(USNYSE.futureNonBusinessDate(day1, 2), "2016-09-04");
//        assertEquals(JPOSE.futureNonBusinessDate(day1, 2), "2016-09-04");
//
//        assertEquals(USNYSE.futureNonBusinessDate("2016-09-04", -2), "2016-08-28");
//        assertEquals(JPOSE.futureNonBusinessDate("2016-09-04", -2), "2016-08-28");
//
//        assertNull(USNYSE.futureNonBusinessDate("2016-08-30", 0));
//        assertEquals(USNYSE.futureNonBusinessDate("2016-08-28", 0), "2016-08-28");
//
//        // leap day
//        day1 = "2016-02-28";
//        day2 = "2016-03-05";
//        assertEquals(USNYSE.futureNonBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureNonBusinessDate(day1), day2);
//
//        // new year
//        day1 = "2013-12-31";
//        day2 = "2014-01-01";
//        assertEquals(USNYSE.futureNonBusinessDate(day1), day2);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = "2017-03-12";
//        day2 = "2017-03-18";
//        assertEquals(USNYSE.futureNonBusinessDate(day1), day2);
//        assertEquals(JPOSE.futureNonBusinessDate(day1), day2);
//
//        day1 = null;
//        assertNull(USNYSE.futureNonBusinessDate(day1));
//        assertNull(JPOSE.futureNonBusinessDate(day1));
//
//        // incorrectly formatted days
//        try {
//            USNYSE.futureNonBusinessDate("2018-09-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testLastBusinessDay() {
//        assertEquals("2017-09-26", test.pastBusinessDate());
//        assertEquals("2017-09-25", test.pastBusinessDate(2));
//        assertEquals("2017-09-07", test.pastBusinessDate(14));
//
//        assertEquals("2017-09-24", test.pastNonBusinessDate());
//        assertEquals("2017-09-23", test.pastNonBusinessDate(2));
//        assertEquals("2017-09-16", test.pastNonBusinessDate(4));
//
//
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-30T01:00:00.000000000 NY");
//        Instant day2 = DateTimeUtils.parseInstant("2016-09-01T01:00:00.000000000 NY");
//        assertNull(USNYSE.pastBusinessDate((Instant) null, 2));
//        assertEquals(USNYSE.pastBusinessDate(day2, 2), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(USNYSE.pastBusinessDate(day1, -2), DateTimeUtils.formatDate(day2, TZ_NY));
//
//        assertEquals(USNYSE.pastBusinessDate(DateTimeUtils.parseInstant("2016-08-30T15:00:00.000000000 NY"), 0),
//                "2016-08-30");
//        assertNull(USNYSE.pastBusinessDate(DateTimeUtils.parseInstant("2016-08-28T15:00:00.000000000 NY"), 0));
//
//        assertNull(USNYSE.pastNonBusinessDate((Instant) null, 0));
//        assertNull(USNYSE.pastNonBusinessDate(DateTimeUtils.parseInstant("2016-08-30T21:00:00.000000000 NY"), 0));
//        assertEquals(
//                USNYSE.pastNonBusinessDate(DateTimeUtils.parseInstant("2016-08-28T21:00:00.000000000 NY"), 0),
//                "2016-08-28");
//
//        // leap day
//        day1 = DateTimeUtils.parseInstant("2016-02-29T21:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2016-03-01T01:00:00.000000000 NY");
//        assertEquals(USNYSE.pastBusinessDate(day2), DateTimeUtils.formatDate(day1, TZ_NY));
//
//        // new year
//        day1 = DateTimeUtils.parseInstant("2013-12-26T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2014-01-02T01:00:00.000000000 NY");
//        assertEquals(USNYSE.pastBusinessDate(day2, 4), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(USNYSE.pastBusinessDate(day1, -4), DateTimeUtils.formatDate(day2, TZ_NY));
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = DateTimeUtils.parseInstant("2017-02-26T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2017-03-13T01:00:00.000000000 NY");
//        assertEquals(USNYSE.pastNonBusinessDate(day2, 5), DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(USNYSE.pastNonBusinessDate(day1, -5), "2017-03-18");
//
//        day1 = DateTimeUtils.parseInstant("2017-03-12T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2017-03-13T01:00:00.000000000 NY");
//        assertEquals(USNYSE.pastNonBusinessDate(day2), DateTimeUtils.formatDate(day1, TZ_NY));
//
//        day1 = DateTimeUtils.parseInstant("2017-07-04T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2017-07-07T01:00:00.000000000 NY");
//        assertEquals(USNYSE.pastNonBusinessDate(day2), DateTimeUtils.formatDate(day1, TZ_NY));
//
//        day1 = null;
//        assertNull(USNYSE.pastBusinessDate(day1));
//        assertNull(USNYSE.pastNonBusinessDate(day1));
//
//
//
//        day1 = DateTimeUtils.parseInstant("2016-08-31T21:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2016-09-01T21:00:00.000000000 JP");
//        assertEquals(JPOSE.pastBusinessDate(day2), DateTimeUtils.formatDate(day1, TZ_JP));
//
//        // leap day
//        day1 = DateTimeUtils.parseInstant("2016-02-29T01:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2016-03-01T01:00:00.000000000 JP");
//        assertEquals(JPOSE.pastBusinessDate(day2), DateTimeUtils.formatDate(day1, TZ_JP));
//
//        // new year
//        day1 = DateTimeUtils.parseInstant("2013-12-31T11:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2014-01-01T11:00:00.000000000 JP");
//        assertEquals(JPOSE.pastBusinessDate(day2), DateTimeUtils.formatDate(day1, TZ_JP));
//
//        // Daylight savings starts in JP (UTC-7:00) at 2 AM 2017-03-12
//        day1 = DateTimeUtils.parseInstant("2017-03-12T01:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2017-03-13T01:00:00.000000000 JP");
//        assertEquals(JPOSE.pastNonBusinessDate(day2), DateTimeUtils.formatDate(day1, TZ_JP));
//
//
//        day1 = null;
//        assertNull(JPOSE.pastBusinessDate(day1));
//        assertNull(JPOSE.pastNonBusinessDate(day1));
//    }
//
//    public void testLastBusinessDayString() {
//        String day1 = "2016-08-31";
//        String day2 = "2016-09-01";
//        assertNull(USNYSE.pastBusinessDate((String) null));
//        assertEquals(USNYSE.pastBusinessDate(day2), day1);
//        assertEquals(JPOSE.pastBusinessDate(day2), day1);
//
//        assertNull(USNYSE.pastBusinessDate((String) null, 2));
//        assertEquals(USNYSE.pastBusinessDate("2016-08-30", 0), "2016-08-30");
//        assertNull(USNYSE.pastBusinessDate("2016-08-28", 0));
//
//        day1 = "2016-08-29";
//        assertEquals(USNYSE.pastBusinessDate(day2, 3), day1);
//        assertEquals(JPOSE.pastBusinessDate(day2, 3), day1);
//        assertEquals(USNYSE.pastBusinessDate(day1, -3), day2);
//        assertEquals(JPOSE.pastBusinessDate(day1, -3), day2);
//
//        // leap day
//        day1 = "2016-02-29";
//        day2 = "2016-03-01";
//        assertEquals(USNYSE.pastBusinessDate(day2), day1);
//        assertEquals(JPOSE.pastBusinessDate(day2), day1);
//
//        // new year
//        day1 = "2013-12-30";
//        day2 = "2014-01-01";
//        assertEquals(USNYSE.pastBusinessDate(day2, 2), day1);
//        assertEquals(JPOSE.pastBusinessDate(day2, 2), day1);
//        assertEquals(USNYSE.pastBusinessDate(day1, -2), "2014-01-02");
//        assertEquals(JPOSE.pastBusinessDate(day1, -2), day2);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = "2017-03-10";
//        day2 = "2017-03-13";
//        assertEquals(USNYSE.pastBusinessDate(day2), day1);
//        assertEquals(JPOSE.pastBusinessDate(day2), day1);
//
//        day1 = null;
//        assertNull(USNYSE.pastBusinessDate(day1));
//        assertNull(JPOSE.pastBusinessDate(day1));
//
//        // incorrectly formatted days
//        try {
//            USNYSE.pastBusinessDate("2018-09-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testLastBusinessSchedule() {
//        assertEquals(test.previousBusinessSchedule(curDay), test.previousBusinessSchedule());
//        assertEquals(test.previousBusinessSchedule(curDay, 2), test.previousBusinessSchedule(2));
//
//
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-30T01:00:00.000000000 NY");
//        Instant day2 = DateTimeUtils.parseInstant("2016-09-01T01:00:00.000000000 NY");
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day2, 2).getSOBD(), TZ_NY),
//                DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day1, -2).getSOBD(), TZ_NY),
//                DateTimeUtils.formatDate(day2, TZ_NY));
//
//        assertEquals(
//                DateTimeUtils.formatDate(USNYSE
//                        .previousBusinessSchedule(DateTimeUtils.parseInstant("2016-08-30T15:00:00.000000000 NY"), 0)
//                        .getSOBD(), TZ_NY),
//                "2016-08-30");
//        assertNull(USNYSE.previousBusinessSchedule((Instant) null, 0));
//
//        // leap day
//        day1 = DateTimeUtils.parseInstant("2016-02-29T21:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2016-03-01T01:00:00.000000000 NY");
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day2).getSOBD(), TZ_NY),
//                DateTimeUtils.formatDate(day1, TZ_NY));
//
//        // new year
//        day1 = DateTimeUtils.parseInstant("2013-12-26T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2014-01-02T01:00:00.000000000 NY");
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day2, 7).getSOBD(), TZ_NY),
//                DateTimeUtils.formatDate(day1, TZ_NY));
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day1, -7).getSOBD(), TZ_NY),
//                DateTimeUtils.formatDate(day2, TZ_NY));
//
//        day1 = null;
//        assertNull(USNYSE.previousBusinessSchedule(day1));
//
//
//        day1 = DateTimeUtils.parseInstant("2016-08-31T21:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2016-09-01T21:00:00.000000000 JP");
//        assertEquals(DateTimeUtils.formatDate(JPOSE.previousBusinessSchedule(day2).getSOBD(), TZ_JP),
//                DateTimeUtils.formatDate(day1, TZ_JP));
//
//        // leap day
//        day1 = DateTimeUtils.parseInstant("2016-02-29T01:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2016-03-01T01:00:00.000000000 JP");
//        assertEquals(DateTimeUtils.formatDate(JPOSE.previousBusinessSchedule(day2).getSOBD(), TZ_JP),
//                DateTimeUtils.formatDate(day1, TZ_JP));
//
//        // new year
//        day1 = DateTimeUtils.parseInstant("2013-12-31T11:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2014-01-01T11:00:00.000000000 JP");
//        assertEquals(DateTimeUtils.formatDate(JPOSE.previousBusinessSchedule(day2).getSOBD(), TZ_JP),
//                DateTimeUtils.formatDate(day1, TZ_JP));
//
//
//        day1 = null;
//        assertNull(JPOSE.previousBusinessSchedule(day1));
//    }
//
//    public void testLastBusinessScheduleString() {
//        String day1 = "2016-08-31";
//        String day2 = "2016-09-01";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day2).getSOBD(), TZ_NY), day1);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.previousBusinessSchedule(day2).getSOBD(), TZ_JP), day1);
//
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule("2016-08-30", 0).getSOBD(), TZ_NY),
//                "2016-08-30");
//        assertNull(USNYSE.previousBusinessSchedule((String) null, 0));
//
//        day1 = "2016-08-29";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day2, 3).getSOBD(), TZ_NY), day1);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.previousBusinessSchedule(day2, 3).getSOBD(), TZ_JP), day1);
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day1, -3).getSOBD(), TZ_NY), day2);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.previousBusinessSchedule(day1, -3).getSOBD(), TZ_JP), day2);
//
//        // leap day
//        day1 = "2016-02-29";
//        day2 = "2016-03-01";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day2).getSOBD(), TZ_NY), day1);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.previousBusinessSchedule(day2).getSOBD(), TZ_JP), day1);
//
//        // new year
//        day1 = "2014-12-29";
//        day2 = "2014-12-31";
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day2, 2).getSOBD(), TZ_NY), day1);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.previousBusinessSchedule(day2, 2).getSOBD(), TZ_JP), day1);
//        assertEquals(DateTimeUtils.formatDate(USNYSE.previousBusinessSchedule(day1, -2).getSOBD(), TZ_NY), day2);
//        assertEquals(DateTimeUtils.formatDate(JPOSE.previousBusinessSchedule(day1, -2).getSOBD(), TZ_JP), day2);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = "2017-03-10";
//        day2 = "2017-03-13";
//        assertEquals(
//                DateTimeUtils.formatDate(
//                        USNYSE.previousBusinessSchedule(USNYSE.pastDate(USNYSE.pastDate(day2))).getSOBD(), TZ_NY),
//                day1);
//        assertEquals(
//                DateTimeUtils.formatDate(
//                        JPOSE.previousBusinessSchedule(JPOSE.pastDate(JPOSE.pastDate(day2))).getSOBD(), TZ_JP),
//                day1);
//
//        day1 = null;
//        assertNull(USNYSE.previousBusinessSchedule(day1));
//        assertNull(JPOSE.previousBusinessSchedule(day1));
//
//        // incorrectly formatted days
//        try {
//            USNYSE.previousBusinessSchedule("2018-09-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testLastNonBusinessDayString() {
//        String day1 = "2016-08-28";
//        String day2 = "2016-09-01";
//        assertNull(USNYSE.pastNonBusinessDate((String) null));
//        assertEquals(USNYSE.pastNonBusinessDate(day2), day1);
//        assertEquals(JPOSE.pastNonBusinessDate(day2), day1);
//
//        assertNull(USNYSE.pastNonBusinessDate((String) null, 2));
//        assertNull(USNYSE.pastNonBusinessDate("2016-08-30", 0));
//        assertEquals(USNYSE.pastNonBusinessDate("2016-08-28", 0), "2016-08-28");
//
//        // leap day
//        day1 = "2016-02-27";
//        day2 = "2016-03-01";
//        assertEquals(USNYSE.pastNonBusinessDate(day2, 2), day1);
//        assertEquals(JPOSE.pastNonBusinessDate(day2, 2), day1);
//        assertEquals(USNYSE.pastNonBusinessDate(day1, -2), "2016-03-05");
//        assertEquals(JPOSE.pastNonBusinessDate(day1, -2), "2016-03-05");
//
//        // new year
//        day1 = "2013-12-29";
//        day2 = "2014-01-01";
//        assertEquals(USNYSE.pastNonBusinessDate(day2), day1);
//        assertEquals(JPOSE.pastNonBusinessDate(day2), day1);
//
//        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
//        day1 = "2017-03-05";
//        day2 = "2017-03-13";
//        assertEquals(USNYSE.pastNonBusinessDate(day2, 3), day1);
//        assertEquals(JPOSE.pastNonBusinessDate(day2, 3), day1);
//        assertEquals(USNYSE.pastNonBusinessDate(day1, -3), "2017-03-18");
//        assertEquals(JPOSE.pastNonBusinessDate(day1, -3), "2017-03-18");
//
//        day1 = null;
//        assertNull(USNYSE.pastNonBusinessDate(day1));
//        assertNull(JPOSE.pastNonBusinessDate(day1));
//
//        // incorrectly formatted days
//        try {
//            USNYSE.pastNonBusinessDate("2018-09-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testDiff() {
//        // standard business day
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        Instant day2 = DateTimeUtils.parseInstant("2016-09-01T01:00:00.000000000 NY");
//        assertEquals(USNYSE.diffDay(day1, day2), 1.0);
//        assertEquals(USNYSE.diffNanos(day1, day2), DateTimeUtils.DAY);
//        assertEquals(JPOSE.diffYear365(day1, day2), (double) DateTimeUtils.DAY / (double) DateTimeUtils.YEAR_365);
//        assertEquals(JPOSE.diffYearAvg(day1, day2), (double) DateTimeUtils.DAY / (double) DateTimeUtils.YEAR_AVG);
//    }
//
//    public void testBusinessTimeDiff() {
//        // standard business day
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        Instant day2 = DateTimeUtils.parseInstant("2016-09-01T01:00:00.000000000 NY");
//        assertEquals(USNYSE.diffBusinessDays(day1, day2), 1.0);
//        assertEquals(JPOSE.diffBusinessDays(day1, day2), 1.0);
//
//        // 2.5 standard business days
//        day1 = DateTimeUtils.parseInstant("2017-01-23T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2017-01-25T12:45:00.000000000 NY");
//        assertEquals(USNYSE.diffBusinessDays(day1, day2), 2.5);
//
//        day1 = DateTimeUtils.parseInstant("2017-01-23T01:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2017-01-25T12:45:00.000000000 JP");
//        assertEquals(JPOSE.diffBusinessDays(day1, day2), 2.55);
//
//        // middle of a business period
//        day1 = DateTimeUtils.parseInstant("2017-01-23T10:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2017-01-23T13:00:00.000000000 JP");
//        assertEquals(JPOSE.diffBusinessNanos(day1, day2), 2 * DateTimeUtils.HOUR);
//
//        // after a business period
//        day1 = DateTimeUtils.parseInstant("2017-01-23T10:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2017-01-23T16:15:00.000000000 JP");
//        assertEquals(JPOSE.diffBusinessNanos(day1, day2), 4 * DateTimeUtils.HOUR);
//
//        // middle of the second business period
//        day1 = DateTimeUtils.parseInstant("2017-01-23T08:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2017-01-23T14:00:00.000000000 JP");
//        assertEquals(JPOSE.diffBusinessNanos(day1, day2), 4 * DateTimeUtils.HOUR);
//
//        // weekend non business
//        day1 = DateTimeUtils.parseInstant("2017-01-21T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2017-01-23T01:00:00.000000000 NY");
//        assertEquals(USNYSE.diffBusinessDays(day1, day2), 0.0);
//
//        // one business year
//        day1 = DateTimeUtils.parseInstant("2016-01-01T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2016-12-31T23:59:00.000000000 NY");
//        double yearDiff = USNYSE.diffBusinessYears(day1, day2);
//        assertTrue(yearDiff < 1.004);
//        assertTrue(yearDiff > 0.996);
//        yearDiff = JPOSE.diffBusinessYears(day1, day2);
//        assertTrue(yearDiff < 1.004);
//        assertTrue(yearDiff > 0.996);
//
//        // half year
//        day1 = DateTimeUtils.parseInstant("2017-01-01T01:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2017-07-02T01:00:00.000000000 NY");
//        yearDiff = USNYSE.diffBusinessYears(day1, day2);
//        assertTrue(yearDiff < 0.503);
//        assertTrue(yearDiff > 0.497);
//        yearDiff = JPOSE.diffBusinessYears(day1, day2);
//        assertTrue(yearDiff < 0.503);
//        assertTrue(yearDiff > 0.497);
//
//
//        day1 = null;
//        assertEquals(USNYSE.diffBusinessYears(day1, day2), QueryConstants.NULL_DOUBLE);
//        assertEquals(USNYSE.diffBusinessDays(day1, day2), QueryConstants.NULL_DOUBLE);
//        assertEquals(USNYSE.diffBusinessNanos(day1, day2), QueryConstants.NULL_LONG);
//
//        day1 = day2;
//        assertEquals(USNYSE.diffBusinessYears(day1, day2), 0.0);
//        assertEquals(USNYSE.diffBusinessDays(day1, day2), 0.0);
//        assertEquals(USNYSE.diffBusinessNanos(day1, day2), 0);
//    }
//
//    public void testNonBusinessTimeDiff() {
//        // USNYSE
//        // standard business day
//        Instant day1 = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
//        Instant day2 = DateTimeUtils.parseInstant("2016-09-01T01:00:00.000000000 NY");
//        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), 63000000000000L); // 17.5 hours
//        assertEquals(USNYSE.diffNonBusinessNanos(day2, day1), -63000000000000L); // 17.5 hours
//
//        // middle of a business period
//        day1 = DateTimeUtils.parseInstant("2017-01-23T10:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2017-01-23T12:30:00.000000000 NY");
//        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), 0);
//
//        // after a business period
//        day1 = DateTimeUtils.parseInstant("2017-01-23T10:00:00.000000000 NY");
//        day2 = DateTimeUtils.parseInstant("2017-01-23T16:15:00.000000000 NY");
//        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), 15 * DateTimeUtils.MINUTE);
//
//        // JPOSE
//        // standard business day
//        day1 = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2016-09-01T01:00:00.000000000 JP");
//        assertEquals(JPOSE.diffNonBusinessNanos(day1, day2), 19 * DateTimeUtils.HOUR); // 17.5 hours
//
//        // middle of a business period
//        day1 = DateTimeUtils.parseInstant("2017-01-23T10:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2017-01-23T11:30:00.000000000 JP");
//        assertEquals(JPOSE.diffNonBusinessNanos(day1, day2), 0);
//
//        // after a business period
//        day1 = DateTimeUtils.parseInstant("2017-01-23T10:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2017-01-23T16:00:00.000000000 JP");
//        assertEquals(JPOSE.diffNonBusinessNanos(day1, day2), 2 * DateTimeUtils.HOUR);
//        assertEquals(JPOSE.diffNonBusinessDays(day1, day2),
//                ((double) (2 * DateTimeUtils.HOUR)) / (double) JPOSE.standardBusinessNanos());
//
//
//
//        day1 = null;
//        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), QueryConstants.NULL_LONG);
//
//        day1 = day2;
//        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), 0);
//
//        day1 = null;
//        assertEquals(USNYSE.diffNonBusinessDays(day1, day2), QueryConstants.NULL_DOUBLE);
//    }
//
//    public void testBusinessDateRange() {
//        // day light savings
//        Instant startDate = DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY");
//        Instant endDate = DateTimeUtils.parseInstant("2017-03-14T01:00:00.000000000 NY");
//
//        String[] goodResults = new String[] {
//                "2017-03-13",
//                "2017-03-14"
//        };
//
//        String[] results = USNYSE.businessDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        boolean answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//        assertEquals(new String[0], USNYSE.businessDates(endDate, startDate));
//
//        startDate = DateTimeUtils.parseInstant("2017-11-23T01:00:00.000000000 JP");
//        endDate = DateTimeUtils.parseInstant("2017-11-25T01:00:00.000000000 JP");
//
//        goodResults = new String[] {
//                "2017-11-24"
//        };
//
//        results = JPOSE.businessDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//        startDate = null;
//        assertEquals(JPOSE.businessDates(startDate, endDate).length, 0);
//
//        // non business
//        startDate = DateTimeUtils.parseInstant("2017-03-11T01:00:00.000000000 NY");
//        endDate = DateTimeUtils.parseInstant("2017-03-14T01:00:00.000000000 NY");
//
//        goodResults = new String[] {
//                "2017-03-11",
//                "2017-03-12"
//        };
//
//        results = USNYSE.nonBusinessDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//
//        startDate = DateTimeUtils.parseInstant("2017-11-23T01:00:00.000000000 JP");
//        endDate = DateTimeUtils.parseInstant("2017-11-25T01:00:00.000000000 JP");
//
//        goodResults = new String[] {
//                "2017-11-23",
//                "2017-11-25"
//        };
//
//        assertEquals(new String[0], USNYSE.nonBusinessDates(endDate, startDate));
//        results = JPOSE.nonBusinessDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//        startDate = null;
//        assertEquals(JPOSE.nonBusinessDates(startDate, endDate).length, 0);
//
//        startDate = null;
//        assertEquals(USNYSE.nonBusinessDates(startDate, endDate).length, 0);
//    }
//
//    public void testBusinessDateStringRange() {
//        // USNYSE
//        String startDate = "2014-02-16";
//        String endDate = "2014-03-05";
//        String[] goodResults = new String[] {
//                "2014-02-18", "2014-02-19", "2014-02-20", "2014-02-21",
//                "2014-02-24", "2014-02-25", "2014-02-26", "2014-02-27", "2014-02-28",
//                "2014-03-03", "2014-03-04", "2014-03-05",
//        };
//
//        assertEquals(new String[0], USNYSE.businessDates(endDate, startDate));
//        String[] results = USNYSE.businessDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        boolean answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//        startDate = null;
//        assertEquals(USNYSE.businessDates(startDate, endDate).length, 0);
//
//        startDate = endDate;
//        assertEquals(USNYSE.businessDates(startDate, endDate).length, 1);
//
//        // JPOSE
//        startDate = "2018-01-01";
//        endDate = "2018-01-05";
//        goodResults = new String[] {
//                "2018-01-04",
//                "2018-01-05"
//        };
//
//        results = JPOSE.businessDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//
//        // non business
//        startDate = "2020-01-01";
//        endDate = "2020-01-20";
//        goodResults = new String[] {
//                "2020-01-01", "2020-01-04", "2020-01-05", "2020-01-11", "2020-01-12",
//                "2020-01-18", "2020-01-19", "2020-01-20"
//        };
//
//        assertEquals(new String[0], USNYSE.nonBusinessDates(endDate, startDate));
//        results = USNYSE.nonBusinessDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//
//        // JPOSE
//        startDate = "2018-01-01";
//        endDate = "2018-01-05";
//        goodResults = new String[] {
//                "2018-01-01",
//                "2018-01-02",
//                "2018-01-03"
//        };
//
//        results = JPOSE.nonBusinessDates(startDate, endDate);
//        Arrays.sort(goodResults);
//        Arrays.sort(results);
//        answer = Arrays.equals(goodResults, results);
//        assertTrue(answer);
//
//
//        // null tests
//        startDate = null;
//        assertEquals(USNYSE.nonBusinessDates(startDate, endDate).length, 0);
//
//        startDate = endDate = "2018-01-06";
//        assertEquals(USNYSE.nonBusinessDates(startDate, endDate).length, 1);
//
//        // incorrectly formatted days
//        try {
//            USNYSE.nonBusinessDates("2018-09-31", "2018-010-31");
//            fail();
//        } catch (IllegalArgumentException e) {
//            // ok
//        }
//    }
//
//    public void testDayOfWeek() {
//        assertEquals(DayOfWeek.WEDNESDAY, test.dayOfWeek());
//
//        String dateString = "2017-02-06";
//        assertEquals(USNYSE.dayOfWeek(dateString), DayOfWeek.MONDAY);
//        assertEquals(JPOSE.dayOfWeek(dateString), DayOfWeek.MONDAY);
//
//        Instant dateTime = DateTimeUtils.parseInstant("2017-09-01T00:00:00.000000000 NY");
//        assertEquals(USNYSE.dayOfWeek(dateTime), DayOfWeek.FRIDAY);
//        assertEquals(JPOSE.dayOfWeek(dateTime), DayOfWeek.FRIDAY);
//
//        dateString = null;
//        dateTime = null;
//        assertNull(USNYSE.dayOfWeek(dateString));
//        assertNull(USNYSE.dayOfWeek(dateTime));
//
//        // incorrectly formatted days
//        try {
//            USNYSE.dayOfWeek("2018-09-31");
//            fail();
//        } catch (DateTimeException e) {
//            // ok
//        }
//    }
//
//    public void testLastBusinessDayOfWeek() {
//        assertFalse(test.isLastBusinessDayOfWeek());
//
//        String dateString = "2017-02-10";
//        Instant dateTime = DateTimeUtils.parseInstant("2017-02-07T00:00:00.000000000 NY");
//        assertTrue(USNYSE.isLastBusinessDayOfWeek(dateString));
//        assertFalse(USNYSE.isLastBusinessDayOfWeek(dateTime));
//        assertTrue(JPOSE.isLastBusinessDayOfWeek(dateString));
//        assertFalse(JPOSE.isLastBusinessDayOfWeek(dateTime));
//
//        dateString = null;
//        assertFalse(USNYSE.isLastBusinessDayOfWeek(dateString));
//    }
//
//    public void testLastBusinessDayOfMonth() {
//        assertFalse(test.isLastBusinessDayOfMonth());
//
//        String dateString = "2017-02-28";
//        Instant dateTime = DateTimeUtils.parseInstant("2017-02-07T00:00:00.000000000 NY");
//        assertTrue(USNYSE.isLastBusinessDayOfMonth(dateString));
//        assertFalse(USNYSE.isLastBusinessDayOfMonth(dateTime));
//        assertTrue(JPOSE.isLastBusinessDayOfMonth(dateString));
//        assertFalse(JPOSE.isLastBusinessDayOfMonth(dateTime));
//
//        dateString = null;
//        assertFalse(USNYSE.isLastBusinessDayOfMonth(dateString));
//        assertFalse(JPOSE.isLastBusinessDayOfMonth(dateString));
//    }
//
//    public void testFractionOfBusinessDay() {
//        assertEquals(1.0, test.fractionStandardBusinessDay());
//
//
//        // half day, USNYSE market open from 0930 to 1300
//        String dateString = "2018-11-23";
//
//        // full day
//        Instant dateTime = DateTimeUtils.parseInstant("2017-02-07T00:00:00.000000000 NY");
//
//        assertEquals(USNYSE.fractionStandardBusinessDay(dateString), 3.5 / 6.5);
//        assertEquals(1.0, USNYSE.fractionStandardBusinessDay(dateTime));
//
//        // half day, JPOSE market open from 0930 to 1300
//        dateString = "2006-01-04";
//
//        assertEquals(JPOSE.fractionStandardBusinessDay(dateString), 0.5);
//        assertEquals(1.0, JPOSE.fractionStandardBusinessDay(dateTime));
//
//
//        dateString = null;
//        dateTime = null;
//        assertEquals(JPOSE.fractionStandardBusinessDay(dateString), 0.0);
//        assertEquals(JPOSE.fractionStandardBusinessDay(dateTime), 0.0);
//    }
//
//    public void testFractionOfBusinessDayLeft() {
//        // half day, market open from 0930 to 1300
//        Instant day1 = DateTimeUtils.parseInstant("2018-11-23T10:00:00.000000000 NY");
//
//        // full day
//        Instant day2 = DateTimeUtils.parseInstant("2017-02-07T00:00:00.000000000 NY");
//
//        // holiday
//        Instant day3 = DateTimeUtils.parseInstant("2017-07-04T00:00:00.000000000 NY");
//
//        assertEquals(USNYSE.fractionBusinessDayRemaining(day1), 3.0 / 3.5);
//        assertEquals(USNYSE.fractionBusinessDayComplete(day1), 0.5 / 3.5, 0.0000001);
//        assertEquals(USNYSE.fractionBusinessDayRemaining(day2), 1.0);
//        assertEquals(USNYSE.fractionBusinessDayComplete(day2), 0.0);
//        assertEquals(USNYSE.fractionBusinessDayRemaining(day3), 0.0);
//
//        // half day, market open from 0900 to 1130
//        day1 = DateTimeUtils.parseInstant("2006-01-04T10:00:00.000000000 JP");
//        day2 = DateTimeUtils.parseInstant("2017-02-07T00:00:00.000000000 JP");
//        assertEquals(JPOSE.fractionBusinessDayRemaining(day1), 1.5 / 2.5);
//        assertEquals(JPOSE.fractionBusinessDayComplete(day1), 1.0 / 2.5);
//        assertEquals(JPOSE.fractionBusinessDayRemaining(day2), 1.0);
//        assertEquals(JPOSE.fractionBusinessDayComplete(day2), 0.0);
//
//
//        assertEquals(JPOSE.fractionOfBusinessDayRemaining(null), QueryConstants.NULL_DOUBLE);
//        assertEquals(JPOSE.fractionOfBusinessDayComplete(null), QueryConstants.NULL_DOUBLE);
//    }
//
//    public void testCurrentBusinessSchedule() {
//        assertEquals(test.nextBusinessSchedule("2017-09-26"), test.businessSchedule());
//    }
//
//    public void testMidnightClose() {
//        assertEquals(DateTimeUtils.DAY, UTC.standardBusinessNanos());
//        assertEquals("2019-04-16", UTC.futureDate("2019-04-15"));
//        assertEquals("2019-04-16", UTC.futureBusinessDate("2019-04-15"));
//        assertEquals("2019-04-18", UTC.futureBusinessDate("2019-04-15", 3));
//        assertEquals("2019-08-19",
//                UTC.futureBusinessDate(DateTimeUtils.parseInstant("2019-08-18T00:00:00.000000000 UTC")));
//
//        assertEquals("2019-05-16", DateTimeUtils.formatDate(UTC.businessSchedule("2019-05-16").businessStart(), TZ_UTC));
//        assertEquals("2019-05-17", DateTimeUtils.formatDate(UTC.businessSchedule("2019-05-16").businessEnd(), TZ_UTC));
//    }
}
