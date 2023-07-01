/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Tests for {@link StaticCalendarMethods}
 */
public class StaticCalendarMethodsTest extends BaseArrayTestCase {

    private final BusinessCalendar calendar = Calendars.calendar();
    private final Instant time1 = DateTimeUtils.parseInstant("2002-01-01T01:00:00.000000000 NY");
    private final Instant time2 = DateTimeUtils.parseInstant("2002-01-21T01:00:00.000000000 NY");
    private final String date1 = "2017-08-01";
    private final String date2 = "2017-08-05";

    public void testCalendarMethods() {
        assertEquals(calendar.name(), StaticCalendarMethods.name());

        assertEquals(calendar.currentDate(), StaticCalendarMethods.currentDay());

        assertEquals(calendar.pastDate(), StaticCalendarMethods.previousDay());
        assertEquals(calendar.pastDate(4), StaticCalendarMethods.previousDay(4));
        assertEquals(calendar.pastDate(time1), StaticCalendarMethods.previousDay(time1));
        assertEquals(calendar.pastDate(time1, 4), StaticCalendarMethods.previousDay(time1, 4));
        assertEquals(calendar.pastDate(date1), StaticCalendarMethods.previousDay(date1));
        assertEquals(calendar.pastDate(date1, 14), StaticCalendarMethods.previousDay(date1, 14));

        assertEquals(calendar.futureDate(), StaticCalendarMethods.nextDay());
        assertEquals(calendar.futureDate(4), StaticCalendarMethods.nextDay(4));
        assertEquals(calendar.futureDate(time2), StaticCalendarMethods.nextDay(time2));
        assertEquals(calendar.futureDate(time2, 4), StaticCalendarMethods.nextDay(time2, 4));
        assertEquals(calendar.futureDate(date2), StaticCalendarMethods.nextDay(date2));
        assertEquals(calendar.futureDate(date2, 14), StaticCalendarMethods.nextDay(date2, 14));

        assertEquals(calendar.calendarDates(time1, time2), StaticCalendarMethods.daysInRange(time1, time2));
        assertEquals(calendar.calendarDates(date1, date2), StaticCalendarMethods.daysInRange(date1, date2));

        assertEquals(calendar.numberCalendarDates(time1, time2), StaticCalendarMethods.numberOfDays(time1, time2));
        assertEquals(calendar.numberCalendarDates(time1, time2, true), StaticCalendarMethods.numberOfDays(time1, time2, true));
        assertEquals(calendar.numberCalendarDates(date1, date2), StaticCalendarMethods.numberOfDays(date1, date2));
        assertEquals(calendar.numberCalendarDates(date1, date2, true), StaticCalendarMethods.numberOfDays(date1, date2, true));


        assertEquals(calendar.dayOfWeek(), StaticCalendarMethods.dayOfWeek());
        assertEquals(calendar.dayOfWeek(time2), StaticCalendarMethods.dayOfWeek(time2));
        assertEquals(calendar.dayOfWeek(date2), StaticCalendarMethods.dayOfWeek(date2));

        assertEquals(calendar.timeZone(), StaticCalendarMethods.calendarTimeZone());
    }

    public void testBusinessCalendarMethods() {
        assertEquals(calendar.isBusinessDay(), StaticCalendarMethods.isBusinessDay());
        assertEquals(calendar.isBusinessDay(time2), StaticCalendarMethods.isBusinessDay(time2));
        assertEquals(calendar.isBusinessDay(date2), StaticCalendarMethods.isBusinessDay(date2));


        assertEquals(calendar.isBusinessDay(), StaticCalendarMethods.isBusinessDay());
        assertEquals(calendar.isBusinessDay(time1), StaticCalendarMethods.isBusinessDay(time1));
        assertEquals(calendar.isBusinessDay(date1), StaticCalendarMethods.isBusinessDay(date1));
        assertEquals(calendar.isBusinessDay(LocalDate.now()), StaticCalendarMethods.isBusinessDay(LocalDate.now()));


        assertEquals(calendar.isBusinessTime(time1), StaticCalendarMethods.isBusinessTime(time1));
        assertEquals(calendar.isBusinessTime(time2), StaticCalendarMethods.isBusinessTime(time2));


        assertEquals(calendar.pastBusinessDate(), StaticCalendarMethods.previousBusinessDay());
        assertEquals(calendar.pastBusinessDate(12), StaticCalendarMethods.previousBusinessDay(12));
        assertEquals(calendar.pastBusinessDate(time1), StaticCalendarMethods.previousBusinessDay(time1));
        assertEquals(calendar.pastBusinessDate(time1, 6), StaticCalendarMethods.previousBusinessDay(time1, 6));
        assertEquals(calendar.pastBusinessDate(date1), StaticCalendarMethods.previousBusinessDay(date1));
        assertEquals(calendar.pastBusinessDate(date1, 16), StaticCalendarMethods.previousBusinessDay(date1, 16));


        assertEquals(calendar.previousBusinessSchedule(), StaticCalendarMethods.previousBusinessSchedule());
        assertEquals(calendar.previousBusinessSchedule(12), StaticCalendarMethods.previousBusinessSchedule(12));
        assertEquals(calendar.previousBusinessSchedule(time1), StaticCalendarMethods.previousBusinessSchedule(time1));
        assertEquals(calendar.previousBusinessSchedule(time1, 6),
                StaticCalendarMethods.previousBusinessSchedule(time1, 6));
        assertEquals(calendar.previousBusinessSchedule(date1), StaticCalendarMethods.previousBusinessSchedule(date1));
        assertEquals(calendar.previousBusinessSchedule(date1, 16),
                StaticCalendarMethods.previousBusinessSchedule(date1, 16));


        assertEquals(calendar.pastNonBusinessDate(), StaticCalendarMethods.previousNonBusinessDay());
        assertEquals(calendar.pastNonBusinessDate(12), StaticCalendarMethods.previousNonBusinessDay(12));
        assertEquals(calendar.pastNonBusinessDate(time1), StaticCalendarMethods.previousNonBusinessDay(time1));
        assertEquals(calendar.pastNonBusinessDate(time1, 6), StaticCalendarMethods.previousNonBusinessDay(time1, 6));
        assertEquals(calendar.pastNonBusinessDate(date1), StaticCalendarMethods.previousNonBusinessDay(date1));
        assertEquals(calendar.pastNonBusinessDate(date1, 16),
                StaticCalendarMethods.previousNonBusinessDay(date1, 16));


        assertEquals(calendar.futureBusinessDate(), StaticCalendarMethods.nextBusinessDay());
        assertEquals(calendar.futureBusinessDate(12), StaticCalendarMethods.nextBusinessDay(12));
        assertEquals(calendar.futureBusinessDate(time1), StaticCalendarMethods.nextBusinessDay(time1));
        assertEquals(calendar.futureBusinessDate(time1, 6), StaticCalendarMethods.nextBusinessDay(time1, 6));
        assertEquals(calendar.futureBusinessDate(date1), StaticCalendarMethods.nextBusinessDay(date1));
        assertEquals(calendar.futureBusinessDate(date1, 16), StaticCalendarMethods.nextBusinessDay(date1, 16));


        assertEquals(calendar.nextBusinessSchedule(), StaticCalendarMethods.nextBusinessSchedule());
        assertEquals(calendar.nextBusinessSchedule(12), StaticCalendarMethods.nextBusinessSchedule(12));
        assertEquals(calendar.nextBusinessSchedule(time1), StaticCalendarMethods.nextBusinessSchedule(time1));
        assertEquals(calendar.nextBusinessSchedule(time1, 6), StaticCalendarMethods.nextBusinessSchedule(time1, 6));
        assertEquals(calendar.nextBusinessSchedule(date1), StaticCalendarMethods.nextBusinessSchedule(date1));
        assertEquals(calendar.nextBusinessSchedule(date1, 16), StaticCalendarMethods.nextBusinessSchedule(date1, 16));


        assertEquals(calendar.futureNonBusinessDate(), StaticCalendarMethods.nextNonBusinessDay());
        assertEquals(calendar.futureNonBusinessDate(12), StaticCalendarMethods.nextNonBusinessDay(12));
        assertEquals(calendar.futureNonBusinessDate(time1), StaticCalendarMethods.nextNonBusinessDay(time1));
        assertEquals(calendar.futureNonBusinessDate(time1, 6), StaticCalendarMethods.nextNonBusinessDay(time1, 6));
        assertEquals(calendar.futureNonBusinessDate(date1), StaticCalendarMethods.nextNonBusinessDay(date1));
        assertEquals(calendar.futureNonBusinessDate(date1, 16), StaticCalendarMethods.nextNonBusinessDay(date1, 16));


        assertEquals(calendar.businessDates(time1, time2),
                StaticCalendarMethods.businessDaysInRange(time1, time2));
        assertEquals(calendar.businessDates(date1, date2),
                StaticCalendarMethods.businessDaysInRange(date1, date2));


        assertEquals(calendar.nonBusinessDates(time1, time2),
                StaticCalendarMethods.nonBusinessDaysInRange(time1, time2));
        assertEquals(calendar.nonBusinessDates(date1, date2),
                StaticCalendarMethods.nonBusinessDaysInRange(date1, date2));


        assertEquals(calendar.standardBusinessDayLengthNanos(), StaticCalendarMethods.standardBusinessDayLengthNanos());


        assertEquals(calendar.diffBusinessNanos(time1, time2), StaticCalendarMethods.diffBusinessNanos(time1, time2));
        assertEquals(calendar.diffNonBusinessNanos(time1, time2),
                StaticCalendarMethods.diffNonBusinessNanos(time1, time2));
        assertEquals(calendar.diffBusinessDays(time1, time2), StaticCalendarMethods.diffBusinessDay(time1, time2));
        assertEquals(calendar.diffNonBusinessDays(time1, time2), StaticCalendarMethods.diffNonBusinessDay(time1, time2));
        assertEquals(calendar.diffBusinessYears(time1, time2), StaticCalendarMethods.diffBusinessYear(time1, time2));


        assertEquals(calendar.numberOfBusinessDays(time1, time2),
                StaticCalendarMethods.numberOfBusinessDays(time1, time2));
        assertEquals(calendar.numberBusinessDates(time1, time2, true),
                StaticCalendarMethods.numberOfBusinessDays(time1, time2, true));
        assertEquals(calendar.numberOfBusinessDays(date1, date2),
                StaticCalendarMethods.numberOfBusinessDays(date1, date2));
        assertEquals(calendar.numberBusinessDates(date1, date2, true),
                StaticCalendarMethods.numberOfBusinessDays(date1, date2, true));


        assertEquals(calendar.numberOfNonBusinessDays(time1, time2),
                StaticCalendarMethods.numberOfNonBusinessDays(time1, time2));
        assertEquals(calendar.numberNonBusinessDates(time1, time2, true),
                StaticCalendarMethods.numberOfNonBusinessDays(time1, time2, true));
        assertEquals(calendar.numberOfNonBusinessDays(date1, date2),
                StaticCalendarMethods.numberOfNonBusinessDays(date1, date2));
        assertEquals(calendar.numberNonBusinessDates(date1, date2, true),
                StaticCalendarMethods.numberOfNonBusinessDays(date1, date2, true));


        assertEquals(calendar.fractionOfStandardBusinessDay(), StaticCalendarMethods.fractionOfStandardBusinessDay());
        assertEquals(calendar.fractionOfStandardBusinessDay(time1),
                StaticCalendarMethods.fractionOfStandardBusinessDay(time1));
        assertEquals(calendar.fractionOfStandardBusinessDay(date1),
                StaticCalendarMethods.fractionOfStandardBusinessDay(date1));


        assertEquals(calendar.fractionOfBusinessDayRemaining(time1),
                StaticCalendarMethods.fractionOfBusinessDayRemaining(time1));
        assertEquals(calendar.fractionOfBusinessDayComplete(time1),
                StaticCalendarMethods.fractionOfBusinessDayComplete(time1));


        assertEquals(calendar.isLastBusinessDayOfMonth(), StaticCalendarMethods.isLastBusinessDayOfMonth());
        assertEquals(calendar.isLastBusinessDayOfMonth(time1), StaticCalendarMethods.isLastBusinessDayOfMonth(time1));
        assertEquals(calendar.isLastBusinessDayOfMonth(date1), StaticCalendarMethods.isLastBusinessDayOfMonth(date1));


        assertEquals(calendar.isLastBusinessDayOfWeek(), StaticCalendarMethods.isLastBusinessDayOfWeek());
        assertEquals(calendar.isLastBusinessDayOfWeek(time1), StaticCalendarMethods.isLastBusinessDayOfWeek(time1));
        assertEquals(calendar.isLastBusinessDayOfWeek(date1), StaticCalendarMethods.isLastBusinessDayOfWeek(date1));

        assertEquals(calendar.businessSchedule(time1), StaticCalendarMethods.getBusinessSchedule(time1));
        assertEquals(calendar.businessSchedule(date1), StaticCalendarMethods.getBusinessSchedule(date1));
        assertEquals(calendar.businessSchedule(LocalDate.now()),
                StaticCalendarMethods.getBusinessSchedule(LocalDate.now()));
    }
}
