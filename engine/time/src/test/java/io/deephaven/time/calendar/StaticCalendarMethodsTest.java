package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;

import java.time.LocalDate;

/**
 * Tests for {@link StaticCalendarMethods}
 */
public class StaticCalendarMethodsTest extends BaseArrayTestCase {

    private final BusinessCalendar calendar = Calendars.calendar();
    private final DateTime time1 = DateTimeUtils.convertDateTime("2002-01-01T01:00:00.000000000 NY");
    private final DateTime time2 = DateTimeUtils.convertDateTime("2002-01-21T01:00:00.000000000 NY");
    private final String date1 = "2017-08-01";
    private final String date2 = "2017-08-05";

    public void testCalendarMethods() {
        assertEquals(calendar.name(), StaticCalendarMethods.name());

        assertEquals(calendar.currentDay(), StaticCalendarMethods.currentDay());

        assertEquals(calendar.previousDay(), StaticCalendarMethods.previousDay());
        assertEquals(calendar.previousDay(4), StaticCalendarMethods.previousDay(4));
        assertEquals(calendar.previousDay(time1), StaticCalendarMethods.previousDay(time1));
        assertEquals(calendar.previousDay(time1, 4), StaticCalendarMethods.previousDay(time1, 4));
        assertEquals(calendar.previousDay(date1), StaticCalendarMethods.previousDay(date1));
        assertEquals(calendar.previousDay(date1, 14), StaticCalendarMethods.previousDay(date1, 14));

        assertEquals(calendar.nextDay(), StaticCalendarMethods.nextDay());
        assertEquals(calendar.nextDay(4), StaticCalendarMethods.nextDay(4));
        assertEquals(calendar.nextDay(time2), StaticCalendarMethods.nextDay(time2));
        assertEquals(calendar.nextDay(time2, 4), StaticCalendarMethods.nextDay(time2, 4));
        assertEquals(calendar.nextDay(date2), StaticCalendarMethods.nextDay(date2));
        assertEquals(calendar.nextDay(date2, 14), StaticCalendarMethods.nextDay(date2, 14));

        assertEquals(calendar.daysInRange(time1, time2), StaticCalendarMethods.daysInRange(time1, time2));
        assertEquals(calendar.daysInRange(date1, date2), StaticCalendarMethods.daysInRange(date1, date2));

        assertEquals(calendar.numberOfDays(time1, time2), StaticCalendarMethods.numberOfDays(time1, time2));
        assertEquals(calendar.numberOfDays(time1, time2, true), StaticCalendarMethods.numberOfDays(time1, time2, true));
        assertEquals(calendar.numberOfDays(date1, date2), StaticCalendarMethods.numberOfDays(date1, date2));
        assertEquals(calendar.numberOfDays(date1, date2, true), StaticCalendarMethods.numberOfDays(date1, date2, true));


        assertEquals(calendar.dayOfWeek(), StaticCalendarMethods.dayOfWeek());
        assertEquals(calendar.dayOfWeek(time2), StaticCalendarMethods.dayOfWeek(time2));
        assertEquals(calendar.dayOfWeek(date2), StaticCalendarMethods.dayOfWeek(date2));

        assertEquals(calendar.timeZone(), StaticCalendarMethods.timeZone());
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


        assertEquals(calendar.previousBusinessDay(), StaticCalendarMethods.previousBusinessDay());
        assertEquals(calendar.previousBusinessDay(12), StaticCalendarMethods.previousBusinessDay(12));
        assertEquals(calendar.previousBusinessDay(time1), StaticCalendarMethods.previousBusinessDay(time1));
        assertEquals(calendar.previousBusinessDay(time1, 6), StaticCalendarMethods.previousBusinessDay(time1, 6));
        assertEquals(calendar.previousBusinessDay(date1), StaticCalendarMethods.previousBusinessDay(date1));
        assertEquals(calendar.previousBusinessDay(date1, 16), StaticCalendarMethods.previousBusinessDay(date1, 16));


        assertEquals(calendar.previousBusinessSchedule(), StaticCalendarMethods.previousBusinessSchedule());
        assertEquals(calendar.previousBusinessSchedule(12), StaticCalendarMethods.previousBusinessSchedule(12));
        assertEquals(calendar.previousBusinessSchedule(time1), StaticCalendarMethods.previousBusinessSchedule(time1));
        assertEquals(calendar.previousBusinessSchedule(time1, 6),
                StaticCalendarMethods.previousBusinessSchedule(time1, 6));
        assertEquals(calendar.previousBusinessSchedule(date1), StaticCalendarMethods.previousBusinessSchedule(date1));
        assertEquals(calendar.previousBusinessSchedule(date1, 16),
                StaticCalendarMethods.previousBusinessSchedule(date1, 16));


        assertEquals(calendar.previousNonBusinessDay(), StaticCalendarMethods.previousNonBusinessDay());
        assertEquals(calendar.previousNonBusinessDay(12), StaticCalendarMethods.previousNonBusinessDay(12));
        assertEquals(calendar.previousNonBusinessDay(time1), StaticCalendarMethods.previousNonBusinessDay(time1));
        assertEquals(calendar.previousNonBusinessDay(time1, 6), StaticCalendarMethods.previousNonBusinessDay(time1, 6));
        assertEquals(calendar.previousNonBusinessDay(date1), StaticCalendarMethods.previousNonBusinessDay(date1));
        assertEquals(calendar.previousNonBusinessDay(date1, 16),
                StaticCalendarMethods.previousNonBusinessDay(date1, 16));


        assertEquals(calendar.nextBusinessDay(), StaticCalendarMethods.nextBusinessDay());
        assertEquals(calendar.nextBusinessDay(12), StaticCalendarMethods.nextBusinessDay(12));
        assertEquals(calendar.nextBusinessDay(time1), StaticCalendarMethods.nextBusinessDay(time1));
        assertEquals(calendar.nextBusinessDay(time1, 6), StaticCalendarMethods.nextBusinessDay(time1, 6));
        assertEquals(calendar.nextBusinessDay(date1), StaticCalendarMethods.nextBusinessDay(date1));
        assertEquals(calendar.nextBusinessDay(date1, 16), StaticCalendarMethods.nextBusinessDay(date1, 16));


        assertEquals(calendar.nextBusinessSchedule(), StaticCalendarMethods.nextBusinessSchedule());
        assertEquals(calendar.nextBusinessSchedule(12), StaticCalendarMethods.nextBusinessSchedule(12));
        assertEquals(calendar.nextBusinessSchedule(time1), StaticCalendarMethods.nextBusinessSchedule(time1));
        assertEquals(calendar.nextBusinessSchedule(time1, 6), StaticCalendarMethods.nextBusinessSchedule(time1, 6));
        assertEquals(calendar.nextBusinessSchedule(date1), StaticCalendarMethods.nextBusinessSchedule(date1));
        assertEquals(calendar.nextBusinessSchedule(date1, 16), StaticCalendarMethods.nextBusinessSchedule(date1, 16));


        assertEquals(calendar.nextNonBusinessDay(), StaticCalendarMethods.nextNonBusinessDay());
        assertEquals(calendar.nextNonBusinessDay(12), StaticCalendarMethods.nextNonBusinessDay(12));
        assertEquals(calendar.nextNonBusinessDay(time1), StaticCalendarMethods.nextNonBusinessDay(time1));
        assertEquals(calendar.nextNonBusinessDay(time1, 6), StaticCalendarMethods.nextNonBusinessDay(time1, 6));
        assertEquals(calendar.nextNonBusinessDay(date1), StaticCalendarMethods.nextNonBusinessDay(date1));
        assertEquals(calendar.nextNonBusinessDay(date1, 16), StaticCalendarMethods.nextNonBusinessDay(date1, 16));


        assertEquals(calendar.businessDaysInRange(time1, time2),
                StaticCalendarMethods.businessDaysInRange(time1, time2));
        assertEquals(calendar.businessDaysInRange(date1, date2),
                StaticCalendarMethods.businessDaysInRange(date1, date2));


        assertEquals(calendar.nonBusinessDaysInRange(time1, time2),
                StaticCalendarMethods.nonBusinessDaysInRange(time1, time2));
        assertEquals(calendar.nonBusinessDaysInRange(date1, date2),
                StaticCalendarMethods.nonBusinessDaysInRange(date1, date2));


        assertEquals(calendar.standardBusinessDayLengthNanos(), StaticCalendarMethods.standardBusinessDayLengthNanos());


        assertEquals(calendar.diffBusinessNanos(time1, time2), StaticCalendarMethods.diffBusinessNanos(time1, time2));
        assertEquals(calendar.diffNonBusinessNanos(time1, time2),
                StaticCalendarMethods.diffNonBusinessNanos(time1, time2));
        assertEquals(calendar.diffBusinessDay(time1, time2), StaticCalendarMethods.diffBusinessDay(time1, time2));
        assertEquals(calendar.diffNonBusinessDay(time1, time2), StaticCalendarMethods.diffNonBusinessDay(time1, time2));
        assertEquals(calendar.diffBusinessYear(time1, time2), StaticCalendarMethods.diffBusinessYear(time1, time2));


        assertEquals(calendar.numberOfBusinessDays(time1, time2),
                StaticCalendarMethods.numberOfBusinessDays(time1, time2));
        assertEquals(calendar.numberOfBusinessDays(time1, time2, true),
                StaticCalendarMethods.numberOfBusinessDays(time1, time2, true));
        assertEquals(calendar.numberOfBusinessDays(date1, date2),
                StaticCalendarMethods.numberOfBusinessDays(date1, date2));
        assertEquals(calendar.numberOfBusinessDays(date1, date2, true),
                StaticCalendarMethods.numberOfBusinessDays(date1, date2, true));


        assertEquals(calendar.numberOfNonBusinessDays(time1, time2),
                StaticCalendarMethods.numberOfNonBusinessDays(time1, time2));
        assertEquals(calendar.numberOfNonBusinessDays(time1, time2, true),
                StaticCalendarMethods.numberOfNonBusinessDays(time1, time2, true));
        assertEquals(calendar.numberOfNonBusinessDays(date1, date2),
                StaticCalendarMethods.numberOfNonBusinessDays(date1, date2));
        assertEquals(calendar.numberOfNonBusinessDays(date1, date2, true),
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

        assertEquals(calendar.getBusinessSchedule(time1), StaticCalendarMethods.getBusinessSchedule(time1));
        assertEquals(calendar.getBusinessSchedule(date1), StaticCalendarMethods.getBusinessSchedule(date1));
        assertEquals(calendar.getBusinessSchedule(LocalDate.now()),
                StaticCalendarMethods.getBusinessSchedule(LocalDate.now()));
    }
}
