/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.impl.DataAccessHelpers;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendars;
import io.deephaven.time.calendar.StaticCalendarMethods;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.time.LocalDate;

import static io.deephaven.engine.util.TableTools.emptyTable;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link StaticCalendarMethods} from the {@link Table} API.
 */
@Category(OutOfBandTest.class)
public class TestCalendarMethodsFromTable {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private final BusinessCalendar calendar = Calendars.calendar();
    private final Instant time1 = DateTimeUtils.parseInstant("2002-01-01T01:00:00.000000000 NY");
    private final Instant time2 = DateTimeUtils.parseInstant("2002-01-21T01:00:00.000000000 NY");
    private final String date1 = "2017-08-01";
    private final String date2 = "2017-08-05";

    // test to make sure these methods work inside the query strings
    // previous clash with DateTimeUtils
    @Test
    public void testCalendarMethodsTable() {
        if (!ExecutionContext.getContext().getQueryLibrary().getStaticImports().contains(StaticCalendarMethods.class)) {
            ExecutionContext.getContext().getQueryLibrary().importStatic(StaticCalendarMethods.class);
        }
        QueryScope.addParam("time1", time1);
        QueryScope.addParam("time2", time2);
        QueryScope.addParam("date1", date1);
        QueryScope.addParam("date2", date2);

        assertEquals(calendar.name(), getVal(emptyTable(1).update("Name = calendarName()"), "Name"));

        assertEquals(calendar.calendarDate(), getVal(emptyTable(1).update("currentDay = calendarDate()"), "currentDay"));

        assertEquals(calendar.pastDate(1),
                getVal(emptyTable(1).update("previousDay = pastDate(1)"), "previousDay"));
        assertEquals(calendar.pastDate(4),
                getVal(emptyTable(1).update("previousDay = pastDate(4)"), "previousDay"));
        assertEquals(calendar.minusDays(time1, 1),
                getVal(emptyTable(1).update("previousDay = minusDays(time1,1)"), "previousDay"));
        assertEquals(calendar.minusDays(time1, 4),
                getVal(emptyTable(1).update("previousDay = minusDays(time1, 4)"), "previousDay"));
        assertEquals(calendar.minusDays(date1, 1),
                getVal(emptyTable(1).update("previousDay = minusDays(date1, 1)"), "previousDay"));
        assertEquals(calendar.minusDays(date1, 14),
                getVal(emptyTable(1).update("previousDay = minusDays(date1, 14)"), "previousDay"));


        assertEquals(calendar.futureDate(1), getVal(emptyTable(1).update("nextDay = futureDate(1)"), "nextDay"));
        assertEquals(calendar.futureDate(4), getVal(emptyTable(1).update("nextDay = futureDate(4)"), "nextDay"));
        assertEquals(calendar.plusDays(time1, 1), getVal(emptyTable(1).update("nextDay = plusDays(time1, 1)"), "nextDay"));
        assertEquals(calendar.plusDays(time1, 4),
                getVal(emptyTable(1).update("nextDay = plusDays(time1, 4)"), "nextDay"));
        assertEquals(calendar.plusDays(date1, 1), getVal(emptyTable(1).update("nextDay = plusDays(date1, 1)"), "nextDay"));
        assertEquals(calendar.plusDays(date1, 14),
                getVal(emptyTable(1).update("nextDay = plusDays(date1, 14)"), "nextDay"));

        assertEquals(calendar.calendarDates(time1, time2),
                (LocalDate[]) getVal(emptyTable(1).update("daysInRange = calendarDates(time1, time2)"), "daysInRange"));
        assertEquals(calendar.calendarDates(date1, date2),
                (LocalDate[]) getVal(emptyTable(1).update("daysInRange = calendarDates(date1, date2)"), "daysInRange"));


        assertEquals(calendar.numberCalendarDates(time1, time2),
                getVal(emptyTable(1).update("numberOfDays = numberCalendarDates(time1, time2)"), "numberOfDays"));
        assertEquals(calendar.numberCalendarDates(time1, time2, true, true),
                getVal(emptyTable(1).update("numberOfDays = numberCalendarDates(time1, time2, true, true)"), "numberOfDays"));
        assertEquals(calendar.numberCalendarDates(date1, date2),
                getVal(emptyTable(1).update("numberOfDays = numberCalendarDates(date1, date2)"), "numberOfDays"));
        assertEquals(calendar.numberCalendarDates(date1, date2, true, true),
                getVal(emptyTable(1).update("numberOfDays = numberCalendarDates(date1, date2, true, true)"), "numberOfDays"));


        assertEquals(calendar.dayOfWeek(),
                getVal(emptyTable(1).update("dayOfWeek = calendarDayOfWeek()"), "dayOfWeek"));
        assertEquals(calendar.dayOfWeek(time2),
                getVal(emptyTable(1).update("dayOfWeek = calendarDayOfWeek(time2)"), "dayOfWeek"));
        assertEquals(calendar.dayOfWeek(date2),
                getVal(emptyTable(1).update("dayOfWeek = calendarDayOfWeek(date2)"), "dayOfWeek"));

        assertEquals(calendar.timeZone(), getVal(emptyTable(1).update("timeZone = calendarTimeZone()"), "timeZone"));

        assertEquals(calendar.isBusinessDay(),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay()"), "isBusinessDay"));
        assertEquals(calendar.isBusinessDay(time2),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay(time2)"), "isBusinessDay"));
        assertEquals(calendar.isBusinessDay(date2),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay(date2)"), "isBusinessDay"));
    }

    @Test
    public void testBusinessCalendarMethodsTable() {

        if (!ExecutionContext.getContext().getQueryLibrary().getStaticImports().contains(StaticCalendarMethods.class)) {
            ExecutionContext.getContext().getQueryLibrary().importStatic(StaticCalendarMethods.class);
        }
        final LocalDate localDate = LocalDate.now();
        QueryScope.addParam("localDate", localDate);
        QueryScope.addParam("time1", time1);
        QueryScope.addParam("time2", time2);
        QueryScope.addParam("date1", date1);
        QueryScope.addParam("date2", date2);


        assertEquals(calendar.isBusinessDay(),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay()"), "isBusinessDay"));
        assertEquals(calendar.isBusinessDay(time2),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay(time2)"), "isBusinessDay"));
        assertEquals(calendar.isBusinessDay(date2),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay(date2)"), "isBusinessDay"));
        assertEquals(calendar.isBusinessDay(localDate),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay(localDate)"), "isBusinessDay"));


        assertEquals(calendar.isBusinessTime(time1),
                getVal(emptyTable(1).update("isBusinessTime = isBusinessTime(time1)"), "isBusinessTime"));
        assertEquals(calendar.isBusinessTime(time2),
                getVal(emptyTable(1).update("isBusinessTime = isBusinessTime(time2)"), "isBusinessTime"));


        assertEquals(calendar.pastBusinessDate(1),
                getVal(emptyTable(1).update("previousBusinessDay = pastBusinessDate(1)"), "previousBusinessDay"));
        assertEquals(calendar.pastBusinessDate(12),
                getVal(emptyTable(1).update("previousBusinessDay = pastBusinessDate(12)"), "previousBusinessDay"));
        assertEquals(calendar.plusBusinessDays(time1, 1), getVal(
                emptyTable(1).update("previousBusinessDay = plusBusinessDays(time1, 1)"), "previousBusinessDay"));
        assertEquals(calendar.plusBusinessDays(time1, 6), getVal(
                emptyTable(1).update("previousBusinessDay = plusBusinessDays(time1, 6)"), "previousBusinessDay"));
        assertEquals(calendar.plusBusinessDays(date1, 1), getVal(
                emptyTable(1).update("previousBusinessDay = plusBusinessDays(date1, 1)"), "previousBusinessDay"));
        assertEquals(calendar.plusBusinessDays(date1, 16), getVal(
                emptyTable(1).update("previousBusinessDay = plusBusinessDays(date1, 16)"), "previousBusinessDay"));


        assertEquals(calendar.standardBusinessSchedule(),
                getVal(emptyTable(1).update("standardBusinessSchedule = standardBusinessSchedule()"),
                        "standardBusinessSchedule"));
        assertEquals(calendar.businessSchedule(),
                getVal(emptyTable(1).update("businessSchedule = businessSchedule()"),
                        "businessSchedule"));


        assertEquals(calendar.pastNonBusinessDate(1), getVal(
                emptyTable(1).update("previousNonBusinessDay = pastNonBusinessDate(1)"), "previousNonBusinessDay"));
        assertEquals(calendar.pastNonBusinessDate(12), getVal(
                emptyTable(1).update("previousNonBusinessDay = pastNonBusinessDate(12)"), "previousNonBusinessDay"));
        assertEquals(calendar.minusNonBusinessDays(time1, 1),
                getVal(emptyTable(1).update("previousNonBusinessDay = minusNonBusinessDays(time1, 1)"),
                        "previousNonBusinessDay"));
        assertEquals(calendar.minusNonBusinessDays(time1, 6),
                getVal(emptyTable(1).update("previousNonBusinessDay = minusNonBusinessDays(time1, 6)"),
                        "previousNonBusinessDay"));
        assertEquals(calendar.minusNonBusinessDays(date1, 1),
                getVal(emptyTable(1).update("previousNonBusinessDay = minusNonBusinessDays(date1, 1)"),
                        "previousNonBusinessDay"));
        assertEquals(calendar.minusNonBusinessDays(date1, 16),
                getVal(emptyTable(1).update("previousNonBusinessDay = minusNonBusinessDays(date1, 16)"),
                        "previousNonBusinessDay"));


        assertEquals(calendar.futureBusinessDate(1),
                getVal(emptyTable(1).update("nextBusinessDay = futureBusinessDate(1)"), "nextBusinessDay"));
        assertEquals(calendar.futureBusinessDate(12),
                getVal(emptyTable(1).update("nextBusinessDay = futureBusinessDate(12)"), "nextBusinessDay"));
        assertEquals(calendar.plusBusinessDays(time1, 1),
                getVal(emptyTable(1).update("nextBusinessDay = plusBusinessDays(time1, 1)"), "nextBusinessDay"));
        assertEquals(calendar.plusBusinessDays(time1, 6),
                getVal(emptyTable(1).update("nextBusinessDay = plusBusinessDays(time1, 6)"), "nextBusinessDay"));
        assertEquals(calendar.plusBusinessDays(date1, 1),
                getVal(emptyTable(1).update("nextBusinessDay = plusBusinessDays(date1, 1)"), "nextBusinessDay"));
        assertEquals(calendar.plusBusinessDays(date1, 16),
                getVal(emptyTable(1).update("nextBusinessDay = plusBusinessDays(date1, 16)"), "nextBusinessDay"));


        assertEquals(calendar.futureNonBusinessDate(1),
                getVal(emptyTable(1).update("nextNonBusinessDay = futureNonBusinessDate(1)"), "nextNonBusinessDay"));
        assertEquals(calendar.futureNonBusinessDate(12),
                getVal(emptyTable(1).update("nextNonBusinessDay = futureNonBusinessDate(12)"), "nextNonBusinessDay"));
        assertEquals(calendar.plusNonBusinessDays(time1, 1),
                getVal(emptyTable(1).update("nextNonBusinessDay = plusNonBusinessDays(time1, 1)"), "nextNonBusinessDay"));
        assertEquals(calendar.plusNonBusinessDays(time1, 6), getVal(
                emptyTable(1).update("nextNonBusinessDay = plusNonBusinessDays(time1, 6)"), "nextNonBusinessDay"));
        assertEquals(calendar.plusNonBusinessDays(date1, 1),
                getVal(emptyTable(1).update("nextNonBusinessDay = plusNonBusinessDays(date1, 1)"),
                        "nextNonBusinessDay"));
        assertEquals(calendar.plusNonBusinessDays(date1, 16), getVal(
                emptyTable(1).update("nextNonBusinessDay = plusNonBusinessDays(date1, 16)"), "nextNonBusinessDay"));


        assertEquals(calendar.businessDates(time1, time2),
                (LocalDate[]) getVal(emptyTable(1).update("businessDaysInRange = businessDates(time1, time2)"),
                        "businessDaysInRange"));
        assertEquals(calendar.businessDates(date1, date2),
                (LocalDate[]) getVal(emptyTable(1).update("businessDaysInRange = businessDates(date1, date2)"),
                        "businessDaysInRange"));


        assertEquals(calendar.nonBusinessDates(time1, time2),
                (LocalDate[]) getVal(emptyTable(1).update("nonBusinessDaysInRange = nonBusinessDates(time1, time2)"),
                        "nonBusinessDaysInRange"));
        assertEquals(calendar.nonBusinessDates(date1, date2),
                (LocalDate[]) getVal(emptyTable(1).update("nonBusinessDaysInRange = nonBusinessDates(date1, date2)"),
                        "nonBusinessDaysInRange"));


        assertEquals(calendar.standardBusinessNanos(),
                getVal(emptyTable(1).update("standardBusinessDayLengthNanos = standardBusinessNanos()"),
                        "standardBusinessDayLengthNanos"));


        assertEquals(calendar.diffBusinessNanos(time1, time2), getVal(
                emptyTable(1).update("diffBusinessNanos = diffBusinessNanos(time1, time2)"), "diffBusinessNanos"));
        assertEquals(calendar.diffNonBusinessNanos(time1, time2),
                getVal(emptyTable(1).update("diffNonBusinessNanos = diffNonBusinessNanos(time1, time2)"),
                        "diffNonBusinessNanos"));
        assertEquals(calendar.diffBusinessDays(time1, time2),
                getVal(emptyTable(1).update("diffBusinessDay = diffBusinessDays(time1, time2)"), "diffBusinessDay"));
        assertEquals(calendar.diffBusinessYears(time1, time2),
                getVal(emptyTable(1).update("diffBusinessYear = diffBusinessYears(time1, time2)"), "diffBusinessYear"));



        assertEquals(calendar.numberBusinessDates(time1, time2),
                getVal(emptyTable(1).update("numberOfBusinessDays = numberBusinessDates(time1, time2)"),
                        "numberOfBusinessDays"));
        assertEquals(calendar.numberBusinessDates(time1, time2, true, true),
                getVal(emptyTable(1).update("numberOfBusinessDays = numberBusinessDates(time1, time2, true, true)"),
                        "numberOfBusinessDays"));
        assertEquals(calendar.numberBusinessDates(date1, date2),
                getVal(emptyTable(1).update("numberOfBusinessDays = numberBusinessDates(date1, date2)"),
                        "numberOfBusinessDays"));
        assertEquals(calendar.numberBusinessDates(date1, date2, true, true),
                getVal(emptyTable(1).update("numberOfBusinessDays = numberBusinessDates(date1, date2, true, true)"),
                        "numberOfBusinessDays"));


        assertEquals(calendar.numberNonBusinessDates(time1, time2),
                getVal(emptyTable(1).update("numberOfNonBusinessDays = numberNonBusinessDates(time1, time2)"),
                        "numberOfNonBusinessDays"));
        assertEquals(calendar.numberNonBusinessDates(time1, time2, true, true),
                getVal(emptyTable(1).update("numberOfNonBusinessDays = numberNonBusinessDates(time1, time2, true, true)"),
                        "numberOfNonBusinessDays"));
        assertEquals(calendar.numberNonBusinessDates(date1, date2),
                getVal(emptyTable(1).update("numberOfNonBusinessDays = numberNonBusinessDates(date1, date2)"),
                        "numberOfNonBusinessDays"));
        assertEquals(calendar.numberNonBusinessDates(date1, date2, true, true),
                getVal(emptyTable(1).update("numberOfNonBusinessDays = numberNonBusinessDates(date1, date2, true, true)"),
                        "numberOfNonBusinessDays"));


        assertEquals(calendar.fractionStandardBusinessDay(),
                getVal(emptyTable(1).update("fractionOfStandardBusinessDay = fractionStandardBusinessDay()"),
                        "fractionOfStandardBusinessDay"));
        assertEquals(calendar.fractionStandardBusinessDay(time1),
                getVal(emptyTable(1).update("fractionOfStandardBusinessDay = fractionStandardBusinessDay(time1)"),
                        "fractionOfStandardBusinessDay"));
        assertEquals(calendar.fractionStandardBusinessDay(date1),
                getVal(emptyTable(1).update("fractionOfStandardBusinessDay = fractionStandardBusinessDay(date1)"),
                        "fractionOfStandardBusinessDay"));


        assertEquals(calendar.fractionBusinessDayRemaining(time1),
                getVal(emptyTable(1).update("fractionOfBusinessDayRemaining = fractionBusinessDayRemaining(time1)"),
                        "fractionOfBusinessDayRemaining"));
        assertEquals(calendar.fractionBusinessDayComplete(time1),
                getVal(emptyTable(1).update("fractionOfBusinessDayComplete = fractionBusinessDayComplete(time1)"),
                        "fractionOfBusinessDayComplete"));


        assertEquals(calendar.isLastBusinessDayOfMonth(),
                getVal(emptyTable(1).update("isLastBusinessDayOfMonth = isLastBusinessDayOfMonth()"),
                        "isLastBusinessDayOfMonth"));
        assertEquals(calendar.isLastBusinessDayOfMonth(time1),
                getVal(emptyTable(1).update("isLastBusinessDayOfMonth = isLastBusinessDayOfMonth(time1)"),
                        "isLastBusinessDayOfMonth"));
        assertEquals(calendar.isLastBusinessDayOfMonth(date1),
                getVal(emptyTable(1).update("isLastBusinessDayOfMonth = isLastBusinessDayOfMonth(date1)"),
                        "isLastBusinessDayOfMonth"));


        assertEquals(calendar.isLastBusinessDayOfWeek(),
                getVal(emptyTable(1).update("isLastBusinessDayOfWeek = isLastBusinessDayOfWeek()"),
                        "isLastBusinessDayOfWeek"));
        assertEquals(calendar.isLastBusinessDayOfWeek(time1),
                getVal(emptyTable(1).update("isLastBusinessDayOfWeek = isLastBusinessDayOfWeek(time1)"),
                        "isLastBusinessDayOfWeek"));
        assertEquals(calendar.isLastBusinessDayOfWeek(date1),
                getVal(emptyTable(1).update("isLastBusinessDayOfWeek = isLastBusinessDayOfWeek(date1)"),
                        "isLastBusinessDayOfWeek"));


        assertEquals(calendar.businessSchedule(time1), getVal(
                emptyTable(1).update("getBusinessSchedule = businessSchedule(time1)"), "getBusinessSchedule"));
        assertEquals(calendar.businessSchedule(date1), getVal(
                emptyTable(1).update("getBusinessSchedule = businessSchedule(date1)"), "getBusinessSchedule"));
        assertEquals(calendar.businessSchedule(localDate), getVal(
                emptyTable(1).update("getBusinessSchedule = businessSchedule(localDate)"), "getBusinessSchedule"));
    }

    private Object getVal(final Table t, final String column) {
        return DataAccessHelpers.getColumn(t, column).get(0);
    }
}
