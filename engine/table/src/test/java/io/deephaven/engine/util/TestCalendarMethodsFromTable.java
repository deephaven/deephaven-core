package io.deephaven.engine.util;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendars;
import io.deephaven.time.calendar.StaticCalendarMethods;
import io.deephaven.test.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

import java.time.LocalDate;

import static io.deephaven.engine.util.TableTools.emptyTable;

/**
 * Tests for {@link StaticCalendarMethods} from the {@link Table} API.
 */
@Category(OutOfBandTest.class)
public class TestCalendarMethodsFromTable extends BaseArrayTestCase {

    private final EngineCleanup base = new EngineCleanup();

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        base.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        base.tearDown();
    }

    private final BusinessCalendar calendar = Calendars.calendar();
    private final DateTime time1 = DateTimeUtils.convertDateTime("2002-01-01T01:00:00.000000000 NY");
    private final DateTime time2 = DateTimeUtils.convertDateTime("2002-01-21T01:00:00.000000000 NY");
    private final String date1 = "2017-08-01";
    private final String date2 = "2017-08-05";

    // test to make sure these methods work inside the query strings
    // previous clash with DateTimeUtils
    public void testCalendarMethodsTable() {
        if (!QueryLibrary.getStaticImports().contains(StaticCalendarMethods.class)) {
            QueryLibrary.importStatic(StaticCalendarMethods.class);
        }
        QueryScope.addParam("time1", time1);
        QueryScope.addParam("time2", time2);
        QueryScope.addParam("date1", date1);
        QueryScope.addParam("date2", date2);

        assertEquals(calendar.name(), getVal(emptyTable(1).update("Name = name()"), "Name"));

        assertEquals(calendar.currentDay(), getVal(emptyTable(1).update("currentDay = currentDay()"), "currentDay"));

        assertEquals(calendar.previousDay(),
                getVal(emptyTable(1).update("previousDay = previousDay()"), "previousDay"));
        assertEquals(calendar.previousDay(4),
                getVal(emptyTable(1).update("previousDay = previousDay(4)"), "previousDay"));
        assertEquals(calendar.previousDay(time1),
                getVal(emptyTable(1).update("previousDay = previousDay(time1)"), "previousDay"));
        assertEquals(calendar.previousDay(time1, 4),
                getVal(emptyTable(1).update("previousDay = previousDay(time1, 4)"), "previousDay"));
        assertEquals(calendar.previousDay(date1),
                getVal(emptyTable(1).update("previousDay = previousDay(date1)"), "previousDay"));
        assertEquals(calendar.previousDay(date1, 14),
                getVal(emptyTable(1).update("previousDay = previousDay(date1, 14)"), "previousDay"));


        assertEquals(calendar.nextDay(), getVal(emptyTable(1).update("nextDay = nextDay()"), "nextDay"));
        assertEquals(calendar.nextDay(4), getVal(emptyTable(1).update("nextDay = nextDay(4)"), "nextDay"));
        assertEquals(calendar.nextDay(time1), getVal(emptyTable(1).update("nextDay = nextDay(time1)"), "nextDay"));
        assertEquals(calendar.nextDay(time1, 4),
                getVal(emptyTable(1).update("nextDay = nextDay(time1, 4)"), "nextDay"));
        assertEquals(calendar.nextDay(date1), getVal(emptyTable(1).update("nextDay = nextDay(date1)"), "nextDay"));
        assertEquals(calendar.nextDay(date1, 14),
                getVal(emptyTable(1).update("nextDay = nextDay(date1, 14)"), "nextDay"));

        assertEquals(calendar.daysInRange(time1, time2),
                (String[]) getVal(emptyTable(1).update("daysInRange = daysInRange(time1, time2)"), "daysInRange"));
        assertEquals(calendar.daysInRange(date1, date2),
                (String[]) getVal(emptyTable(1).update("daysInRange = daysInRange(date1, date2)"), "daysInRange"));


        assertEquals(calendar.numberOfDays(time1, time2),
                getVal(emptyTable(1).update("numberOfDays = numberOfDays(time1, time2)"), "numberOfDays"));
        assertEquals(calendar.numberOfDays(time1, time2, true),
                getVal(emptyTable(1).update("numberOfDays = numberOfDays(time1, time2, true)"), "numberOfDays"));
        assertEquals(calendar.numberOfDays(date1, date2),
                getVal(emptyTable(1).update("numberOfDays = numberOfDays(date1, date2)"), "numberOfDays"));
        assertEquals(calendar.numberOfDays(date1, date2, true),
                getVal(emptyTable(1).update("numberOfDays = numberOfDays(date1, date2, true)"), "numberOfDays"));


        assertEquals(calendar.dayOfWeek(), getVal(emptyTable(1).update("dayOfWeek = dayOfWeek()"), "dayOfWeek"));
        assertEquals(calendar.dayOfWeek(time2),
                getVal(emptyTable(1).update("dayOfWeek = dayOfWeek(time2)"), "dayOfWeek"));
        assertEquals(calendar.dayOfWeek(date2),
                getVal(emptyTable(1).update("dayOfWeek = dayOfWeek(date2)"), "dayOfWeek"));


        assertEquals(calendar.timeZone(), getVal(emptyTable(1).update("timeZone = timeZone()"), "timeZone"));


        assertEquals(calendar.isBusinessDay(),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay()"), "isBusinessDay"));
        assertEquals(calendar.isBusinessDay(time2),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay(time2)"), "isBusinessDay"));
        assertEquals(calendar.isBusinessDay(date2),
                getVal(emptyTable(1).update("isBusinessDay = isBusinessDay(date2)"), "isBusinessDay"));
    }

    public void testBusinessCalendarMethodsTable() {

        if (!QueryLibrary.getStaticImports().contains(StaticCalendarMethods.class)) {
            QueryLibrary.importStatic(StaticCalendarMethods.class);
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


        assertEquals(calendar.previousBusinessDay(),
                getVal(emptyTable(1).update("previousBusinessDay = previousBusinessDay()"), "previousBusinessDay"));
        assertEquals(calendar.previousBusinessDay(12),
                getVal(emptyTable(1).update("previousBusinessDay = previousBusinessDay(12)"), "previousBusinessDay"));
        assertEquals(calendar.previousBusinessDay(time1), getVal(
                emptyTable(1).update("previousBusinessDay = previousBusinessDay(time1)"), "previousBusinessDay"));
        assertEquals(calendar.previousBusinessDay(time1, 6), getVal(
                emptyTable(1).update("previousBusinessDay = previousBusinessDay(time1, 6)"), "previousBusinessDay"));
        assertEquals(calendar.previousBusinessDay(date1), getVal(
                emptyTable(1).update("previousBusinessDay = previousBusinessDay(date1)"), "previousBusinessDay"));
        assertEquals(calendar.previousBusinessDay(date1, 16), getVal(
                emptyTable(1).update("previousBusinessDay = previousBusinessDay(date1, 16)"), "previousBusinessDay"));


        assertEquals(calendar.previousBusinessSchedule(),
                getVal(emptyTable(1).update("previousBusinessSchedule = previousBusinessSchedule()"),
                        "previousBusinessSchedule"));
        assertEquals(calendar.previousBusinessSchedule(12),
                getVal(emptyTable(1).update("previousBusinessSchedule = previousBusinessSchedule(12)"),
                        "previousBusinessSchedule"));
        assertEquals(calendar.previousBusinessSchedule(time1),
                getVal(emptyTable(1).update("previousBusinessSchedule = previousBusinessSchedule(time1)"),
                        "previousBusinessSchedule"));
        assertEquals(calendar.previousBusinessSchedule(time1, 6),
                getVal(emptyTable(1).update("previousBusinessSchedule = previousBusinessSchedule(time1, 6)"),
                        "previousBusinessSchedule"));
        assertEquals(calendar.previousBusinessSchedule(date1),
                getVal(emptyTable(1).update("previousBusinessSchedule = previousBusinessSchedule(date1)"),
                        "previousBusinessSchedule"));
        assertEquals(calendar.previousBusinessSchedule(date1, 16),
                getVal(emptyTable(1).update("previousBusinessSchedule = previousBusinessSchedule(date1, 16)"),
                        "previousBusinessSchedule"));


        assertEquals(calendar.previousNonBusinessDay(), getVal(
                emptyTable(1).update("previousNonBusinessDay = previousNonBusinessDay()"), "previousNonBusinessDay"));
        assertEquals(calendar.previousNonBusinessDay(12), getVal(
                emptyTable(1).update("previousNonBusinessDay = previousNonBusinessDay(12)"), "previousNonBusinessDay"));
        assertEquals(calendar.previousNonBusinessDay(time1),
                getVal(emptyTable(1).update("previousNonBusinessDay = previousNonBusinessDay(time1)"),
                        "previousNonBusinessDay"));
        assertEquals(calendar.previousNonBusinessDay(time1, 6),
                getVal(emptyTable(1).update("previousNonBusinessDay = previousNonBusinessDay(time1, 6)"),
                        "previousNonBusinessDay"));
        assertEquals(calendar.previousNonBusinessDay(date1),
                getVal(emptyTable(1).update("previousNonBusinessDay = previousNonBusinessDay(date1)"),
                        "previousNonBusinessDay"));
        assertEquals(calendar.previousNonBusinessDay(date1, 16),
                getVal(emptyTable(1).update("previousNonBusinessDay = previousNonBusinessDay(date1, 16)"),
                        "previousNonBusinessDay"));


        assertEquals(calendar.nextBusinessDay(),
                getVal(emptyTable(1).update("nextBusinessDay = nextBusinessDay()"), "nextBusinessDay"));
        assertEquals(calendar.nextBusinessDay(12),
                getVal(emptyTable(1).update("nextBusinessDay = nextBusinessDay(12)"), "nextBusinessDay"));
        assertEquals(calendar.nextBusinessDay(time1),
                getVal(emptyTable(1).update("nextBusinessDay = nextBusinessDay(time1)"), "nextBusinessDay"));
        assertEquals(calendar.nextBusinessDay(time1, 6),
                getVal(emptyTable(1).update("nextBusinessDay = nextBusinessDay(time1, 6)"), "nextBusinessDay"));
        assertEquals(calendar.nextBusinessDay(date1),
                getVal(emptyTable(1).update("nextBusinessDay = nextBusinessDay(date1)"), "nextBusinessDay"));
        assertEquals(calendar.nextBusinessDay(date1, 16),
                getVal(emptyTable(1).update("nextBusinessDay = nextBusinessDay(date1, 16)"), "nextBusinessDay"));


        assertEquals(calendar.nextBusinessSchedule(),
                getVal(emptyTable(1).update("nextBusinessSchedule = nextBusinessSchedule()"), "nextBusinessSchedule"));
        assertEquals(calendar.nextBusinessSchedule(12), getVal(
                emptyTable(1).update("nextBusinessSchedule = nextBusinessSchedule(12)"), "nextBusinessSchedule"));
        assertEquals(calendar.nextBusinessSchedule(time1), getVal(
                emptyTable(1).update("nextBusinessSchedule = nextBusinessSchedule(time1)"), "nextBusinessSchedule"));
        assertEquals(calendar.nextBusinessSchedule(time1, 6), getVal(
                emptyTable(1).update("nextBusinessSchedule = nextBusinessSchedule(time1, 6)"), "nextBusinessSchedule"));
        assertEquals(calendar.nextBusinessSchedule(date1), getVal(
                emptyTable(1).update("nextBusinessSchedule = nextBusinessSchedule(date1)"), "nextBusinessSchedule"));
        assertEquals(calendar.nextBusinessSchedule(date1, 16),
                getVal(emptyTable(1).update("nextBusinessSchedule = nextBusinessSchedule(date1, 16)"),
                        "nextBusinessSchedule"));


        assertEquals(calendar.nextNonBusinessDay(),
                getVal(emptyTable(1).update("nextNonBusinessDay = nextNonBusinessDay()"), "nextNonBusinessDay"));
        assertEquals(calendar.nextNonBusinessDay(12),
                getVal(emptyTable(1).update("nextNonBusinessDay = nextNonBusinessDay(12)"), "nextNonBusinessDay"));
        assertEquals(calendar.nextNonBusinessDay(time1),
                getVal(emptyTable(1).update("nextNonBusinessDay = nextNonBusinessDay(time1)"), "nextNonBusinessDay"));
        assertEquals(calendar.nextNonBusinessDay(time1, 6), getVal(
                emptyTable(1).update("nextNonBusinessDay = nextNonBusinessDay(time1, 6)"), "nextNonBusinessDay"));
        assertEquals(calendar.nextNonBusinessDay(date1),
                getVal(emptyTable(1).update("nextNonBusinessDay = nextNonBusinessDay(date1)"), "nextNonBusinessDay"));
        assertEquals(calendar.nextNonBusinessDay(date1, 16), getVal(
                emptyTable(1).update("nextNonBusinessDay = nextNonBusinessDay(date1, 16)"), "nextNonBusinessDay"));


        assertEquals(calendar.businessDaysInRange(time1, time2),
                (String[]) getVal(emptyTable(1).update("businessDaysInRange = businessDaysInRange(time1, time2)"),
                        "businessDaysInRange"));
        assertEquals(calendar.businessDaysInRange(date1, date2),
                (String[]) getVal(emptyTable(1).update("businessDaysInRange = businessDaysInRange(date1, date2)"),
                        "businessDaysInRange"));


        assertEquals(calendar.nonBusinessDaysInRange(time1, time2),
                (String[]) getVal(emptyTable(1).update("nonBusinessDaysInRange = nonBusinessDaysInRange(time1, time2)"),
                        "nonBusinessDaysInRange"));
        assertEquals(calendar.nonBusinessDaysInRange(date1, date2),
                (String[]) getVal(emptyTable(1).update("nonBusinessDaysInRange = nonBusinessDaysInRange(date1, date2)"),
                        "nonBusinessDaysInRange"));


        assertEquals(calendar.standardBusinessDayLengthNanos(),
                getVal(emptyTable(1).update("standardBusinessDayLengthNanos = standardBusinessDayLengthNanos()"),
                        "standardBusinessDayLengthNanos"));


        assertEquals(calendar.diffBusinessNanos(time1, time2), getVal(
                emptyTable(1).update("diffBusinessNanos = diffBusinessNanos(time1, time2)"), "diffBusinessNanos"));
        assertEquals(calendar.diffNonBusinessNanos(time1, time2),
                getVal(emptyTable(1).update("diffNonBusinessNanos = diffNonBusinessNanos(time1, time2)"),
                        "diffNonBusinessNanos"));
        assertEquals(calendar.diffBusinessDay(time1, time2),
                getVal(emptyTable(1).update("diffBusinessDay = diffBusinessDay(time1, time2)"), "diffBusinessDay"));
        assertEquals(calendar.diffNonBusinessDay(time1, time2), getVal(
                emptyTable(1).update("diffNonBusinessDay = diffNonBusinessDay(time1, time2)"), "diffNonBusinessDay"));
        assertEquals(calendar.diffBusinessYear(time1, time2),
                getVal(emptyTable(1).update("diffBusinessYear = diffBusinessYear(time1, time2)"), "diffBusinessYear"));



        assertEquals(calendar.numberOfBusinessDays(time1, time2),
                getVal(emptyTable(1).update("numberOfBusinessDays = numberOfBusinessDays(time1, time2)"),
                        "numberOfBusinessDays"));
        assertEquals(calendar.numberOfBusinessDays(time1, time2, true),
                getVal(emptyTable(1).update("numberOfBusinessDays = numberOfBusinessDays(time1, time2, true)"),
                        "numberOfBusinessDays"));
        assertEquals(calendar.numberOfBusinessDays(date1, date2),
                getVal(emptyTable(1).update("numberOfBusinessDays = numberOfBusinessDays(date1, date2)"),
                        "numberOfBusinessDays"));
        assertEquals(calendar.numberOfBusinessDays(date1, date2, true),
                getVal(emptyTable(1).update("numberOfBusinessDays = numberOfBusinessDays(date1, date2, true)"),
                        "numberOfBusinessDays"));


        assertEquals(calendar.numberOfNonBusinessDays(time1, time2),
                getVal(emptyTable(1).update("numberOfNonBusinessDays = numberOfNonBusinessDays(time1, time2)"),
                        "numberOfNonBusinessDays"));
        assertEquals(calendar.numberOfNonBusinessDays(time1, time2, true),
                getVal(emptyTable(1).update("numberOfNonBusinessDays = numberOfNonBusinessDays(time1, time2, true)"),
                        "numberOfNonBusinessDays"));
        assertEquals(calendar.numberOfNonBusinessDays(date1, date2),
                getVal(emptyTable(1).update("numberOfNonBusinessDays = numberOfNonBusinessDays(date1, date2)"),
                        "numberOfNonBusinessDays"));
        assertEquals(calendar.numberOfNonBusinessDays(date1, date2, true),
                getVal(emptyTable(1).update("numberOfNonBusinessDays = numberOfNonBusinessDays(date1, date2, true)"),
                        "numberOfNonBusinessDays"));


        assertEquals(calendar.fractionOfStandardBusinessDay(),
                getVal(emptyTable(1).update("fractionOfStandardBusinessDay = fractionOfStandardBusinessDay()"),
                        "fractionOfStandardBusinessDay"));
        assertEquals(calendar.fractionOfStandardBusinessDay(time1),
                getVal(emptyTable(1).update("fractionOfStandardBusinessDay = fractionOfStandardBusinessDay(time1)"),
                        "fractionOfStandardBusinessDay"));
        assertEquals(calendar.fractionOfStandardBusinessDay(date1),
                getVal(emptyTable(1).update("fractionOfStandardBusinessDay = fractionOfStandardBusinessDay(date1)"),
                        "fractionOfStandardBusinessDay"));


        assertEquals(calendar.fractionOfBusinessDayRemaining(time1),
                getVal(emptyTable(1).update("fractionOfBusinessDayRemaining = fractionOfBusinessDayRemaining(time1)"),
                        "fractionOfBusinessDayRemaining"));
        assertEquals(calendar.fractionOfBusinessDayComplete(time1),
                getVal(emptyTable(1).update("fractionOfBusinessDayComplete = fractionOfBusinessDayComplete(time1)"),
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


        assertEquals(calendar.getBusinessSchedule(time1), getVal(
                emptyTable(1).update("getBusinessSchedule = getBusinessSchedule(time1)"), "getBusinessSchedule"));
        assertEquals(calendar.getBusinessSchedule(date1), getVal(
                emptyTable(1).update("getBusinessSchedule = getBusinessSchedule(date1)"), "getBusinessSchedule"));
        assertEquals(calendar.getBusinessSchedule(localDate), getVal(
                emptyTable(1).update("getBusinessSchedule = getBusinessSchedule(localDate)"), "getBusinessSchedule"));
    }

    private Object getVal(final Table t, final String column) {
        return t.getColumn(column).get(0);
    }
}
