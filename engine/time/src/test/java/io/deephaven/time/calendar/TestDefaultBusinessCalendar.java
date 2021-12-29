/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.TimeZone;
import io.deephaven.util.QueryConstants;

import java.io.File;
import java.io.FileWriter;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.util.Arrays;

@SuppressWarnings("ConstantConditions")
public class TestDefaultBusinessCalendar extends BaseArrayTestCase {

    private final BusinessCalendar USNYSE = Calendars.calendar("USNYSE");
    private final BusinessCalendar JPOSE = Calendars.calendar("JPOSE");
    private final BusinessCalendar UTC = Calendars.calendar("UTC");

    private final String curDay = "2017-09-27";
    private File testCal;
    private BusinessCalendar test;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        testCal = File.createTempFile("Test", ".calendar");
        final FileWriter fw = new FileWriter(testCal);
        fw.write("<!--\n" +
                "  ~ Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
                "  -->\n" +
                "\n" +
                "<calendar>\n" +
                "    <name>TEST</name>\n" +
                "    <timeZone>TZ_NY</timeZone>\n" +
                "    <language>en</language>\n" +
                "    <country>US</country>\n" +
                "    <default>\n" +
                "        <businessPeriod>09:30,16:00</businessPeriod>\n" +
                "        <weekend>Saturday</weekend>\n" +
                "        <weekend>Sunday</weekend>\n" +
                "    </default>\n" +
                "</calendar>");
        fw.flush();
        fw.close();



        test = new DefaultBusinessCalendar(DefaultBusinessCalendar.constructCalendarElements(testCal)) {
            @Override
            public String currentDay() {
                return curDay;
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        assertTrue(testCal.delete());
        super.tearDown();
    }

    public void testName() {
        Calendar cal = Calendars.calendar("USNYSE");
        assertEquals(cal.name(), "USNYSE");
    }

    public void testNextDay() {
        assertEquals("2017-09-28", test.nextDay());
        assertEquals("2017-09-29", test.nextDay(2));
        assertEquals("2017-10-11", test.nextDay(14));

        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        String day2 = "2016-09-02";
        assertEquals(USNYSE.nextDay(day1, 2), day2);
        assertEquals(JPOSE.nextDay(day1, 2), day2);
        assertEquals(USNYSE.nextDay(day2, -2), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.nextDay(day2, -2), day1.toDateString(TimeZone.TZ_JP));

        assertEquals(USNYSE.nextDay(day1, 0), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.nextDay(day1, 0), day1.toDateString(TimeZone.TZ_JP));

        // leap day
        day1 = DateTimeUtils.convertDateTime("2016-02-28T01:00:00.000000000 NY");
        day2 = "2016-02-29";
        assertEquals(USNYSE.nextDay(day1), day2);
        assertEquals(JPOSE.nextDay(day1), day2);

        // new year
        day1 = DateTimeUtils.convertDateTime("2013-12-31T01:00:00.000000000 NY");
        day2 = "2014-01-05";
        assertEquals(USNYSE.nextDay(day1, 5), day2);
        assertEquals(JPOSE.nextDay(day1, 5), day2);
        assertEquals(USNYSE.nextDay(day2, -5), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.nextDay(day2, -5), day1.toDateString(TimeZone.TZ_JP));

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = DateTimeUtils.convertDateTime("2017-03-12T01:00:00.000000000 NY");
        day2 = "2017-03-13";
        assertEquals(USNYSE.nextDay(day1), day2);
        assertEquals(JPOSE.nextDay(day1), day2);

        // outside calendar range
        day1 = DateTimeUtils.convertDateTime("2069-12-31T01:00:00.000000000 NY");
        day2 = "2070-01-01";
        assertEquals(USNYSE.nextDay(day1), day2);
        assertEquals(JPOSE.nextDay(day1), day2);

        day1 = null;
        assertNull(USNYSE.nextDay(day1));
        assertNull(JPOSE.nextDay(day1));
    }

    public void testNextDayString() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-04";
        assertEquals(USNYSE.nextDay(day1, 4), day2);
        assertEquals(JPOSE.nextDay(day1, 4), day2);
        assertEquals(USNYSE.nextDay(day2, -4), day1);
        assertEquals(JPOSE.nextDay(day2, -4), day1);

        assertEquals(USNYSE.nextDay(day1, 0), day1);
        assertEquals(JPOSE.nextDay(day1, 0), day1);

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-02-29";
        assertEquals(USNYSE.nextDay(day1), day2);
        assertEquals(JPOSE.nextDay(day1), day2);

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertEquals(USNYSE.nextDay(day1), day2);
        assertEquals(JPOSE.nextDay(day1), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-12";
        day2 = "2017-03-15";
        assertEquals(USNYSE.nextDay(day1, 3), day2);
        assertEquals(JPOSE.nextDay(day1, 3), day2);
        assertEquals(USNYSE.nextDay(day2, -3), day1);
        assertEquals(JPOSE.nextDay(day2, -3), day1);

        day1 = null;
        assertNull(USNYSE.nextDay(day1));
        assertNull(JPOSE.nextDay(day1));


        day1 = "2014-03-10";
        day2 = "2017-03-13";
        assertEquals(USNYSE.nextDay(day1, 1099), day2);

        // incorrectly formatted days
        try {
            USNYSE.nextDay("2018-02-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            USNYSE.nextDay("20193-02-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testPreviousDay() {
        assertEquals("2017-09-26", test.previousDay());
        assertEquals("2017-09-25", test.previousDay(2));
        assertEquals("2017-09-13", test.previousDay(14));

        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime day2 = DateTimeUtils.convertDateTime("2016-09-01T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousDay(day2), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.previousDay(day2), day1.toDateString(TimeZone.TZ_JP));

        assertEquals(USNYSE.previousDay(day1, 0), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.previousDay(day1, 0), day1.toDateString(TimeZone.TZ_JP));

        // leap day
        day1 = DateTimeUtils.convertDateTime("2016-02-29T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2016-03-01T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousDay(day2), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.previousDay(day2), day1.toDateString(TimeZone.TZ_JP));

        // new year
        day1 = DateTimeUtils.convertDateTime("2013-12-29T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2014-01-01T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousDay(day2, 3), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.previousDay(day2, 3), day1.toDateString(TimeZone.TZ_JP));
        assertEquals(USNYSE.previousDay(day1, -3), day2.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.previousDay(day1, -3), day2.toDateString(TimeZone.TZ_JP));

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = DateTimeUtils.convertDateTime("2017-03-11T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2017-03-13T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousDay(day2, 2), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.previousDay(day2, 2), day1.toDateString(TimeZone.TZ_JP));
        assertEquals(USNYSE.previousDay(day1, -2), day2.toDateString(TimeZone.TZ_NY));
        assertEquals(JPOSE.previousDay(day1, -2), day2.toDateString(TimeZone.TZ_JP));

        day1 = null;
        assertNull(USNYSE.previousDay(day1));
        assertNull(JPOSE.previousDay(day1));
    }

    public void testPreviousDayString() {
        String day1 = "2016-08-30";
        String day2 = "2016-09-01";
        assertEquals(USNYSE.previousDay(day2, 2), day1);
        assertEquals(JPOSE.previousDay(day2, 2), day1);
        assertEquals(USNYSE.previousDay(day1, -2), day2);
        assertEquals(JPOSE.previousDay(day1, -2), day2);

        assertEquals(USNYSE.previousDay(day1, 0), day1);
        assertEquals(JPOSE.previousDay(day1, 0), day1);

        // leap day
        day1 = "2016-02-29";
        day2 = "2016-03-01";
        assertEquals(USNYSE.previousDay(day2), day1);
        assertEquals(JPOSE.previousDay(day2), day1);

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertEquals(USNYSE.previousDay(day2), day1);
        assertEquals(JPOSE.previousDay(day2), day1);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-10";
        day2 = "2017-03-13";
        assertEquals(USNYSE.previousDay(day2, 3), day1);
        assertEquals(JPOSE.previousDay(day2, 3), day1);
        assertEquals(USNYSE.previousDay(day1, -3), day2);
        assertEquals(JPOSE.previousDay(day1, -3), day2);

        day1 = null;
        assertNull(USNYSE.previousDay(day1));
        assertNull(JPOSE.previousDay(day1));

        day1 = "2014-03-10";
        day2 = "2017-03-13";
        assertEquals(USNYSE.previousDay(day2, 1099), day1);

        // incorrectly formatted days
        try {
            USNYSE.previousDay("2018-02-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            USNYSE.previousDay("20193-02-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testDateRange() {
        // day light savings
        DateTime startDate = DateTimeUtils.convertDateTime("2017-03-11T01:00:00.000000000 NY");
        DateTime endDate = DateTimeUtils.convertDateTime("2017-03-14T01:00:00.000000000 NY");

        String[] goodResults = new String[] {
                "2017-03-11",
                "2017-03-12",
                "2017-03-13",
                "2017-03-14"
        };

        String[] results = USNYSE.daysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        boolean answer = Arrays.equals(goodResults, results);
        assertTrue(answer);


        startDate = DateTimeUtils.convertDateTime("2017-03-11T01:00:00.000000000 JP");
        endDate = DateTimeUtils.convertDateTime("2017-03-14T01:00:00.000000000 JP");
        results = JPOSE.daysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        answer = Arrays.equals(goodResults, results);
        assertTrue(answer);


        startDate = null;
        assertEquals(USNYSE.daysInRange(startDate, endDate).length, 0);
        assertEquals(JPOSE.daysInRange(startDate, endDate).length, 0);
    }

    public void testDateStringRange() {
        String startDate = "2014-02-18";
        String endDate = "2014-03-05";
        String[] goodResults = new String[] {
                "2014-02-18", "2014-02-19", "2014-02-20", "2014-02-21", "2014-02-22", "2014-02-23",
                "2014-02-24", "2014-02-25", "2014-02-26", "2014-02-27", "2014-02-28",
                "2014-03-01", "2014-03-02", "2014-03-03", "2014-03-04", "2014-03-05"
        };

        String[] results = USNYSE.daysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        boolean answer = Arrays.equals(goodResults, results);
        assertTrue(answer);

        results = JPOSE.daysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        answer = Arrays.equals(goodResults, results);
        assertTrue(answer);

        startDate = "2020-01-01";
        endDate = "2020-01-20";

        results = USNYSE.daysInRange(startDate, endDate);
        assertEquals(results.length, 20);

        results = JPOSE.daysInRange(startDate, endDate);
        assertEquals(results.length, 20);

        startDate = null;
        assertEquals(USNYSE.daysInRange(startDate, endDate).length, 0);
        assertEquals(JPOSE.daysInRange(startDate, endDate).length, 0);


        // incorrectly formatted days
        assertEquals(new String[0], USNYSE.daysInRange("2018-02-31", "2019-02-31"));
    }

    public void testNumberOfDays() {
        DateTime startDate = DateTimeUtils.convertDateTime("2014-02-18T01:00:00.000000000 NY");
        DateTime endDate = DateTimeUtils.convertDateTime("2014-03-05T01:00:00.000000000 NY");

        assertEquals(USNYSE.numberOfDays(startDate, endDate), 15);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 11);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, false), 11);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, true), 12);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 4);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, false), 4);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, true), 4);


        startDate = DateTimeUtils.convertDateTime("2020-01-01T01:00:00.000000000 NY");
        endDate = DateTimeUtils.convertDateTime("2020-01-20T01:00:00.000000000 NY");

        assertEquals(USNYSE.numberOfDays(startDate, endDate), 19);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 12);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, true), 12);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 7);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, true), 8);

        startDate = endDate;
        assertEquals(USNYSE.numberOfDays(startDate, endDate), 0);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 0);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, true), 0);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 0);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, true), 1);

        startDate = DateTimeUtils.convertDateTime("2020-01-02T01:00:00.000000000 NY");
        endDate = startDate;
        assertEquals(USNYSE.numberOfDays(startDate, endDate), 0);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 0);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, true), 1);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 0);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, true), 0);

        startDate = null;
        assertEquals(USNYSE.numberOfDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, true), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, true), QueryConstants.NULL_INT);

        startDate = DateTimeUtils.convertDateTime("2014-02-18T01:00:00.000000000 NY");
        endDate = null;
        assertEquals(USNYSE.numberOfDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, true), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, true), QueryConstants.NULL_INT);

        startDate = DateTimeUtils.convertDateTime("2014-02-18T01:00:00.000000000 NY");
        endDate = DateTimeUtils.convertDateTime("2017-02-18T01:00:00.000000000 NY");
        assertEquals(USNYSE.numberOfDays(startDate, endDate), 1096);
        assertEquals(USNYSE.numberOfDays(startDate, endDate, true), 1097);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 758);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, true), 758);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 338);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, true), 339);
    }

    public void testNumberOfDaysString() {
        String startDate = "2014-02-18";
        String endDate = "2014-03-05";

        assertEquals(USNYSE.numberOfDays(startDate, endDate), 15);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 11);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 4);

        assertEquals(USNYSE.numberOfDays(startDate, endDate, false), 15);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, false), 11);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, false), 4);

        assertEquals(USNYSE.numberOfDays(startDate, endDate, true), 16);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, true), 12);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, true), 4);

        assertEquals(USNYSE.numberOfDays(endDate, startDate), -15);
        assertEquals(USNYSE.numberOfBusinessDays(endDate, startDate), -11);
        assertEquals(USNYSE.numberOfNonBusinessDays(endDate, startDate), -4);

        assertEquals(USNYSE.numberOfDays(endDate, startDate, false), -15);
        assertEquals(USNYSE.numberOfBusinessDays(endDate, startDate, false), -11);
        assertEquals(USNYSE.numberOfNonBusinessDays(endDate, startDate, false), -4);

        assertEquals(USNYSE.numberOfDays(endDate, startDate, true), -16);
        assertEquals(USNYSE.numberOfBusinessDays(endDate, startDate, true), -12);
        assertEquals(USNYSE.numberOfNonBusinessDays(endDate, startDate, true), -4);

        endDate = startDate;

        assertEquals(USNYSE.numberOfDays(startDate, endDate), 0);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 0);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 0);


        startDate = "2020-01-01";
        endDate = "2020-01-20";

        assertEquals(USNYSE.numberOfDays(startDate, endDate), 19);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 12);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 7);

        startDate = null;
        assertEquals(USNYSE.numberOfDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), QueryConstants.NULL_INT);

        startDate = "2014-02-18";
        endDate = null;
        assertEquals(USNYSE.numberOfDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), QueryConstants.NULL_INT);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), QueryConstants.NULL_INT);



        startDate = "2014-02-18";
        endDate = "2017-02-18";
        assertEquals(USNYSE.numberOfDays(startDate, endDate), 1096);
        assertEquals(USNYSE.numberOfDays(startDate, endDate, true), 1097);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate), 758);
        assertEquals(USNYSE.numberOfBusinessDays(startDate, endDate, true), 758);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate), 338);
        assertEquals(USNYSE.numberOfNonBusinessDays(startDate, endDate, true), 339);


        // incorrectly formatted days
        try {
            USNYSE.numberOfDays("2018-02-31", "2019-02-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testIsBusinessDay() {
        assertTrue(test.isBusinessDay());

        DateTime businessDay = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime halfDay = DateTimeUtils.convertDateTime("2014-07-03T01:00:00.000000000 NY");
        DateTime holiday = DateTimeUtils.convertDateTime("2002-01-01T01:00:00.000000000 NY");
        DateTime holiday2 = DateTimeUtils.convertDateTime("2002-01-21T01:00:00.000000000 NY");

        assertTrue(USNYSE.isBusinessDay(businessDay));
        assertTrue(USNYSE.isBusinessDay(halfDay));
        assertFalse(USNYSE.isBusinessDay(holiday));
        assertFalse(USNYSE.isBusinessDay(holiday2));

        businessDay = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 JP");
        halfDay = DateTimeUtils.convertDateTime("2006-01-04T01:00:00.000000000 JP");
        holiday = DateTimeUtils.convertDateTime("2006-01-02T01:00:00.000000000 JP");
        holiday2 = DateTimeUtils.convertDateTime("2007-12-23T01:00:00.000000000 JP");

        assertTrue(JPOSE.isBusinessDay(businessDay));
        assertTrue(JPOSE.isBusinessDay(halfDay));
        assertFalse(JPOSE.isBusinessDay(holiday));
        assertFalse(JPOSE.isBusinessDay(holiday2));


        businessDay = null;
        // noinspection ConstantConditions
        assertFalse(USNYSE.isBusinessDay(businessDay));
        // noinspection ConstantConditions
        assertFalse(JPOSE.isBusinessDay(businessDay));
    }

    public void testIsBusinessTime() {
        DateTime businessDayNotTime = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime halfDayTime = DateTimeUtils.convertDateTime("2014-07-03T12:00:00.000000000 NY");
        DateTime holiday = DateTimeUtils.convertDateTime("2002-01-01T01:00:00.000000000 NY");
        DateTime holiday2 = DateTimeUtils.convertDateTime("2002-01-21T01:00:00.000000000 NY");

        assertFalse(USNYSE.isBusinessTime(businessDayNotTime));
        assertTrue(USNYSE.isBusinessTime(halfDayTime));
        assertFalse(USNYSE.isBusinessTime(holiday));
        assertFalse(USNYSE.isBusinessTime(holiday2));

        DateTime businessDayTime = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 JP");
        halfDayTime = DateTimeUtils.convertDateTime("2006-01-04T11:00:00.000000000 JP");
        holiday = DateTimeUtils.convertDateTime("2006-01-02T01:00:00.000000000 JP");
        holiday2 = DateTimeUtils.convertDateTime("2007-12-23T01:00:00.000000000 JP");

        assertFalse(JPOSE.isBusinessTime(businessDayTime));
        assertTrue(JPOSE.isBusinessTime(halfDayTime));
        assertFalse(JPOSE.isBusinessTime(holiday));
        assertFalse(JPOSE.isBusinessTime(holiday2));


        holiday = null;
        // noinspection ConstantConditions
        assertFalse(USNYSE.isBusinessTime(holiday));
        // noinspection ConstantConditions
        assertFalse(JPOSE.isBusinessTime(holiday));
    }

    public void testIsBusinessDayString() {
        String businessDay = "2016-08-31";
        String halfDay = "2014-07-03";
        String holiday = "2002-01-01";
        String holiday2 = "2002-01-21";

        assertTrue(USNYSE.isBusinessDay(businessDay));
        assertTrue(USNYSE.isBusinessDay(halfDay));
        assertFalse(USNYSE.isBusinessDay(holiday));
        assertFalse(USNYSE.isBusinessDay(holiday2));

        businessDay = "2016-08-31";
        halfDay = "2006-01-04";
        holiday = "2007-09-17";
        holiday2 = "2006-02-11";

        assertTrue(JPOSE.isBusinessDay(businessDay));
        assertTrue(JPOSE.isBusinessDay(halfDay));
        assertFalse(JPOSE.isBusinessDay(holiday));
        assertFalse(JPOSE.isBusinessDay(holiday2));

        businessDay = null;
        assertFalse(JPOSE.isBusinessDay(businessDay));
        assertFalse(JPOSE.isBusinessDay(businessDay));


        // incorrectly formatted days
        try {
            USNYSE.isBusinessDay("2018-09-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testNextBusinessDay() {
        assertEquals("2017-09-28", test.nextBusinessDay());
        assertEquals("2017-09-29", test.nextBusinessDay(2));
        assertEquals("2017-10-17", test.nextBusinessDay(14));

        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime day1JP = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 JP");
        String day2 = "2016-09-01";
        assertNull(USNYSE.nextBusinessDay((DateTime) null));
        assertEquals(USNYSE.nextBusinessDay(day1), day2);
        assertEquals(JPOSE.nextBusinessDay(day1JP), day2);

        assertNull(USNYSE.nextBusinessDay((DateTime) null, 2));
        assertEquals(USNYSE.nextBusinessDay(day1, 2), "2016-09-02");
        assertEquals(JPOSE.nextBusinessDay(day1JP, 2), "2016-09-02");

        assertEquals(USNYSE.nextBusinessDay(DateTimeUtils.convertDateTime("2016-09-02T01:00:00.000000000 NY"), -2),
                "2016-08-31");
        assertEquals(JPOSE.nextBusinessDay(DateTimeUtils.convertDateTime("2016-09-02T01:00:00.000000000 JP"), -2),
                "2016-08-31");

        assertEquals(USNYSE.nextBusinessDay(DateTimeUtils.convertDateTime("2016-08-30T01:00:00.000000000 NY"), 0),
                "2016-08-30");
        assertNull(USNYSE.nextBusinessDay(DateTimeUtils.convertDateTime("2016-08-28T01:00:00.000000000 NY"), 0));

        // leap day
        day1 = DateTimeUtils.convertDateTime("2016-02-28T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2016-02-28T01:00:00.000000000 JP");
        day2 = "2016-02-29";
        assertEquals(USNYSE.nextBusinessDay(day1), day2);
        assertEquals(JPOSE.nextBusinessDay(day1JP), day2);

        // new year
        day1 = DateTimeUtils.convertDateTime("2013-12-31T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2013-12-31T01:00:00.000000000 JP");
        day2 = "2014-01-02";
        assertEquals(USNYSE.nextBusinessDay(day1), day2);

        day2 = "2014-01-01";
        assertEquals(JPOSE.nextBusinessDay(day1JP), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        // Japan doesn't observe day light savings
        day1 = DateTimeUtils.convertDateTime("2017-03-12T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2017-03-12T01:00:00.000000000 JP");
        day2 = "2017-03-13";
        assertEquals(USNYSE.nextBusinessDay(day1), day2);
        assertEquals(JPOSE.nextBusinessDay(day1JP), day2);

        // outside calendar range, so no day off for new years, but weekend should still be off
        day1 = DateTimeUtils.convertDateTime("2069-12-31T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2069-12-31T01:00:00.000000000 JP");
        day2 = "2070-01-01";
        assertEquals(USNYSE.nextBusinessDay(day1).compareTo(day2), 0);
        assertEquals(JPOSE.nextBusinessDay(day1JP), day2);

        day1 = DateTimeUtils.convertDateTime("2070-01-03T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2070-01-03T01:00:00.000000000 JP");
        day2 = "2070-01-06";
        assertEquals(USNYSE.nextBusinessDay(day1), day2);
        assertEquals(JPOSE.nextBusinessDay(day1JP), day2);

        day1 = null;
        assertNull(USNYSE.nextBusinessDay(day1));
        assertNull(JPOSE.nextBusinessDay(day1));
    }

    public void testNextBusinessDayString() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertNull(USNYSE.nextBusinessDay((String) null));
        assertEquals(USNYSE.nextBusinessDay(day1), day2);
        assertEquals(JPOSE.nextBusinessDay(day1), day2);

        assertNull(USNYSE.nextBusinessDay((String) null, 2));
        assertEquals(USNYSE.nextBusinessDay(day1, 2), "2016-09-02");
        assertEquals(JPOSE.nextBusinessDay(day1, 2), "2016-09-02");

        assertEquals(USNYSE.nextBusinessDay("2016-09-02", -2), "2016-08-31");
        assertEquals(JPOSE.nextBusinessDay("2016-09-02", -2), "2016-08-31");

        assertEquals(USNYSE.nextBusinessDay("2016-08-30", 0), "2016-08-30");
        assertNull(USNYSE.nextBusinessDay("2016-08-28", 0));

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-02-29";
        assertEquals(USNYSE.nextBusinessDay(day1), day2);
        assertEquals(JPOSE.nextBusinessDay(day1), day2);

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-02";
        assertEquals(USNYSE.nextBusinessDay(day1), day2);

        day1 = "2007-01-01";
        day2 = "2007-01-04";
        assertEquals(JPOSE.nextBusinessDay(day1), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-12";
        day2 = "2017-03-13";
        assertEquals(USNYSE.nextBusinessDay(day1), day2);
        assertEquals(JPOSE.nextBusinessDay(day1), day2);

        day1 = null;
        assertNull(USNYSE.nextBusinessDay(day1));
        assertNull(JPOSE.nextBusinessDay(day1));

        // incorrectly formatted days
        try {
            USNYSE.nextBusinessDay("2018-09-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testNextBusinessSchedule() {
        assertEquals(test.nextBusinessSchedule(curDay), test.nextBusinessSchedule());
        assertEquals(test.nextBusinessSchedule(curDay, 2), test.nextBusinessSchedule(2));

        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime day1JP = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 JP");
        String day2 = "2016-09-01";
        assertEquals(USNYSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_NY), day2);
        assertEquals(JPOSE.nextBusinessSchedule(day1JP).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        assertEquals(USNYSE.nextBusinessSchedule(day1, 2).getSOBD().toDateString(TimeZone.TZ_NY), "2016-09-02");
        assertEquals(JPOSE.nextBusinessSchedule(day1JP, 2).getSOBD().toDateString(TimeZone.TZ_JP), "2016-09-02");

        assertEquals(USNYSE.nextBusinessSchedule(DateTimeUtils.convertDateTime("2016-09-02T01:00:00.000000000 NY"), -2)
                .getSOBD().toDateString(TimeZone.TZ_NY), "2016-08-31");
        assertEquals(JPOSE.nextBusinessSchedule(DateTimeUtils.convertDateTime("2016-09-02T01:00:00.000000000 JP"), -2)
                .getSOBD().toDateString(TimeZone.TZ_JP), "2016-08-31");

        assertEquals(USNYSE.nextBusinessSchedule(DateTimeUtils.convertDateTime("2016-08-30T01:00:00.000000000 NY"), 0)
                .getSOBD().toDateString(TimeZone.TZ_NY), "2016-08-30");

        // leap day
        day1 = DateTimeUtils.convertDateTime("2016-02-28T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2016-02-28T01:00:00.000000000 JP");
        day2 = "2016-02-29";
        assertEquals(USNYSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_NY), day2);
        assertEquals(JPOSE.nextBusinessSchedule(day1JP).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        // new year
        day1 = DateTimeUtils.convertDateTime("2013-12-31T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2013-12-31T01:00:00.000000000 JP");
        day2 = "2014-01-03";
        assertEquals(USNYSE.nextBusinessSchedule(USNYSE.nextBusinessDay(day1)).getSOBD().toDateString(TimeZone.TZ_NY),
                day2);

        day2 = "2014-01-01";
        assertEquals(JPOSE.nextBusinessSchedule(day1JP).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        // Japan doesn't observe day light savings
        day1 = DateTimeUtils.convertDateTime("2017-03-12T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2017-03-12T01:00:00.000000000 JP");
        day2 = "2017-03-13";
        assertEquals(USNYSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_NY), day2);
        assertEquals(JPOSE.nextBusinessSchedule(day1JP).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        // outside calendar range, so no day off for new years, but weekend should still be off
        day1 = DateTimeUtils.convertDateTime("2069-12-31T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2069-12-31T01:00:00.000000000 JP");
        day2 = "2070-01-01";
        assertEquals(USNYSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_NY).compareTo(day2), 0);
        assertEquals(JPOSE.nextBusinessSchedule(day1JP).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        day1 = DateTimeUtils.convertDateTime("2070-01-05T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2070-01-05T01:00:00.000000000 JP");
        day2 = "2070-01-06";
        assertEquals(USNYSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_NY), day2);
        assertEquals(JPOSE.nextBusinessSchedule(day1JP).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        day1 = null;
        assertNull(USNYSE.nextBusinessSchedule(day1));
        assertNull(JPOSE.nextBusinessSchedule(day1));


        // holiday
        final BusinessSchedule holiday = USNYSE.getBusinessSchedule("2017-12-25");
        assertEquals(0, holiday.getBusinessPeriods().length);
        assertEquals(0, holiday.getLengthOfBusinessDay());
        try {
            // noinspection ResultOfMethodCallIgnored
            holiday.getEOBD();
            fail("Expected an exception!");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            // noinspection ResultOfMethodCallIgnored
            holiday.getSOBD();
            fail("Expected an exception!");
        } catch (UnsupportedOperationException e) {
            // pass
        }
    }

    public void testNextBusinessScheduleString() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertEquals(USNYSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_NY), day2);
        assertEquals(JPOSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        assertEquals(USNYSE.nextBusinessSchedule(day1, 2).getSOBD().toDateString(TimeZone.TZ_NY), "2016-09-02");
        assertEquals(JPOSE.nextBusinessSchedule(day1, 2).getSOBD().toDateString(TimeZone.TZ_JP), "2016-09-02");

        assertEquals(USNYSE.nextBusinessSchedule("2016-09-02", -2).getSOBD().toDateString(TimeZone.TZ_NY),
                "2016-08-31");
        assertEquals(JPOSE.nextBusinessSchedule("2016-09-02", -2).getSOBD().toDateString(TimeZone.TZ_JP),
                "2016-08-31");

        assertEquals(USNYSE.nextBusinessSchedule("2016-08-30", 0).getSOBD().toDateString(TimeZone.TZ_NY),
                "2016-08-30");
        assertNull(USNYSE.nextBusinessSchedule((String) null, 0));

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-02-29";
        assertEquals(USNYSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_NY), day2);
        assertEquals(JPOSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        // new year
        day1 = "2014-01-01";
        day2 = "2014-01-02";
        assertEquals(USNYSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_NY), day2);

        day1 = "2007-01-03";
        day2 = "2007-01-04";
        assertEquals(JPOSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-12";
        day2 = "2017-03-13";
        assertEquals(USNYSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_NY), day2);
        assertEquals(JPOSE.nextBusinessSchedule(day1).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        day1 = null;
        assertNull(USNYSE.nextBusinessSchedule(day1));
        assertNull(JPOSE.nextBusinessSchedule(day1));
    }

    public void testNextNonBusinessDay() {
        assertEquals("2017-09-30", test.nextNonBusinessDay());
        assertEquals("2017-10-01", test.nextNonBusinessDay(2));
        assertEquals("2017-10-08", test.nextNonBusinessDay(4));

        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime day1JP = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 JP");
        String day2 = "2016-09-03";
        assertNull(USNYSE.nextNonBusinessDay((DateTime) null));
        assertEquals(USNYSE.nextNonBusinessDay(day1), day2);
        assertEquals(JPOSE.nextNonBusinessDay(day1JP), day2);

        assertNull(USNYSE.nextNonBusinessDay((DateTime) null, 2));
        assertEquals(USNYSE.nextNonBusinessDay(day1, 2), "2016-09-04");
        assertEquals(JPOSE.nextNonBusinessDay(day1JP, 2), "2016-09-04");

        assertEquals(USNYSE.nextNonBusinessDay(DateTimeUtils.convertDateTime("2016-09-04T01:00:00.000000000 NY"), -2),
                "2016-08-28");
        assertEquals(JPOSE.nextNonBusinessDay(DateTimeUtils.convertDateTime("2016-09-04T01:00:00.000000000 JP"), -2),
                "2016-08-28");

        assertNull(USNYSE.nextNonBusinessDay(DateTimeUtils.convertDateTime("2016-08-30T01:00:00.000000000 NY"), 0));
        assertEquals(USNYSE.nextNonBusinessDay(DateTimeUtils.convertDateTime("2016-08-28T01:00:00.000000000 NY"), 0),
                "2016-08-28");

        // leap day
        day1 = DateTimeUtils.convertDateTime("2016-02-28T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2016-02-28T01:00:00.000000000 JP");
        day2 = "2016-03-05";
        assertEquals(USNYSE.nextNonBusinessDay(day1), day2);
        assertEquals(JPOSE.nextNonBusinessDay(day1JP), day2);

        // new year
        day1 = DateTimeUtils.convertDateTime("2013-12-31T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2013-12-31T01:00:00.000000000 JP");
        day2 = "2014-01-01";
        assertEquals(USNYSE.nextNonBusinessDay(day1), day2);

        day2 = "2014-01-04";
        assertEquals(JPOSE.nextNonBusinessDay(day1JP), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = DateTimeUtils.convertDateTime("2017-03-12T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2017-03-12T01:00:00.000000000 JP");
        day2 = "2017-03-18";
        assertEquals(USNYSE.nextNonBusinessDay(day1), day2);
        assertEquals(JPOSE.nextNonBusinessDay(day1JP), day2);

        // outside calendar range, so no day off for new years, but weekend should still be off
        day1 = DateTimeUtils.convertDateTime("2069-12-31T01:00:00.000000000 NY");
        day1JP = DateTimeUtils.convertDateTime("2069-12-31T01:00:00.000000000 JP");
        day2 = "2070-01-04";
        assertEquals(USNYSE.nextNonBusinessDay(day1).compareTo(day2), 0);
        assertEquals(JPOSE.nextNonBusinessDay(day1JP), day2);

        day1 = null;
        assertNull(USNYSE.nextNonBusinessDay(day1));
        assertNull(JPOSE.nextNonBusinessDay(day1));
    }

    public void testNextNonBusinessDayString() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-03";
        assertNull(USNYSE.nextNonBusinessDay((String) null));
        assertEquals(USNYSE.nextNonBusinessDay(day1), day2);
        assertEquals(JPOSE.nextNonBusinessDay(day1), day2);

        assertNull(USNYSE.nextNonBusinessDay((String) null, 2));
        assertEquals(USNYSE.nextNonBusinessDay(day1, 2), "2016-09-04");
        assertEquals(JPOSE.nextNonBusinessDay(day1, 2), "2016-09-04");

        assertEquals(USNYSE.nextNonBusinessDay("2016-09-04", -2), "2016-08-28");
        assertEquals(JPOSE.nextNonBusinessDay("2016-09-04", -2), "2016-08-28");

        assertNull(USNYSE.nextNonBusinessDay("2016-08-30", 0));
        assertEquals(USNYSE.nextNonBusinessDay("2016-08-28", 0), "2016-08-28");

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-03-05";
        assertEquals(USNYSE.nextNonBusinessDay(day1), day2);
        assertEquals(JPOSE.nextNonBusinessDay(day1), day2);

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertEquals(USNYSE.nextNonBusinessDay(day1), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-12";
        day2 = "2017-03-18";
        assertEquals(USNYSE.nextNonBusinessDay(day1), day2);
        assertEquals(JPOSE.nextNonBusinessDay(day1), day2);

        day1 = null;
        assertNull(USNYSE.nextNonBusinessDay(day1));
        assertNull(JPOSE.nextNonBusinessDay(day1));

        // incorrectly formatted days
        try {
            USNYSE.nextNonBusinessDay("2018-09-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testLastBusinessDay() {
        assertEquals("2017-09-26", test.previousBusinessDay());
        assertEquals("2017-09-25", test.previousBusinessDay(2));
        assertEquals("2017-09-07", test.previousBusinessDay(14));

        assertEquals("2017-09-24", test.previousNonBusinessDay());
        assertEquals("2017-09-23", test.previousNonBusinessDay(2));
        assertEquals("2017-09-16", test.previousNonBusinessDay(4));


        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-30T01:00:00.000000000 NY");
        DateTime day2 = DateTimeUtils.convertDateTime("2016-09-01T01:00:00.000000000 NY");
        assertNull(USNYSE.previousBusinessDay((DateTime) null, 2));
        assertEquals(USNYSE.previousBusinessDay(day2, 2), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(USNYSE.previousBusinessDay(day1, -2), day2.toDateString(TimeZone.TZ_NY));

        assertEquals(USNYSE.previousBusinessDay(DateTimeUtils.convertDateTime("2016-08-30T15:00:00.000000000 NY"), 0),
                "2016-08-30");
        assertNull(USNYSE.previousBusinessDay(DateTimeUtils.convertDateTime("2016-08-28T15:00:00.000000000 NY"), 0));

        assertNull(USNYSE.previousNonBusinessDay((DateTime) null, 0));
        assertNull(USNYSE.previousNonBusinessDay(DateTimeUtils.convertDateTime("2016-08-30T21:00:00.000000000 NY"), 0));
        assertEquals(
                USNYSE.previousNonBusinessDay(DateTimeUtils.convertDateTime("2016-08-28T21:00:00.000000000 NY"), 0),
                "2016-08-28");

        // leap day
        day1 = DateTimeUtils.convertDateTime("2016-02-29T21:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2016-03-01T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousBusinessDay(day2), day1.toDateString(TimeZone.TZ_NY));

        // new year
        day1 = DateTimeUtils.convertDateTime("2013-12-26T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2014-01-02T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousBusinessDay(day2, 4), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(USNYSE.previousBusinessDay(day1, -4), day2.toDateString(TimeZone.TZ_NY));

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = DateTimeUtils.convertDateTime("2017-02-26T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2017-03-13T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousNonBusinessDay(day2, 5), day1.toDateString(TimeZone.TZ_NY));
        assertEquals(USNYSE.previousNonBusinessDay(day1, -5), "2017-03-18");

        day1 = DateTimeUtils.convertDateTime("2017-03-12T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2017-03-13T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousNonBusinessDay(day2), day1.toDateString(TimeZone.TZ_NY));

        day1 = DateTimeUtils.convertDateTime("2017-07-04T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2017-07-07T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousNonBusinessDay(day2), day1.toDateString(TimeZone.TZ_NY));

        day1 = null;
        assertNull(USNYSE.previousBusinessDay(day1));
        assertNull(USNYSE.previousNonBusinessDay(day1));



        day1 = DateTimeUtils.convertDateTime("2016-08-31T21:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2016-09-01T21:00:00.000000000 JP");
        assertEquals(JPOSE.previousBusinessDay(day2), day1.toDateString(TimeZone.TZ_JP));

        // leap day
        day1 = DateTimeUtils.convertDateTime("2016-02-29T01:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2016-03-01T01:00:00.000000000 JP");
        assertEquals(JPOSE.previousBusinessDay(day2), day1.toDateString(TimeZone.TZ_JP));

        // new year
        day1 = DateTimeUtils.convertDateTime("2013-12-31T11:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2014-01-01T11:00:00.000000000 JP");
        assertEquals(JPOSE.previousBusinessDay(day2), day1.toDateString(TimeZone.TZ_JP));

        // Daylight savings starts in JP (UTC-7:00) at 2 AM 2017-03-12
        day1 = DateTimeUtils.convertDateTime("2017-03-12T01:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2017-03-13T01:00:00.000000000 JP");
        assertEquals(JPOSE.previousNonBusinessDay(day2), day1.toDateString(TimeZone.TZ_JP));


        day1 = null;
        assertNull(JPOSE.previousBusinessDay(day1));
        assertNull(JPOSE.previousNonBusinessDay(day1));
    }

    public void testLastBusinessDayString() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertNull(USNYSE.previousBusinessDay((String) null));
        assertEquals(USNYSE.previousBusinessDay(day2), day1);
        assertEquals(JPOSE.previousBusinessDay(day2), day1);

        assertNull(USNYSE.previousBusinessDay((String) null, 2));
        assertEquals(USNYSE.previousBusinessDay("2016-08-30", 0), "2016-08-30");
        assertNull(USNYSE.previousBusinessDay("2016-08-28", 0));

        day1 = "2016-08-29";
        assertEquals(USNYSE.previousBusinessDay(day2, 3), day1);
        assertEquals(JPOSE.previousBusinessDay(day2, 3), day1);
        assertEquals(USNYSE.previousBusinessDay(day1, -3), day2);
        assertEquals(JPOSE.previousBusinessDay(day1, -3), day2);

        // leap day
        day1 = "2016-02-29";
        day2 = "2016-03-01";
        assertEquals(USNYSE.previousBusinessDay(day2), day1);
        assertEquals(JPOSE.previousBusinessDay(day2), day1);

        // new year
        day1 = "2013-12-30";
        day2 = "2014-01-01";
        assertEquals(USNYSE.previousBusinessDay(day2, 2), day1);
        assertEquals(JPOSE.previousBusinessDay(day2, 2), day1);
        assertEquals(USNYSE.previousBusinessDay(day1, -2), "2014-01-02");
        assertEquals(JPOSE.previousBusinessDay(day1, -2), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-10";
        day2 = "2017-03-13";
        assertEquals(USNYSE.previousBusinessDay(day2), day1);
        assertEquals(JPOSE.previousBusinessDay(day2), day1);

        day1 = null;
        assertNull(USNYSE.previousBusinessDay(day1));
        assertNull(JPOSE.previousBusinessDay(day1));

        // incorrectly formatted days
        try {
            USNYSE.previousBusinessDay("2018-09-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testLastBusinessSchedule() {
        assertEquals(test.previousBusinessSchedule(curDay), test.previousBusinessSchedule());
        assertEquals(test.previousBusinessSchedule(curDay, 2), test.previousBusinessSchedule(2));


        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-30T01:00:00.000000000 NY");
        DateTime day2 = DateTimeUtils.convertDateTime("2016-09-01T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousBusinessSchedule(day2, 2).getSOBD().toDateString(TimeZone.TZ_NY),
                day1.toDateString(TimeZone.TZ_NY));
        assertEquals(USNYSE.previousBusinessSchedule(day1, -2).getSOBD().toDateString(TimeZone.TZ_NY),
                day2.toDateString(TimeZone.TZ_NY));

        assertEquals(
                USNYSE.previousBusinessSchedule(DateTimeUtils.convertDateTime("2016-08-30T15:00:00.000000000 NY"), 0)
                        .getSOBD().toDateString(TimeZone.TZ_NY),
                "2016-08-30");
        assertNull(USNYSE.previousBusinessSchedule((DateTime) null, 0));

        // leap day
        day1 = DateTimeUtils.convertDateTime("2016-02-29T21:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2016-03-01T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousBusinessSchedule(day2).getSOBD().toDateString(TimeZone.TZ_NY),
                day1.toDateString(TimeZone.TZ_NY));

        // new year
        day1 = DateTimeUtils.convertDateTime("2013-12-26T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2014-01-02T01:00:00.000000000 NY");
        assertEquals(USNYSE.previousBusinessSchedule(day2, 7).getSOBD().toDateString(TimeZone.TZ_NY),
                day1.toDateString(TimeZone.TZ_NY));
        assertEquals(USNYSE.previousBusinessSchedule(day1, -7).getSOBD().toDateString(TimeZone.TZ_NY),
                day2.toDateString(TimeZone.TZ_NY));

        day1 = null;
        assertNull(USNYSE.previousBusinessSchedule(day1));


        day1 = DateTimeUtils.convertDateTime("2016-08-31T21:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2016-09-01T21:00:00.000000000 JP");
        assertEquals(JPOSE.previousBusinessSchedule(day2).getSOBD().toDateString(TimeZone.TZ_JP),
                day1.toDateString(TimeZone.TZ_JP));

        // leap day
        day1 = DateTimeUtils.convertDateTime("2016-02-29T01:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2016-03-01T01:00:00.000000000 JP");
        assertEquals(JPOSE.previousBusinessSchedule(day2).getSOBD().toDateString(TimeZone.TZ_JP),
                day1.toDateString(TimeZone.TZ_JP));

        // new year
        day1 = DateTimeUtils.convertDateTime("2013-12-31T11:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2014-01-01T11:00:00.000000000 JP");
        assertEquals(JPOSE.previousBusinessSchedule(day2).getSOBD().toDateString(TimeZone.TZ_JP),
                day1.toDateString(TimeZone.TZ_JP));


        day1 = null;
        assertNull(JPOSE.previousBusinessSchedule(day1));
    }

    public void testLastBusinessScheduleString() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertEquals(USNYSE.previousBusinessSchedule(day2).getSOBD().toDateString(TimeZone.TZ_NY), day1);
        assertEquals(JPOSE.previousBusinessSchedule(day2).getSOBD().toDateString(TimeZone.TZ_JP), day1);

        assertEquals(USNYSE.previousBusinessSchedule("2016-08-30", 0).getSOBD().toDateString(TimeZone.TZ_NY),
                "2016-08-30");
        assertNull(USNYSE.previousBusinessSchedule((String) null, 0));

        day1 = "2016-08-29";
        assertEquals(USNYSE.previousBusinessSchedule(day2, 3).getSOBD().toDateString(TimeZone.TZ_NY), day1);
        assertEquals(JPOSE.previousBusinessSchedule(day2, 3).getSOBD().toDateString(TimeZone.TZ_JP), day1);
        assertEquals(USNYSE.previousBusinessSchedule(day1, -3).getSOBD().toDateString(TimeZone.TZ_NY), day2);
        assertEquals(JPOSE.previousBusinessSchedule(day1, -3).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        // leap day
        day1 = "2016-02-29";
        day2 = "2016-03-01";
        assertEquals(USNYSE.previousBusinessSchedule(day2).getSOBD().toDateString(TimeZone.TZ_NY), day1);
        assertEquals(JPOSE.previousBusinessSchedule(day2).getSOBD().toDateString(TimeZone.TZ_JP), day1);

        // new year
        day1 = "2014-12-29";
        day2 = "2014-12-31";
        assertEquals(USNYSE.previousBusinessSchedule(day2, 2).getSOBD().toDateString(TimeZone.TZ_NY), day1);
        assertEquals(JPOSE.previousBusinessSchedule(day2, 2).getSOBD().toDateString(TimeZone.TZ_JP), day1);
        assertEquals(USNYSE.previousBusinessSchedule(day1, -2).getSOBD().toDateString(TimeZone.TZ_NY), day2);
        assertEquals(JPOSE.previousBusinessSchedule(day1, -2).getSOBD().toDateString(TimeZone.TZ_JP), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-10";
        day2 = "2017-03-13";
        assertEquals(USNYSE.previousBusinessSchedule(USNYSE.previousDay(USNYSE.previousDay(day2))).getSOBD()
                .toDateString(TimeZone.TZ_NY), day1);
        assertEquals(JPOSE.previousBusinessSchedule(JPOSE.previousDay(JPOSE.previousDay(day2))).getSOBD()
                .toDateString(TimeZone.TZ_JP), day1);

        day1 = null;
        assertNull(USNYSE.previousBusinessSchedule(day1));
        assertNull(JPOSE.previousBusinessSchedule(day1));

        // incorrectly formatted days
        try {
            USNYSE.previousBusinessSchedule("2018-09-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testLastNonBusinessDayString() {
        String day1 = "2016-08-28";
        String day2 = "2016-09-01";
        assertNull(USNYSE.previousNonBusinessDay((String) null));
        assertEquals(USNYSE.previousNonBusinessDay(day2), day1);
        assertEquals(JPOSE.previousNonBusinessDay(day2), day1);

        assertNull(USNYSE.previousNonBusinessDay((String) null, 2));
        assertNull(USNYSE.previousNonBusinessDay("2016-08-30", 0));
        assertEquals(USNYSE.previousNonBusinessDay("2016-08-28", 0), "2016-08-28");

        // leap day
        day1 = "2016-02-27";
        day2 = "2016-03-01";
        assertEquals(USNYSE.previousNonBusinessDay(day2, 2), day1);
        assertEquals(JPOSE.previousNonBusinessDay(day2, 2), day1);
        assertEquals(USNYSE.previousNonBusinessDay(day1, -2), "2016-03-05");
        assertEquals(JPOSE.previousNonBusinessDay(day1, -2), "2016-03-05");

        // new year
        day1 = "2013-12-29";
        day2 = "2014-01-01";
        assertEquals(USNYSE.previousNonBusinessDay(day2), day1);
        assertEquals(JPOSE.previousNonBusinessDay(day2), day1);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-05";
        day2 = "2017-03-13";
        assertEquals(USNYSE.previousNonBusinessDay(day2, 3), day1);
        assertEquals(JPOSE.previousNonBusinessDay(day2, 3), day1);
        assertEquals(USNYSE.previousNonBusinessDay(day1, -3), "2017-03-18");
        assertEquals(JPOSE.previousNonBusinessDay(day1, -3), "2017-03-18");

        day1 = null;
        assertNull(USNYSE.previousNonBusinessDay(day1));
        assertNull(JPOSE.previousNonBusinessDay(day1));

        // incorrectly formatted days
        try {
            USNYSE.previousNonBusinessDay("2018-09-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testDiff() {
        // standard business day
        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime day2 = DateTimeUtils.convertDateTime("2016-09-01T01:00:00.000000000 NY");
        assertEquals(USNYSE.diffDay(day1, day2), 1.0);
        assertEquals(USNYSE.diffNanos(day1, day2), DateTimeUtils.DAY);
        assertEquals(JPOSE.diffYear(day1, day2), (double) DateTimeUtils.DAY / (double) DateTimeUtils.YEAR);
    }

    public void testBusinessTimeDiff() {
        // standard business day
        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime day2 = DateTimeUtils.convertDateTime("2016-09-01T01:00:00.000000000 NY");
        assertEquals(USNYSE.diffBusinessDay(day1, day2), 1.0);
        assertEquals(JPOSE.diffBusinessDay(day1, day2), 1.0);

        // 2.5 standard business days
        day1 = DateTimeUtils.convertDateTime("2017-01-23T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2017-01-25T12:45:00.000000000 NY");
        assertEquals(USNYSE.diffBusinessDay(day1, day2), 2.5);

        day1 = DateTimeUtils.convertDateTime("2017-01-23T01:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2017-01-25T12:45:00.000000000 JP");
        assertEquals(JPOSE.diffBusinessDay(day1, day2), 2.55);

        // middle of a business period
        day1 = DateTimeUtils.convertDateTime("2017-01-23T10:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2017-01-23T13:00:00.000000000 JP");
        assertEquals(JPOSE.diffBusinessNanos(day1, day2), 2 * DateTimeUtils.HOUR);

        // after a business period
        day1 = DateTimeUtils.convertDateTime("2017-01-23T10:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2017-01-23T16:15:00.000000000 JP");
        assertEquals(JPOSE.diffBusinessNanos(day1, day2), 4 * DateTimeUtils.HOUR);

        // middle of the second business period
        day1 = DateTimeUtils.convertDateTime("2017-01-23T08:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2017-01-23T14:00:00.000000000 JP");
        assertEquals(JPOSE.diffBusinessNanos(day1, day2), 4 * DateTimeUtils.HOUR);

        // weekend non business
        day1 = DateTimeUtils.convertDateTime("2017-01-21T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2017-01-23T01:00:00.000000000 NY");
        assertEquals(USNYSE.diffBusinessDay(day1, day2), 0.0);

        // one business year
        day1 = DateTimeUtils.convertDateTime("2016-01-01T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2016-12-31T23:59:00.000000000 NY");
        double yearDiff = USNYSE.diffBusinessYear(day1, day2);
        assertTrue(yearDiff < 1.004);
        assertTrue(yearDiff > 0.996);
        yearDiff = JPOSE.diffBusinessYear(day1, day2);
        assertTrue(yearDiff < 1.004);
        assertTrue(yearDiff > 0.996);

        // half year
        day1 = DateTimeUtils.convertDateTime("2017-01-01T01:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2017-07-02T01:00:00.000000000 NY");
        yearDiff = USNYSE.diffBusinessYear(day1, day2);
        assertTrue(yearDiff < 0.503);
        assertTrue(yearDiff > 0.497);
        yearDiff = JPOSE.diffBusinessYear(day1, day2);
        assertTrue(yearDiff < 0.503);
        assertTrue(yearDiff > 0.497);


        day1 = null;
        assertEquals(USNYSE.diffBusinessYear(day1, day2), QueryConstants.NULL_DOUBLE);
        assertEquals(USNYSE.diffBusinessDay(day1, day2), QueryConstants.NULL_DOUBLE);
        assertEquals(USNYSE.diffBusinessNanos(day1, day2), QueryConstants.NULL_LONG);

        day1 = day2;
        assertEquals(USNYSE.diffBusinessYear(day1, day2), 0.0);
        assertEquals(USNYSE.diffBusinessDay(day1, day2), 0.0);
        assertEquals(USNYSE.diffBusinessNanos(day1, day2), 0);
    }

    public void testNonBusinessTimeDiff() {
        // USNYSE
        // standard business day
        DateTime day1 = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime day2 = DateTimeUtils.convertDateTime("2016-09-01T01:00:00.000000000 NY");
        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), 63000000000000L); // 17.5 hours
        assertEquals(USNYSE.diffNonBusinessNanos(day2, day1), -63000000000000L); // 17.5 hours

        // middle of a business period
        day1 = DateTimeUtils.convertDateTime("2017-01-23T10:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2017-01-23T12:30:00.000000000 NY");
        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), 0);

        // after a business period
        day1 = DateTimeUtils.convertDateTime("2017-01-23T10:00:00.000000000 NY");
        day2 = DateTimeUtils.convertDateTime("2017-01-23T16:15:00.000000000 NY");
        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), 15 * DateTimeUtils.MINUTE);

        // JPOSE
        // standard business day
        day1 = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2016-09-01T01:00:00.000000000 JP");
        assertEquals(JPOSE.diffNonBusinessNanos(day1, day2), 19 * DateTimeUtils.HOUR); // 17.5 hours

        // middle of a business period
        day1 = DateTimeUtils.convertDateTime("2017-01-23T10:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2017-01-23T11:30:00.000000000 JP");
        assertEquals(JPOSE.diffNonBusinessNanos(day1, day2), 0);

        // after a business period
        day1 = DateTimeUtils.convertDateTime("2017-01-23T10:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2017-01-23T16:00:00.000000000 JP");
        assertEquals(JPOSE.diffNonBusinessNanos(day1, day2), 2 * DateTimeUtils.HOUR);
        assertEquals(JPOSE.diffNonBusinessDay(day1, day2),
                ((double) (2 * DateTimeUtils.HOUR)) / (double) JPOSE.standardBusinessDayLengthNanos());



        day1 = null;
        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), QueryConstants.NULL_LONG);

        day1 = day2;
        assertEquals(USNYSE.diffNonBusinessNanos(day1, day2), 0);

        day1 = null;
        assertEquals(USNYSE.diffNonBusinessDay(day1, day2), QueryConstants.NULL_DOUBLE);
    }

    public void testBusinessDateRange() {
        // day light savings
        DateTime startDate = DateTimeUtils.convertDateTime("2017-03-11T01:00:00.000000000 NY");
        DateTime endDate = DateTimeUtils.convertDateTime("2017-03-14T01:00:00.000000000 NY");

        String[] goodResults = new String[] {
                "2017-03-13",
                "2017-03-14"
        };

        String[] results = USNYSE.businessDaysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        boolean answer = Arrays.equals(goodResults, results);
        assertTrue(answer);

        assertEquals(new String[0], USNYSE.businessDaysInRange(endDate, startDate));

        startDate = DateTimeUtils.convertDateTime("2017-11-23T01:00:00.000000000 JP");
        endDate = DateTimeUtils.convertDateTime("2017-11-25T01:00:00.000000000 JP");

        goodResults = new String[] {
                "2017-11-24"
        };

        results = JPOSE.businessDaysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        answer = Arrays.equals(goodResults, results);
        assertTrue(answer);

        startDate = null;
        assertEquals(JPOSE.businessDaysInRange(startDate, endDate).length, 0);

        // non business
        startDate = DateTimeUtils.convertDateTime("2017-03-11T01:00:00.000000000 NY");
        endDate = DateTimeUtils.convertDateTime("2017-03-14T01:00:00.000000000 NY");

        goodResults = new String[] {
                "2017-03-11",
                "2017-03-12"
        };

        results = USNYSE.nonBusinessDaysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        answer = Arrays.equals(goodResults, results);
        assertTrue(answer);


        startDate = DateTimeUtils.convertDateTime("2017-11-23T01:00:00.000000000 JP");
        endDate = DateTimeUtils.convertDateTime("2017-11-25T01:00:00.000000000 JP");

        goodResults = new String[] {
                "2017-11-23",
                "2017-11-25"
        };

        assertEquals(new String[0], USNYSE.nonBusinessDaysInRange(endDate, startDate));
        results = JPOSE.nonBusinessDaysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        answer = Arrays.equals(goodResults, results);
        assertTrue(answer);

        startDate = null;
        assertEquals(JPOSE.nonBusinessDaysInRange(startDate, endDate).length, 0);

        startDate = null;
        assertEquals(USNYSE.nonBusinessDaysInRange(startDate, endDate).length, 0);
    }

    public void testBusinessDateStringRange() {
        // USNYSE
        String startDate = "2014-02-16";
        String endDate = "2014-03-05";
        String[] goodResults = new String[] {
                "2014-02-18", "2014-02-19", "2014-02-20", "2014-02-21",
                "2014-02-24", "2014-02-25", "2014-02-26", "2014-02-27", "2014-02-28",
                "2014-03-03", "2014-03-04", "2014-03-05",
        };

        assertEquals(new String[0], USNYSE.businessDaysInRange(endDate, startDate));
        String[] results = USNYSE.businessDaysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        boolean answer = Arrays.equals(goodResults, results);
        assertTrue(answer);

        startDate = null;
        assertEquals(USNYSE.businessDaysInRange(startDate, endDate).length, 0);

        startDate = endDate;
        assertEquals(USNYSE.businessDaysInRange(startDate, endDate).length, 1);

        // JPOSE
        startDate = "2018-01-01";
        endDate = "2018-01-05";
        goodResults = new String[] {
                "2018-01-04",
                "2018-01-05"
        };

        results = JPOSE.businessDaysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        answer = Arrays.equals(goodResults, results);
        assertTrue(answer);


        // non business
        startDate = "2020-01-01";
        endDate = "2020-01-20";
        goodResults = new String[] {
                "2020-01-01", "2020-01-04", "2020-01-05", "2020-01-11", "2020-01-12",
                "2020-01-18", "2020-01-19", "2020-01-20"
        };

        assertEquals(new String[0], USNYSE.nonBusinessDaysInRange(endDate, startDate));
        results = USNYSE.nonBusinessDaysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        answer = Arrays.equals(goodResults, results);
        assertTrue(answer);


        // JPOSE
        startDate = "2018-01-01";
        endDate = "2018-01-05";
        goodResults = new String[] {
                "2018-01-01",
                "2018-01-02",
                "2018-01-03"
        };

        results = JPOSE.nonBusinessDaysInRange(startDate, endDate);
        Arrays.sort(goodResults);
        Arrays.sort(results);
        answer = Arrays.equals(goodResults, results);
        assertTrue(answer);


        // null tests
        startDate = null;
        assertEquals(USNYSE.nonBusinessDaysInRange(startDate, endDate).length, 0);

        startDate = endDate = "2018-01-06";
        assertEquals(USNYSE.nonBusinessDaysInRange(startDate, endDate).length, 1);

        // incorrectly formatted days
        try {
            USNYSE.nonBusinessDaysInRange("2018-09-31", "2018-010-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testDayOfWeek() {
        assertEquals(DayOfWeek.WEDNESDAY, test.dayOfWeek());

        String dateString = "2017-02-06";
        assertEquals(USNYSE.dayOfWeek(dateString), DayOfWeek.MONDAY);
        assertEquals(JPOSE.dayOfWeek(dateString), DayOfWeek.MONDAY);

        DateTime dateTime = DateTimeUtils.convertDateTime("2017-09-01T00:00:00.000000000 NY");
        assertEquals(USNYSE.dayOfWeek(dateTime), DayOfWeek.FRIDAY);
        assertEquals(JPOSE.dayOfWeek(dateTime), DayOfWeek.FRIDAY);

        dateString = null;
        dateTime = null;
        assertNull(USNYSE.dayOfWeek(dateString));
        assertNull(USNYSE.dayOfWeek(dateTime));

        // incorrectly formatted days
        try {
            USNYSE.dayOfWeek("2018-09-31");
            fail();
        } catch (DateTimeException e) {
            // ok
        }
    }

    public void testLastBusinessDayOfWeek() {
        assertFalse(test.isLastBusinessDayOfWeek());

        String dateString = "2017-02-10";
        DateTime dateTime = DateTimeUtils.convertDateTime("2017-02-07T00:00:00.000000000 NY");
        assertTrue(USNYSE.isLastBusinessDayOfWeek(dateString));
        assertFalse(USNYSE.isLastBusinessDayOfWeek(dateTime));
        assertTrue(JPOSE.isLastBusinessDayOfWeek(dateString));
        assertFalse(JPOSE.isLastBusinessDayOfWeek(dateTime));

        dateString = null;
        assertFalse(USNYSE.isLastBusinessDayOfWeek(dateString));
    }

    public void testLastBusinessDayOfMonth() {
        assertFalse(test.isLastBusinessDayOfMonth());

        String dateString = "2017-02-28";
        DateTime dateTime = DateTimeUtils.convertDateTime("2017-02-07T00:00:00.000000000 NY");
        assertTrue(USNYSE.isLastBusinessDayOfMonth(dateString));
        assertFalse(USNYSE.isLastBusinessDayOfMonth(dateTime));
        assertTrue(JPOSE.isLastBusinessDayOfMonth(dateString));
        assertFalse(JPOSE.isLastBusinessDayOfMonth(dateTime));

        dateString = null;
        assertFalse(USNYSE.isLastBusinessDayOfMonth(dateString));
        assertFalse(JPOSE.isLastBusinessDayOfMonth(dateString));
    }

    public void testFractionOfBusinessDay() {
        assertEquals(1.0, test.fractionOfStandardBusinessDay());


        // half day, USNYSE market open from 0930 to 1300
        String dateString = "2018-11-23";

        // full day
        DateTime dateTime = DateTimeUtils.convertDateTime("2017-02-07T00:00:00.000000000 NY");

        assertEquals(USNYSE.fractionOfStandardBusinessDay(dateString), 3.5 / 6.5);
        assertEquals(1.0, USNYSE.fractionOfStandardBusinessDay(dateTime));

        // half day, JPOSE market open from 0930 to 1300
        dateString = "2006-01-04";

        assertEquals(JPOSE.fractionOfStandardBusinessDay(dateString), 0.5);
        assertEquals(1.0, JPOSE.fractionOfStandardBusinessDay(dateTime));


        dateString = null;
        dateTime = null;
        assertEquals(JPOSE.fractionOfStandardBusinessDay(dateString), 0.0);
        assertEquals(JPOSE.fractionOfStandardBusinessDay(dateTime), 0.0);
    }

    public void testFractionOfBusinessDayLeft() {
        // half day, market open from 0930 to 1300
        DateTime day1 = DateTimeUtils.convertDateTime("2018-11-23T10:00:00.000000000 NY");

        // full day
        DateTime day2 = DateTimeUtils.convertDateTime("2017-02-07T00:00:00.000000000 NY");

        // holiday
        DateTime day3 = DateTimeUtils.convertDateTime("2017-07-04T00:00:00.000000000 NY");

        assertEquals(USNYSE.fractionOfBusinessDayRemaining(day1), 3.0 / 3.5);
        assertEquals(USNYSE.fractionOfBusinessDayComplete(day1), 0.5 / 3.5, 0.0000001);
        assertEquals(USNYSE.fractionOfBusinessDayRemaining(day2), 1.0);
        assertEquals(USNYSE.fractionOfBusinessDayComplete(day2), 0.0);
        assertEquals(USNYSE.fractionOfBusinessDayRemaining(day3), 0.0);

        // half day, market open from 0900 to 1130
        day1 = DateTimeUtils.convertDateTime("2006-01-04T10:00:00.000000000 JP");
        day2 = DateTimeUtils.convertDateTime("2017-02-07T00:00:00.000000000 JP");
        assertEquals(JPOSE.fractionOfBusinessDayRemaining(day1), 1.5 / 2.5);
        assertEquals(JPOSE.fractionOfBusinessDayComplete(day1), 1.0 / 2.5);
        assertEquals(JPOSE.fractionOfBusinessDayRemaining(day2), 1.0);
        assertEquals(JPOSE.fractionOfBusinessDayComplete(day2), 0.0);


        assertEquals(JPOSE.fractionOfBusinessDayRemaining(null), QueryConstants.NULL_DOUBLE);
        assertEquals(JPOSE.fractionOfBusinessDayComplete(null), QueryConstants.NULL_DOUBLE);
    }

    public void testCurrentBusinessSchedule() {
        assertEquals(test.nextBusinessSchedule("2017-09-26"), test.currentBusinessSchedule());
    }

    public void testMidnightClose() {
        assertEquals(DateTimeUtils.DAY, UTC.standardBusinessDayLengthNanos());
        assertEquals("2019-04-16", UTC.nextDay("2019-04-15"));
        assertEquals("2019-04-16", UTC.nextBusinessDay("2019-04-15"));
        assertEquals("2019-04-18", UTC.nextBusinessDay("2019-04-15", 3));
        assertEquals("2019-08-19",
                UTC.nextBusinessDay(DateTimeUtils.convertDateTime("2019-08-18T00:00:00.000000000 UTC")));

        assertEquals("2019-05-16", UTC.getBusinessSchedule("2019-05-16").getSOBD().toDateString(TimeZone.TZ_UTC));
        assertEquals("2019-05-17", UTC.getBusinessSchedule("2019-05-16").getEOBD().toDateString(TimeZone.TZ_UTC));
    }
}
