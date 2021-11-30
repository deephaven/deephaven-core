/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.util.QueryConstants;

public class TestDateStringUtils extends BaseArrayTestCase {

    public void testParse() {
        // ok dates
        DateStringUtils.parseLocalDate("2018-09-05");
        DateStringUtils.parseLocalDate("2018-09-06");
        DateStringUtils.parseLocalDate("2018-09-30");
        DateStringUtils.parseLocalDate("2018-02-01");
        DateStringUtils.parseLocalDate("2018-02-28");

        // leap year
        DateStringUtils.parseLocalDate("2016-02-29");


        // bad dates
        // bad year format
        try {
            DateStringUtils.parseLocalDate("2018243-09-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }


        // bad month format
        try {
            DateStringUtils.parseLocalDate("2018-093-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
        // bad month number
        try {
            DateStringUtils.parseLocalDate("2018-14-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }


        // bad day format
        try {
            DateStringUtils.parseLocalDate("2018-09-5421");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }

        // bad day number
        try {
            DateStringUtils.parseLocalDate("2018-12-32");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }

        // bad day number which would be valid in other months
        try {
            DateStringUtils.parseLocalDate("2018-02-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testPlusDays() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertEquals(DateStringUtils.plusDays(day1, 1), day2);

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-03-01";
        assertEquals(DateStringUtils.plusDays(day1, 2), day2);

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertEquals(DateStringUtils.plusDays(day1, 1), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-12";
        day2 = "2017-03-17";
        assertEquals(DateStringUtils.plusDays(day1, 5), day2);

        assertNull(DateStringUtils.plusDays(null, 6));


        // bad date
        try {
            DateStringUtils.plusDays("2018-02-31", 7);
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testPlusDaysQuiet() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertEquals(DateStringUtils.plusDaysQuiet(day1, 1), day2);

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-03-01";
        assertEquals(DateStringUtils.plusDaysQuiet(day1, 2), day2);

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertEquals(DateStringUtils.plusDaysQuiet(day1, 1), day2);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-12";
        day2 = "2017-03-17";
        assertEquals(DateStringUtils.plusDaysQuiet(day1, 5), day2);

        assertNull(DateStringUtils.plusDaysQuiet(null, 6));
    }

    public void testMinusDays() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertEquals(DateStringUtils.minusDays(day2, 1), day1);

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-03-01";
        assertEquals(DateStringUtils.minusDays(day2, 2), day1);

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertEquals(DateStringUtils.minusDays(day2, 1), day1);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-11";
        day2 = "2017-03-17";
        assertEquals(DateStringUtils.minusDays(day2, 6), day1);

        assertNull(DateStringUtils.minusDays(null, 6));


        // bad date
        try {
            DateStringUtils.minusDays("2018-09-31", 7);
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testMinusDaysQuiet() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertEquals(DateStringUtils.minusDaysQuiet(day2, 1), day1);

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-03-01";
        assertEquals(DateStringUtils.minusDaysQuiet(day2, 2), day1);

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertEquals(DateStringUtils.minusDaysQuiet(day2, 1), day1);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-11";
        day2 = "2017-03-17";
        assertEquals(DateStringUtils.minusDaysQuiet(day2, 6), day1);

        assertNull(DateStringUtils.minusDaysQuiet(null, 6));
    }

    public void testIsAfter() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertTrue(DateStringUtils.isAfter(day2, day1));

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-03-01";
        assertTrue(DateStringUtils.isAfter(day2, day1));

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertTrue(DateStringUtils.isAfter(day2, day1));

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-11";
        day2 = "2017-03-17";
        assertTrue(DateStringUtils.isAfter(day2, day1));

        assertFalse(DateStringUtils.isAfter(day1, null));
        assertFalse(DateStringUtils.isAfter(null, null));

        // bad date
        try {
            DateStringUtils.isAfter("2018-12-31", "2018-12-32");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testIsAfterQuiet() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertTrue(DateStringUtils.isAfterQuiet(day2, day1));

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-03-01";
        assertTrue(DateStringUtils.isAfterQuiet(day2, day1));

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertTrue(DateStringUtils.isAfterQuiet(day2, day1));

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-11";
        day2 = "2017-03-17";
        assertTrue(DateStringUtils.isAfterQuiet(day2, day1));

        assertFalse(DateStringUtils.isAfterQuiet(day1, null));
        assertFalse(DateStringUtils.isAfterQuiet(null, null));
    }

    public void testIsBefore() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertTrue(DateStringUtils.isBefore(day1, day2));

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-03-01";
        assertTrue(DateStringUtils.isBefore(day1, day2));

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertTrue(DateStringUtils.isBefore(day1, day2));

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-11";
        day2 = "2017-03-17";
        assertTrue(DateStringUtils.isBefore(day1, day2));

        assertFalse(DateStringUtils.isBefore(day1, null));
        assertFalse(DateStringUtils.isBefore(null, null));

        // bad date
        try {
            DateStringUtils.isBefore("2018-02-31", "2018-02-19");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    public void testIsBeforeQuiet() {
        String day1 = "2016-08-31";
        String day2 = "2016-09-01";
        assertTrue(DateStringUtils.isBeforeQuiet(day1, day2));

        // leap day
        day1 = "2016-02-28";
        day2 = "2016-03-01";
        assertTrue(DateStringUtils.isBeforeQuiet(day1, day2));

        // new year
        day1 = "2013-12-31";
        day2 = "2014-01-01";
        assertTrue(DateStringUtils.isBeforeQuiet(day1, day2));

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-11";
        day2 = "2017-03-17";
        assertTrue(DateStringUtils.isBeforeQuiet(day1, day2));

        assertFalse(DateStringUtils.isBeforeQuiet(day1, null));
        assertFalse(DateStringUtils.isBeforeQuiet(null, null));
    }

    public void testMonthOfYear() {
        String day1 = "2016-08-31";
        assertEquals(DateStringUtils.monthOfYear(day1), 8);

        // leap day
        day1 = "2016-02-28";
        assertEquals(DateStringUtils.monthOfYear(day1), 2);

        // new year
        day1 = "2013-12-31";
        assertEquals(DateStringUtils.monthOfYear(day1), 12);

        // Daylight savings starts in NY (UTC-7:00) at 2 AM 2017-03-12
        day1 = "2017-03-12";
        assertEquals(DateStringUtils.monthOfYear(day1), 3);

        assertEquals(DateStringUtils.monthOfYear(null), QueryConstants.NULL_INT);

        // bad date
        try {
            DateStringUtils.monthOfYear("2018-02-31");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
    }
}
