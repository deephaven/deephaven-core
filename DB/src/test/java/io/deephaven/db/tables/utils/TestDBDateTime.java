/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.clock.TimeZones;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.util.calendar.Calendars;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.Date;

import static io.deephaven.db.tables.utils.DBTimeUtils.convertDateTime;

public class TestDBDateTime extends BaseArrayTestCase {

    public void testAll() throws Exception {
        DateTime jodaDateTime = new DateTime("2010-01-01T12:13:14.999");

        long nanos = jodaDateTime.getMillis() * 1000000 + 123456;

        DBDateTime dateTime = new DBDateTime(nanos);

        assertEquals(nanos, dateTime.getNanos());

        assertEquals(jodaDateTime.getMillis(), dateTime.getMillis());

        assertEquals(123456, dateTime.getNanosPartial());

        assertEquals(new Date(jodaDateTime.getMillis()), dateTime.getDate());

        assertEquals(jodaDateTime, dateTime.getJodaDateTime());

        assertEquals(DBTimeZone.TZ_NY.getTimeZone(), dateTime.getJodaDateTime(DBTimeZone.TZ_NY).getZone());

        assertTrue(new DBDateTime(123456).equals(new DBDateTime(123456)));

        assertEquals(-1, new DBDateTime(123456).compareTo(new DBDateTime(123457)));
        assertEquals(0, new DBDateTime(123456).compareTo(new DBDateTime(123456)));
        assertEquals(1, new DBDateTime(123456).compareTo(new DBDateTime(123455)));
    }

    public void testInstant() {
        DateTime jodaDateTime = new DateTime("2010-01-11T12:13:14.999");
        DBDateTime dateTime1 = new DBDateTime(jodaDateTime.getMillis() * 1000000);
        long nanos = jodaDateTime.getMillis() * 1000000 + 123456;
        DBDateTime dateTime2 = new DBDateTime(nanos);

        java.time.Instant target1 = java.time.Instant.ofEpochMilli(jodaDateTime.getMillis());
        assertEquals(target1, dateTime1.getInstant());

        java.time.Instant target2 = java.time.Instant.ofEpochSecond(jodaDateTime.getMillis() / 1000, 999123456);
        assertEquals(target2, dateTime2.getInstant());
    }

    private long getMillisFromDateStr(SimpleDateFormat format, String dateStr) {
        try {
            Date date = format.parse(dateStr);
            return date.getTime();
        } catch (ParseException e) {
            return 0;
        }
    }

    public void testLastBusinessDateNy() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setTimeZone(TimeZones.TZ_NEWYORK); // stick with one timezone to ensure the test works properly

        String today;
        String dayBefore;

        // Test that the overloaded methods match (this will break if we manage to straddle midnight while it's run!)
        assertEquals(DBTimeUtils.lastBusinessDateNy(), DBTimeUtils.lastBusinessDateNy(System.currentTimeMillis()));
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test Monday-Friday
        today = "2013-11-18";
        dayBefore = "2013-11-15";
        assertEquals(dayBefore, DBTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test end of month
        today = "2013-11-01";
        dayBefore = "2013-10-31";
        assertEquals(dayBefore, DBTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test end of year
        today = "2012-01-01";
        dayBefore = "2011-12-30";
        assertEquals(dayBefore, DBTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test a holiday (2013 thanksgivig)
        today = "2013-11-28";
        dayBefore = "2013-11-27";
        assertEquals(dayBefore, DBTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;
        today = "2013-11-29";
        dayBefore = "2013-11-27";
        assertEquals(dayBefore, DBTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Now test the current time
        // (Straight business calendar and the DBUtils codepath)
        String fromCal = Calendars.calendar().previousBusinessDay(DBTimeUtils.millisToTime(System.currentTimeMillis()));
        assertEquals(DBTimeUtils.lastBusinessDateNy(), fromCal);
        // Test it a second time, since its cached
        assertEquals(DBTimeUtils.lastBusinessDateNy(), fromCal);
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test cache rollover given times that advance a day
        today = "2013-11-26";
        dayBefore = "2013-11-25";
        assertEquals(dayBefore, DBTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        today = "2013-11-27";
        dayBefore = "2013-11-26";
        assertEquals(dayBefore, DBTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today) + 1)); // make sure
                                                                                                          // it advances
                                                                                                          // just past
                                                                                                          // midnight

        // Rolling back should not work -- we have cached a later day
        today = "2013-11-26";
        String expected = "2013-11-26";
        assertEquals(expected, DBTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Try the date time overrides
        String override = "2013-11-27";
        DBTimeUtils.lastBusinessDayNyOverride = override;
        assertEquals(DBTimeUtils.lastBusinessDateNy(), override);
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Now set the current date and see if the helper function workos
        DBTimeUtils.currentDateNyOverride = override;
        assertEquals(DBTimeUtils.currentDateNy(), override);
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        DBTimeUtils.overrideLastBusinessDateNyFromCurrentDateNy();
        assertEquals(DBTimeUtils.lastBusinessDateNy(), "2013-11-26");
        DBTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

    }

    public void testToDateString() {
        DBDateTime dateTime = convertDateTime("2016-11-06T04:00 UTC"); // 11/6 is the last day of DST

        { // America/New_York
            String zoneId = "America/New_York";
            assertEquals("2016-11-06", dateTime.toDateString(DBTimeZone.TZ_NY));
            assertEquals("2016-11-06", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            assertEquals("2016-11-06", dateTime.toDateString(zoneId));
            assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // EST - supported by joda; not java.time
            String zoneId = "EST";
            assertEquals("2016-11-05", dateTime.toDateString(DateTimeZone.forID(zoneId)));

            try {
                assertEquals("2016-11-05", dateTime.toDateString(zoneId));
                fail("Should have thrown an exception for invalid zone");
            } catch (ZoneRulesException ignored) {
            }
        }

        { // UTC
            String zoneId = "UTC";
            assertEquals("2016-11-06", dateTime.toDateString(DBTimeZone.TZ_UTC));
            assertEquals("2016-11-06", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            assertEquals("2016-11-06", dateTime.toDateString(zoneId));
            assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+2 - 2 hours *EAST*
            String zoneId = "Etc/GMT+2";
            assertEquals("2016-11-06", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            assertEquals("2016-11-06", dateTime.toDateString(zoneId));
            assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+4 -- 4 hours *WEST*
            String zoneId = "Etc/GMT+4";
            assertEquals("2016-11-06", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            assertEquals("2016-11-06", dateTime.toDateString(zoneId));
            assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+2 -- 5 hours *WEST*
            String zoneId = "Etc/GMT+5";
            assertEquals("2016-11-05", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            assertEquals("2016-11-05", dateTime.toDateString(zoneId));
            assertEquals("2016-11-05", dateTime.toDateString(ZoneId.of(zoneId)));
        }
    }
}
