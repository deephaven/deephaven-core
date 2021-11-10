/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.utils;

import io.deephaven.base.clock.TimeZones;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.util.calendar.Calendars;
import org.joda.time.DateTimeZone;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.Date;

import static io.deephaven.engine.tables.utils.DateTimeUtils.convertDateTime;

public class TestDateTime extends BaseArrayTestCase {

    public void testAll() throws Exception {
        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");

        long nanos = jodaDateTime.getMillis() * 1000000 + 123456;

        DateTime dateTime = new DateTime(nanos);

        assertEquals(nanos, dateTime.getNanos());

        assertEquals(jodaDateTime.getMillis(), dateTime.getMillis());

        assertEquals(123456, dateTime.getNanosPartial());

        assertEquals(new Date(jodaDateTime.getMillis()), dateTime.getDate());

        assertEquals(jodaDateTime, dateTime.getJodaDateTime());

        assertEquals(TimeZone.TZ_NY.getTimeZone(), dateTime.getJodaDateTime(TimeZone.TZ_NY).getZone());

        assertTrue(new DateTime(123456).equals(new DateTime(123456)));

        assertEquals(-1, new DateTime(123456).compareTo(new DateTime(123457)));
        assertEquals(0, new DateTime(123456).compareTo(new DateTime(123456)));
        assertEquals(1, new DateTime(123456).compareTo(new DateTime(123455)));
    }

    public void testInstant() {
        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-11T12:13:14.999");
        DateTime dateTime1 = new DateTime(jodaDateTime.getMillis() * 1000000);
        long nanos = jodaDateTime.getMillis() * 1000000 + 123456;
        DateTime dateTime2 = new DateTime(nanos);

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
        assertEquals(DateTimeUtils.lastBusinessDateNy(), DateTimeUtils.lastBusinessDateNy(System.currentTimeMillis()));
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test Monday-Friday
        today = "2013-11-18";
        dayBefore = "2013-11-15";
        assertEquals(dayBefore, DateTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test end of month
        today = "2013-11-01";
        dayBefore = "2013-10-31";
        assertEquals(dayBefore, DateTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test end of year
        today = "2012-01-01";
        dayBefore = "2011-12-30";
        assertEquals(dayBefore, DateTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test a holiday (2013 thanksgivig)
        today = "2013-11-28";
        dayBefore = "2013-11-27";
        assertEquals(dayBefore, DateTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;
        today = "2013-11-29";
        dayBefore = "2013-11-27";
        assertEquals(dayBefore, DateTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Now test the current time
        // (Straight business calendar and the DBUtils codepath)
        String fromCal =
                Calendars.calendar().previousBusinessDay(DateTimeUtils.millisToTime(System.currentTimeMillis()));
        assertEquals(DateTimeUtils.lastBusinessDateNy(), fromCal);
        // Test it a second time, since its cached
        assertEquals(DateTimeUtils.lastBusinessDateNy(), fromCal);
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Test cache rollover given times that advance a day
        today = "2013-11-26";
        dayBefore = "2013-11-25";
        assertEquals(dayBefore, DateTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        today = "2013-11-27";
        dayBefore = "2013-11-26";
        assertEquals(dayBefore, DateTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today) + 1)); // make sure
                                                                                                            // it
                                                                                                            // advances
                                                                                                            // just past
                                                                                                            // midnight

        // Rolling back should not work -- we have cached a later day
        today = "2013-11-26";
        String expected = "2013-11-26";
        assertEquals(expected, DateTimeUtils.lastBusinessDateNy(getMillisFromDateStr(format, today)));
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Try the date time overrides
        String override = "2013-11-27";
        DateTimeUtils.lastBusinessDayNyOverride = override;
        assertEquals(DateTimeUtils.lastBusinessDateNy(), override);
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        // Now set the current date and see if the helper function workos
        DateTimeUtils.currentDateNyOverride = override;
        assertEquals(DateTimeUtils.currentDateNy(), override);
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

        DateTimeUtils.overrideLastBusinessDateNyFromCurrentDateNy();
        assertEquals(DateTimeUtils.lastBusinessDateNy(), "2013-11-26");
        DateTimeUtils.endOfCurrentDateNyLastBusinessDay = 0;

    }

    public void testToDateString() {
        DateTime dateTime = convertDateTime("2016-11-06T04:00 UTC"); // 11/6 is the last day of DST

        { // America/New_York
            String zoneId = "America/New_York";
            assertEquals("2016-11-06", dateTime.toDateString(TimeZone.TZ_NY));
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
            assertEquals("2016-11-06", dateTime.toDateString(TimeZone.TZ_UTC));
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
