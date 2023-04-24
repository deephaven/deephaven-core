/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;
import org.joda.time.DateTimeZone;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.Date;

import static io.deephaven.time.DateTimeUtils.toDateTime;

public class TestDateTime extends BaseArrayTestCase {

    public void testAll() throws Exception {
        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");

        long nanos = jodaDateTime.getMillis() * 1000000 + 123456;

        DateTime dateTime = new DateTime(nanos);

        TestCase.assertEquals(nanos, dateTime.getNanos());

        TestCase.assertEquals(jodaDateTime.getMillis(), dateTime.getMillis());

        TestCase.assertEquals(123456, dateTime.getNanosPartial());

        TestCase.assertEquals(new Date(jodaDateTime.getMillis()), dateTime.getDate());

        TestCase.assertEquals(jodaDateTime, dateTime.getJodaDateTime());

        TestCase.assertEquals(TimeZone.TZ_NY.getTimeZone(), dateTime.getJodaDateTime(TimeZone.TZ_NY).getZone());

        TestCase.assertTrue(new DateTime(123456).equals(new DateTime(123456)));

        TestCase.assertEquals(-1, new DateTime(123456).compareTo(new DateTime(123457)));
        TestCase.assertEquals(0, new DateTime(123456).compareTo(new DateTime(123456)));
        TestCase.assertEquals(1, new DateTime(123456).compareTo(new DateTime(123455)));
    }

    public void testInstant() {
        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-11T12:13:14.999");
        DateTime dateTime1 = new DateTime(jodaDateTime.getMillis() * 1000000);
        long nanos = jodaDateTime.getMillis() * 1000000 + 123456;
        DateTime dateTime2 = new DateTime(nanos);

        java.time.Instant target1 = java.time.Instant.ofEpochMilli(jodaDateTime.getMillis());
        TestCase.assertEquals(target1, dateTime1.getInstant());

        java.time.Instant target2 = java.time.Instant.ofEpochSecond(jodaDateTime.getMillis() / 1000, 999123456);
        TestCase.assertEquals(target2, dateTime2.getInstant());
    }

    private long getMillisFromDateStr(SimpleDateFormat format, String dateStr) {
        try {
            Date date = format.parse(dateStr);
            return date.getTime();
        } catch (ParseException e) {
            return 0;
        }
    }

    public void testToDateString() {
        DateTime dateTime = toDateTime("2016-11-06T04:00 UTC"); // 11/6 is the last day of DST

        { // America/New_York
            String zoneId = "America/New_York";
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(TimeZone.TZ_NY));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(zoneId));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // EST - supported by joda; not java.time
            String zoneId = "EST";
            TestCase.assertEquals("2016-11-05", dateTime.toDateString(DateTimeZone.forID(zoneId)));

            try {
                TestCase.assertEquals("2016-11-05", dateTime.toDateString(zoneId));
                TestCase.fail("Should have thrown an exception for invalid zone");
            } catch (ZoneRulesException ignored) {
            }
        }

        { // UTC
            String zoneId = "UTC";
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(TimeZone.TZ_UTC));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(zoneId));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+2 - 2 hours *EAST*
            String zoneId = "Etc/GMT+2";
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(zoneId));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+4 -- 4 hours *WEST*
            String zoneId = "Etc/GMT+4";
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(zoneId));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+2 -- 5 hours *WEST*
            String zoneId = "Etc/GMT+5";
            TestCase.assertEquals("2016-11-05", dateTime.toDateString(DateTimeZone.forID(zoneId)));
            TestCase.assertEquals("2016-11-05", dateTime.toDateString(zoneId));
            TestCase.assertEquals("2016-11-05", dateTime.toDateString(ZoneId.of(zoneId)));
        }
    }
}
