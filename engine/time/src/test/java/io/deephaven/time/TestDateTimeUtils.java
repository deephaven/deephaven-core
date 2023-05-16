/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.CompareUtils;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.time.*;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;

@SuppressWarnings("deprecation")
public class TestDateTimeUtils extends BaseArrayTestCase {

    public void testFailJoda() {
        TestCase.fail("kill joda");
    }

    public void testParseDate() {
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("20100102", DateTimeUtils.DateStyle.YMD));

        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("2010-01-02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("01-02-2010", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("02-01-2010", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("2010/01/02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("01/02/2010", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("02/01/2010", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("10-01-02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("01-02-10", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("02-01-10", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("10/01/02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("01/02/10", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDate("02/01/10", DateTimeUtils.DateStyle.DMY));

        assertEquals(DateTimeUtils.parseDate("01/02/03"), DateTimeUtils.parseDate("01/02/03", DateTimeUtils.DateStyle.MDY));

        try{
            DateTimeUtils.parseDate("JUNK", DateTimeUtils.DateStyle.YMD);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex){
            //pass
        }

        try{
            //noinspection ConstantConditions
            DateTimeUtils.parseDate(null, DateTimeUtils.DateStyle.YMD);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex){
            //pass
        }

        try{
            DateTimeUtils.parseDate("JUNK", null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex){
            //pass
        }
    }

    public void testParseDateQuiet() {
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("20100102", DateTimeUtils.DateStyle.YMD));

        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("2010-01-02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("01-02-2010", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("02-01-2010", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("2010/01/02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("01/02/2010", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("02/01/2010", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("10-01-02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("01-02-10", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("02-01-10", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("10/01/02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("01/02/10", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010,1,2), DateTimeUtils.parseDateQuiet("02/01/10", DateTimeUtils.DateStyle.DMY));

        assertEquals(DateTimeUtils.parseDateQuiet("01/02/03"), DateTimeUtils.parseDateQuiet("01/02/03", DateTimeUtils.DateStyle.MDY));

        assertNull(DateTimeUtils.parseDateQuiet("JUNK", DateTimeUtils.DateStyle.YMD));
        assertNull(DateTimeUtils.parseDateQuiet(null, DateTimeUtils.DateStyle.YMD));
        assertNull(DateTimeUtils.parseDateQuiet("JUNK", null));
    }

    public void testParseLocalTime() {
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59),
                DateTimeUtils.parseLocalTime("L12:59:59"));
        TestCase.assertEquals(java.time.LocalTime.of(0, 0, 0),
                DateTimeUtils.parseLocalTime("L00:00:00"));
        TestCase.assertEquals(java.time.LocalTime.of(23, 59, 59),
                DateTimeUtils.parseLocalTime("L23:59:59"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59),
                DateTimeUtils.parseLocalTime("L125959"));
        TestCase.assertEquals(java.time.LocalTime.of(0, 0, 0),
                DateTimeUtils.parseLocalTime("L000000"));
        TestCase.assertEquals(java.time.LocalTime.of(23, 59, 59),
                DateTimeUtils.parseLocalTime("L235959"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 0, 0),
                DateTimeUtils.parseLocalTime("L12"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 0),
                DateTimeUtils.parseLocalTime("L12:59"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000),
                DateTimeUtils.parseLocalTime("L12:59:59.123"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000),
                DateTimeUtils.parseLocalTime("L12:59:59.123456"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789),
                DateTimeUtils.parseLocalTime("L12:59:59.123456789"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 0, 0),
                DateTimeUtils.parseLocalTime("L12"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 0),
                DateTimeUtils.parseLocalTime("L1259"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000),
                DateTimeUtils.parseLocalTime("L125959.123"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000),
                DateTimeUtils.parseLocalTime("L125959.123456"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789),
                DateTimeUtils.parseLocalTime("L125959.123456789"));

        try {
            DateTimeUtils.parseLocalTime("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseLocalTime(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }
    }

    public void testParseLocalTimeQuiet() {
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59),
                DateTimeUtils.parseLocalTimeQuiet("L12:59:59"));
        TestCase.assertEquals(java.time.LocalTime.of(0, 0, 0),
                DateTimeUtils.parseLocalTimeQuiet("L00:00:00"));
        TestCase.assertEquals(java.time.LocalTime.of(23, 59, 59),
                DateTimeUtils.parseLocalTimeQuiet("L23:59:59"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59),
                DateTimeUtils.parseLocalTimeQuiet("L125959"));
        TestCase.assertEquals(java.time.LocalTime.of(0, 0, 0),
                DateTimeUtils.parseLocalTimeQuiet("L000000"));
        TestCase.assertEquals(java.time.LocalTime.of(23, 59, 59),
                DateTimeUtils.parseLocalTimeQuiet("L235959"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 0, 0),
                DateTimeUtils.parseLocalTimeQuiet("L12"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 0),
                DateTimeUtils.parseLocalTimeQuiet("L12:59"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000),
                DateTimeUtils.parseLocalTimeQuiet("L12:59:59.123"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000),
                DateTimeUtils.parseLocalTimeQuiet("L12:59:59.123456"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789),
                DateTimeUtils.parseLocalTimeQuiet("L12:59:59.123456789"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 0, 0),
                DateTimeUtils.parseLocalTimeQuiet("L12"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 0),
                DateTimeUtils.parseLocalTimeQuiet("L1259"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000),
                DateTimeUtils.parseLocalTimeQuiet("L125959.123"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000),
                DateTimeUtils.parseLocalTimeQuiet("L125959.123456"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789),
                DateTimeUtils.parseLocalTimeQuiet("L125959.123456789"));

        TestCase.assertNull(DateTimeUtils.parseLocalTimeQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseLocalTimeQuiet(null));
    }

    public void testParseTimeZone() {
        for (TimeZone tz : TimeZone.values()) {
            TestCase.assertEquals(tz, DateTimeUtils.parseTimeZone(tz.toString().split("_")[1]));
        }

        try {
            DateTimeUtils.parseTimeZone("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseTimeZone(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }
    }

    public void testParseTimeZoneQuiet() {
        for (TimeZone tz : TimeZone.values()) {
            TestCase.assertEquals(tz, DateTimeUtils.parseTimeZoneQuiet(tz.toString().split("_")[1]));
        }

        TestCase.assertNull(DateTimeUtils.parseTimeZoneQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseTimeZoneQuiet(null));
    }

    public void testParseTimeZoneId() {
        for (TimeZone tz : TimeZone.values()) {
            TestCase.assertEquals(tz.getZoneId(), DateTimeUtils.parseTimeZoneId(tz.toString().split("_")[1]));
        }

        for(Map.Entry<String,String> e : ZoneId.SHORT_IDS.entrySet()) {
            TestCase.assertEquals(ZoneId.of(e.getKey(), ZoneId.SHORT_IDS), DateTimeUtils.parseTimeZoneId(e.getKey()));
            TestCase.assertEquals(ZoneId.of(e.getValue(), ZoneId.SHORT_IDS), DateTimeUtils.parseTimeZoneId(e.getValue()));
        }

        TestCase.assertEquals(ZoneId.of("America/Denver"), DateTimeUtils.parseTimeZoneId("America/Denver"));

        try {
            DateTimeUtils.parseTimeZoneId("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseTimeZoneId(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }
    }

    public void testParseTimeZoneIdQuiet() {
        for (TimeZone tz : TimeZone.values()) {
            TestCase.assertEquals(tz.getZoneId(), DateTimeUtils.parseTimeZoneIdQuiet(tz.toString().split("_")[1]));
        }

        for(Map.Entry<String,String> e : ZoneId.SHORT_IDS.entrySet()) {
            TestCase.assertEquals(ZoneId.of(e.getKey(), ZoneId.SHORT_IDS), DateTimeUtils.parseTimeZoneIdQuiet(e.getKey()));
            TestCase.assertEquals(ZoneId.of(e.getValue(), ZoneId.SHORT_IDS), DateTimeUtils.parseTimeZoneIdQuiet(e.getValue()));
        }

        TestCase.assertEquals(ZoneId.of("America/Denver"), DateTimeUtils.parseTimeZoneIdQuiet("America/Denver"));

        TestCase.assertNull(DateTimeUtils.parseTimeZoneIdQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseTimeZoneIdQuiet(null));
    }

    public void testParseDateTime() {
        final String[] tzs = {
                "NY",
                "JP",
                "GMT",
                "America/New_York",
                "America/Chicago",
        };

        final String[] roots = {
                "2010-01-01T12:11",
                "2010-01-01T12:00:02",
                "2010-01-01T12:00:00.1",
                "2010-01-01T12:00:00.123",
                "2010-01-01T12:00:00.123",
                "2010-01-01T12:00:00.123456789",
        };

        for(String tz : tzs) {
            for(String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZoneId(tz);
                final ZonedDateTime zdt = LocalDateTime.parse(root).atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", DateTime.of(zdt.toInstant()), DateTimeUtils.parseDateTime(s));
            }
        }

        try {
            DateTimeUtils.parseDateTime("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            DateTimeUtils.parseDateTime("2010-01-01T12:11");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            DateTimeUtils.parseDateTime("2010-01-01T12:11 JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseDateTime(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

    }

    public void testParseDateTimeQuiet() {
        final String[] tzs = {
                "NY",
                "JP",
                "GMT",
                "America/New_York",
                "America/Chicago",
        };

        final String[] roots = {
                "2010-01-01T12:11",
                "2010-01-01T12:00:02",
                "2010-01-01T12:00:00.1",
                "2010-01-01T12:00:00.123",
                "2010-01-01T12:00:00.123",
                "2010-01-01T12:00:00.123456789",
        };

        for(String tz : tzs) {
            for(String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZoneId(tz);
                final ZonedDateTime zdt = LocalDateTime.parse(root).atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", DateTime.of(zdt.toInstant()), DateTimeUtils.parseDateTimeQuiet(s));
            }
        }

        TestCase.assertNull(DateTimeUtils.parseDateTimeQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseDateTimeQuiet("2010-01-01T12:11"));
        TestCase.assertNull(DateTimeUtils.parseDateTimeQuiet("2010-01-01T12:11 JUNK"));
        TestCase.assertNull(DateTimeUtils.parseDateTimeQuiet(null));
    }

    public void testParseNanos() {
        final String[] times = {
                "12:00",
                "12:00:00",
                "12:00:00.123",
                "12:00:00.1234",
                "12:00:00.123456789",
                "2:00",
                "2:00:00",
                "2:00:00",
                "2:00:00.123",
                "2:00:00.1234",
                "2:00:00.123456789",
                "3T2:00",
                "3T2:00:00",
                "3T2:00:00.123",
                "3T2:00:00.123456789",
                "15:25:49.064106107",
        };

        for(boolean isNeg : new boolean[]{false, true}) {
            for (String t : times) {
                long offset = 0;
                String lts = t;

                if (t.contains("T")) {
                    lts = t.split("T")[1];
                    offset = Long.parseLong(t.split("T")[0]) * DateTimeUtils.DAY;
                }

                if (lts.indexOf(":") == 1) {
                    lts = "0" + lts;
                }

                if(isNeg) {
                    t = "-" + t;
                }

                final long sign = isNeg ? -1 : 1;
                TestCase.assertEquals(sign*(LocalTime.parse(lts).toNanoOfDay() + offset), DateTimeUtils.parseNanos(t));
            }
        }

        final String[] periods = {
                "T1h43s",
                "-T1h43s",
        };

            for (String p : periods) {
                final Period pp = DateTimeUtils.parsePeriod(p);
                TestCase.assertEquals(pp.isPositive() ? pp.getDuration().toNanos() : -pp.getDuration().toNanos(), DateTimeUtils.parseNanos(p));
            }

        try {
            DateTimeUtils.parseNanos("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseNanos(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

    }

    public void testParseNanosQuiet() {
        final String[] times = {
                "12:00",
                "12:00:00",
                "12:00:00.123",
                "12:00:00.1234",
                "12:00:00.123456789",
                "2:00",
                "2:00:00",
                "2:00:00",
                "2:00:00.123",
                "2:00:00.1234",
                "2:00:00.123456789",
                "3T2:00",
                "3T2:00:00",
                "3T2:00:00.123",
                "3T2:00:00.123456789",
                "15:25:49.064106107",
        };

        for(boolean isNeg : new boolean[]{false, true}) {
            for (String t : times) {
                long offset = 0;
                String lts = t;

                if (t.contains("T")) {
                    lts = t.split("T")[1];
                    offset = Long.parseLong(t.split("T")[0]) * DateTimeUtils.DAY;
                }

                if (lts.indexOf(":") == 1) {
                    lts = "0" + lts;
                }

                if(isNeg) {
                    t = "-" + t;
                }

                final long sign = isNeg ? -1 : 1;
                TestCase.assertEquals(sign*(LocalTime.parse(lts).toNanoOfDay() + offset), DateTimeUtils.parseNanosQuiet(t));
            }
        }

        final String[] periods = {
                "T1h43s",
                "-T1h43s",
        };

        for (String p : periods) {
            final Period pp = DateTimeUtils.parsePeriod(p);
            TestCase.assertEquals(pp.isPositive() ? pp.getDuration().toNanos() : -pp.getDuration().toNanos(), DateTimeUtils.parseNanosQuiet(p));
        }

        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseNanosQuiet("JUNK"));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseNanosQuiet(null));
    }

    //todo parse nanos quiet

    public void testParsePeriod() {
        final String[] periods = {
                "T1S",
                "T4H1S",
                "4DT1S",
//                "2W3DT4H2S",
                "2Y5DT3H6M7S",
                //TODO: Year, Month, Week, and partial seconds don't work
//                "2Y3M4W5DT3H6M7.655S",
//                "1WT1M",
//                "1W",
        };

        //TODO: our negative periods do not match the negative durations...  we don't prefix a P...
        TestCase.fail("Period format does not match duration");
        TestCase.fail("Duration format does not support Year, Month, and Week");
        TestCase.fail("Do not support Partial Seconds");
        TestCase.fail("Do not support negative in the same way that Duration does.");

        for(boolean isNeg : new boolean[]{false, true}) {
            for (String p : periods) {
                if(isNeg) {
                    p = "-" + p;
                }

                final Period pp = DateTimeUtils.parsePeriod(p);
                final Duration d = Duration.parse("P"+p);


                TestCase.assertEquals("Period: " + p, d, pp.getDuration());
                TestCase.assertEquals("Period: " + p, isNeg, !pp.isPositive());
            }
        }

        try {
            DateTimeUtils.parsePeriod(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }
    }

    public void testParsePeriodQuiet() {
        TestCase.fail("not implemented");
    }

    public void testParseTimePrecision() {
        TestCase.assertEquals(ChronoField.DAY_OF_MONTH, DateTimeUtils.parseTimePrecision("2021-02-03"));
        TestCase.assertEquals(ChronoField.HOUR_OF_DAY, DateTimeUtils.parseTimePrecision("2021-02-03T11"));
        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecision("2021-02-03T11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32.1234"));

        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecision("11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecision("11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.1234"));

        try{
            DateTimeUtils.parseTimePrecision("JUNK");
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex){
            //pass
        }

        try{
            //noinspection ConstantConditions
            DateTimeUtils.parseTimePrecision(null);
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex){
            //pass
        }
    }

    public void testParseTimePrecisionQuiet() {
        TestCase.assertEquals(ChronoField.DAY_OF_MONTH, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03"));
        TestCase.assertEquals(ChronoField.HOUR_OF_DAY, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11"));
        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32.1234"));

        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecisionQuiet("11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecisionQuiet("11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("11:14:32.1234"));

        TestCase.assertNull(DateTimeUtils.parseTimePrecisionQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseTimePrecisionQuiet(null));
    }

    public void testFormatDate() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2021-02-03T11:23:32.456789 NY");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TimeZone.TZ_NY);
        final ZonedDateTime dt4 = dt1.toZonedDateTime(TimeZone.TZ_JP);

        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt1, TimeZone.TZ_NY));
        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt1, TimeZone.TZ_NY.getZoneId()));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt1, TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt1, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt2, TimeZone.TZ_NY));
        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt2, TimeZone.TZ_NY.getZoneId()));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt2, TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt2, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt3));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt4));

        TestCase.assertNull(DateTimeUtils.formatDate((DateTime) null, TimeZone.TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDate((DateTime) null, TimeZone.TZ_NY.getZoneId()));
        TestCase.assertNull(DateTimeUtils.formatDate(dt1, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.formatDate(dt1, (ZoneId) null));

        TestCase.assertNull(DateTimeUtils.formatDate((Instant) null, TimeZone.TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDate((Instant) null, TimeZone.TZ_NY.getZoneId()));
        TestCase.assertNull(DateTimeUtils.formatDate(dt2, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.formatDate(dt2, (ZoneId) null));

        TestCase.assertNull(DateTimeUtils.formatDate(null));
    }

    public void testFormatDateTime() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2021-02-03T11:23:32.45678912 NY");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TimeZone.TZ_NY);
        final ZonedDateTime dt4 = dt1.toZonedDateTime(TimeZone.TZ_JP);

        TestCase.assertEquals("2021-02-04T01:00:00.000000000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:00 NY"), TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:00.000000000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23 NY"), TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:01.000000000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:01 NY"), TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:01.300000000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:01.3 NY"), TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456700000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.4567 NY"), TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456780000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.45678 NY"), TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.456789 NY"), TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789100 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.4567891 NY"), TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.45678912 NY"), TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789123 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.456789123 NY"), TimeZone.TZ_JP));

        TestCase.assertEquals("2021-02-03T11:23:32.456789120 NY", DateTimeUtils.formatDateTime(dt1, TimeZone.TZ_NY));
        TestCase.assertEquals("2021-02-03T11:23:32.456789120 America/New_York", DateTimeUtils.formatDateTime(dt1, TimeZone.TZ_NY.getZoneId()));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP", DateTimeUtils.formatDateTime(dt1, TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 Asia/Tokyo", DateTimeUtils.formatDateTime(dt1, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals("2021-02-03T11:23:32.456789120 NY", DateTimeUtils.formatDateTime(dt2, TimeZone.TZ_NY));
        TestCase.assertEquals("2021-02-03T11:23:32.456789120 America/New_York", DateTimeUtils.formatDateTime(dt2, TimeZone.TZ_NY.getZoneId()));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP", DateTimeUtils.formatDateTime(dt2, TimeZone.TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 Asia/Tokyo", DateTimeUtils.formatDateTime(dt2, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals("2021-02-03T11:23:32.456789120 America/New_York", DateTimeUtils.formatDateTime(dt3));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 Asia/Tokyo", DateTimeUtils.formatDateTime(dt4));

        TestCase.assertNull(DateTimeUtils.formatDateTime((DateTime) null, TimeZone.TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDateTime((DateTime) null, TimeZone.TZ_NY.getZoneId()));
        TestCase.assertNull(DateTimeUtils.formatDateTime(dt1, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.formatDateTime(dt1, (ZoneId) null));

        TestCase.assertNull(DateTimeUtils.formatDateTime((Instant) null, TimeZone.TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDateTime((Instant) null, TimeZone.TZ_NY.getZoneId()));
        TestCase.assertNull(DateTimeUtils.formatDateTime(dt2, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.formatDateTime(dt2, (ZoneId) null));

        TestCase.assertNull(DateTimeUtils.formatDateTime(null));

        TestCase.fail("Clean up time zone handling");
    }

    public void testFormatNanos() {
        TestCase.assertEquals("12:00:00",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("12:00")));
        TestCase.assertEquals("12:00:00",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("12:00:00")));
        TestCase.assertEquals("12:00:00.123000000",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("12:00:00.123")));
        TestCase.assertEquals("12:00:00.123400000",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("12:00:00.1234")));
        TestCase.assertEquals("12:00:00.123456789",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("12:00:00.123456789")));

        TestCase.assertEquals("2:00:00",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("2:00")));
        TestCase.assertEquals("2:00:00",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("2:00:00")));
        TestCase.assertEquals("2:00:00.123000000",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("2:00:00.123")));
        TestCase.assertEquals("2:00:00.123400000",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("2:00:00.1234")));
        TestCase.assertEquals("2:00:00.123456789",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("2:00:00.123456789")));

        TestCase.assertEquals("3T2:00:00",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("3T2:00")));
        TestCase.assertEquals("3T2:00:00",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("3T2:00:00")));
        TestCase.assertEquals("3T2:00:00.123000000",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("3T2:00:00.123")));
        TestCase.assertEquals("3T2:00:00.123400000",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("3T2:00:00.1234")));
        TestCase.assertEquals("3T2:00:00.123456789",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("3T2:00:00.123456789")));
        TestCase.assertEquals("-3T2:00:00.123456789",
                DateTimeUtils.formatNanos(DateTimeUtils.parseNanosQuiet("-3T2:00:00.123456789")));
    }

    public void testMicrosToMillis() {
        final long v = 1234567890;
        TestCase.assertEquals(v/1_000L, DateTimeUtils.microsToMillis(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsToMillis(NULL_LONG));
    }

    public void testMicrosToNanos() {
        final long v = 1234567890;
        TestCase.assertEquals(v*1_000L, DateTimeUtils.microsToNanos(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsToNanos(NULL_LONG));

        try{
            DateTimeUtils.millisToNanos(Long.MAX_VALUE/2);
            TestCase.fail("Should throw an exception");
        }catch (DateTimeUtils.DateTimeOverflowException ex){
            //pass
        }

        try{
            DateTimeUtils.microsToNanos(-Long.MAX_VALUE/2);
            TestCase.fail("Should throw an exception");
        }catch (DateTimeUtils.DateTimeOverflowException ex){
            //pass
        }
    }

    public void testMicrosToSeconds() {
        final long v = 1234567890;
        TestCase.assertEquals(v/1_000_000L, DateTimeUtils.microsToSeconds(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsToSeconds(NULL_LONG));
    }

    public void testMillisToMicros() {
        final long v = 1234567890;
        TestCase.assertEquals(v*1_000L, DateTimeUtils.millisToMicros(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.millisToMicros(NULL_LONG));
    }

    public void testMillisToNanos() {
        final long v = 1234567890;
        TestCase.assertEquals(v*1_000_000L, DateTimeUtils.millisToNanos(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.millisToNanos(NULL_LONG));

        try{
            DateTimeUtils.millisToNanos(Long.MAX_VALUE/2);
            TestCase.fail("Should throw an exception");
        }catch (DateTimeUtils.DateTimeOverflowException ex){
            //pass
        }

        try{
            DateTimeUtils.millisToNanos(-Long.MAX_VALUE/2);
            TestCase.fail("Should throw an exception");
        }catch (DateTimeUtils.DateTimeOverflowException ex){
            //pass
        }
    }

    public void testMillisToSeconds() {
        final long v = 1234567890;
        TestCase.assertEquals(v/1_000L, DateTimeUtils.millisToSeconds(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.millisToSeconds(NULL_LONG));
    }

    public void testNanosToMicros() {
        final long v = 1234567890;
        TestCase.assertEquals(v/1_000L, DateTimeUtils.nanosToMicros(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosToMicros(NULL_LONG));
    }

    public void testNanosToMillis() {
        final long v = 1234567890;
        TestCase.assertEquals(v/1_000_000L, DateTimeUtils.nanosToMillis(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosToMillis(NULL_LONG));
    }

    public void testNanosToSeconds() {
        final long v = 1234567890;
        TestCase.assertEquals(v/1_000_000_000L, DateTimeUtils.nanosToSeconds(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosToSeconds(NULL_LONG));
    }

    public void testSecondsToNanos() {
        final long v = 1234567890;
        TestCase.assertEquals(v*1_000_000_000L, DateTimeUtils.secondsToNanos(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.secondsToNanos(NULL_LONG));

        try{
            DateTimeUtils.secondsToNanos(Long.MAX_VALUE/2);
            TestCase.fail("Should throw an exception");
        }catch (DateTimeUtils.DateTimeOverflowException ex){
            //pass
        }

        try{
            DateTimeUtils.secondsToNanos(-Long.MAX_VALUE/2);
            TestCase.fail("Should throw an exception");
        }catch (DateTimeUtils.DateTimeOverflowException ex){
            //pass
        }
    }

    public void testSecondsToMicros() {
        final long v = 1234567890;
        TestCase.assertEquals(v*1_000_000L, DateTimeUtils.secondsToMicros(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.secondsToMicros(NULL_LONG));
    }

    public void testSecondsToMillis() {
        final long v = 1234567890;
        TestCase.assertEquals(v*1_000L, DateTimeUtils.secondsToMillis(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.secondsToMillis(NULL_LONG));
    }

    public void testToDate() {
        final long millis = 123456789;
        final DateTime dt1 = new DateTime(millis*DateTimeUtils.MILLI);
        final Instant dt2 = Instant.ofEpochSecond(0, millis*DateTimeUtils.MILLI);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());

        TestCase.assertEquals(new Date(millis), DateTimeUtils.toDate(dt1));
        TestCase.assertNull(DateTimeUtils.toDate((DateTime)null));

        TestCase.assertEquals(new Date(millis), DateTimeUtils.toDate(dt2));
        TestCase.assertNull(DateTimeUtils.toDate((Instant) null));

        TestCase.assertEquals(new Date(millis), DateTimeUtils.toDate(dt3));
        TestCase.assertNull(DateTimeUtils.toDate((ZonedDateTime) null));
    }

    public void testToDateTime() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());
        final LocalDate ld = LocalDate.of(1973,11,30);
        final LocalTime lt = LocalTime.of(6,33,9, 123456789);
        final Date d = new Date(DateTimeUtils.nanosToMillis(nanos));

        TestCase.assertEquals(dt1, DateTimeUtils.toDateTime(dt2));
        TestCase.assertNull(DateTimeUtils.toDateTime((Instant) null));

        TestCase.assertEquals(dt1, DateTimeUtils.toDateTime(dt3));
        TestCase.assertNull(DateTimeUtils.toDateTime((ZonedDateTime) null));

        TestCase.assertEquals(dt1, DateTimeUtils.toDateTime(ld, lt, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toDateTime(null, lt, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toDateTime(ld, null, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toDateTime(ld, lt, (TimeZone) null));

        TestCase.assertEquals(dt1, DateTimeUtils.toDateTime(ld, lt, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toDateTime(null, lt, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toDateTime(ld, null, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toDateTime(ld, lt, (ZoneId) null));

        TestCase.assertEquals(new DateTime((nanos/DateTimeUtils.MILLI)*DateTimeUtils.MILLI), DateTimeUtils.toDateTime(d));
        TestCase.assertNull(DateTimeUtils.toDateTime((Date) null));

        try{
            DateTimeUtils.toDateTime(Instant.ofEpochSecond(0, Long.MAX_VALUE-5));
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }

        try{
            DateTimeUtils.toDateTime(Instant.ofEpochSecond(0, Long.MAX_VALUE-5).atZone(TimeZone.TZ_JP.getZoneId()));
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }

    }

    public void testToInstant() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());
        final LocalDate ld = LocalDate.of(1973,11,30);
        final LocalTime lt = LocalTime.of(6,33,9, 123456789);
        final Date d = new Date(DateTimeUtils.nanosToMillis(nanos));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(dt1));
        TestCase.assertNull(DateTimeUtils.toInstant((DateTime) null));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(dt3));
        TestCase.assertNull(DateTimeUtils.toInstant((ZonedDateTime) null));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(ld, lt, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(null, lt, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(ld, null, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(ld, lt, (TimeZone) null));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(ld, lt, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toInstant(null, lt, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toInstant(ld, null, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toInstant(ld, lt, (ZoneId) null));

        TestCase.assertEquals(Instant.ofEpochSecond(0,(nanos/DateTimeUtils.MILLI)*DateTimeUtils.MILLI), DateTimeUtils.toInstant(d));
        TestCase.assertNull(DateTimeUtils.toInstant((Date) null));
    }

    public void testToLocalDate() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());
        final LocalDate ld = LocalDate.of(1973,11,30);

        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt1, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalDate((DateTime) null, TimeZone.TZ_JP));
        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt1, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toLocalDate((DateTime) null, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt2, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalDate((Instant) null, TimeZone.TZ_JP));
        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt2, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toLocalDate((Instant) null, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt3));
        //noinspection ConstantConditions
        TestCase.assertNull(DateTimeUtils.toLocalDate((ZonedDateTime) null));

        TestCase.assertEquals(DateTimeUtils.toLocalDate(dt1, TimeZone.TZ_DEFAULT), DateTimeUtils.toLocalDate(dt1));
        TestCase.assertNull(DateTimeUtils.toLocalDate((DateTime) null));
        TestCase.assertEquals(DateTimeUtils.toLocalDate(dt2, TimeZone.TZ_DEFAULT), DateTimeUtils.toLocalDate(dt2));
        TestCase.assertNull(DateTimeUtils.toLocalDate((Instant) null));
    }

    public void testToLocalTime() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());
        final LocalTime lt = LocalTime.of(6,33,9, 123456789);

        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt1, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalTime((DateTime) null, TimeZone.TZ_JP));
        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt1, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toLocalTime((DateTime) null, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt2, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalTime((Instant) null, TimeZone.TZ_JP));
        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt2, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toLocalTime((Instant) null, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt3));
        //noinspection ConstantConditions
        TestCase.assertNull(DateTimeUtils.toLocalTime((ZonedDateTime) null));

        TestCase.assertEquals(DateTimeUtils.toLocalTime(dt1, TimeZone.TZ_DEFAULT), DateTimeUtils.toLocalTime(dt1));
        TestCase.assertNull(DateTimeUtils.toLocalTime((DateTime) null));
        TestCase.assertEquals(DateTimeUtils.toLocalTime(dt2, TimeZone.TZ_DEFAULT), DateTimeUtils.toLocalTime(dt2));
        TestCase.assertNull(DateTimeUtils.toLocalTime((Instant) null));
    }

    public void testToZonedDateTime() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());
        final LocalDate ld = LocalDate.of(1973,11,30);
        final LocalTime lt = LocalTime.of(6,33,9, 123456789);

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(dt1, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((DateTime) null, TimeZone.TZ_JP));
        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(dt1, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((DateTime) null, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(dt2, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((Instant) null, TimeZone.TZ_JP));
        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(dt2, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((Instant) null, TimeZone.TZ_JP.getZoneId()));

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(ld, lt, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(null, lt, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ld, null, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ld, lt, (TimeZone) null));

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(ld, lt, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(null, lt, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ld, null, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ld, lt, (ZoneId) null));

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1, TimeZone.TZ_DEFAULT), DateTimeUtils.toZonedDateTime(dt1));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((DateTime) null));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt2, TimeZone.TZ_DEFAULT), DateTimeUtils.toZonedDateTime(dt2));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((Instant) null));

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(ld, lt, TimeZone.TZ_DEFAULT), DateTimeUtils.toZonedDateTime(ld, lt));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(null, lt));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ld, null));
    }

    public void testEpochNanos() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());

        TestCase.assertEquals(nanos, DateTimeUtils.epochNanos(dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochNanos((DateTime) null));

        TestCase.assertEquals(nanos, DateTimeUtils.epochNanos(dt2));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochNanos((Instant) null));

        TestCase.assertEquals(nanos, DateTimeUtils.epochNanos(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochNanos((ZonedDateTime) null));
    }

    public void testEpochMicros() {
        final long nanos = 123456789123456789L;
        final long micros = DateTimeUtils.nanosToMicros(nanos);
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());

        TestCase.assertEquals(micros, DateTimeUtils.epochMicros(dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMicros((DateTime) null));

        TestCase.assertEquals(micros, DateTimeUtils.epochMicros(dt2));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMicros((Instant) null));

        TestCase.assertEquals(micros, DateTimeUtils.epochMicros(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMicros((ZonedDateTime) null));
    }

    public void testEpochMillis() {
        final long nanos = 123456789123456789L;
        final long millis = DateTimeUtils.nanosToMillis(nanos);
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());

        TestCase.assertEquals(millis, DateTimeUtils.epochMillis(dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMillis((DateTime) null));

        TestCase.assertEquals(millis, DateTimeUtils.epochMillis(dt2));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMillis((Instant) null));

        TestCase.assertEquals(millis, DateTimeUtils.epochMillis(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMillis((ZonedDateTime) null));
    }

    public void testEpochSeconds() {
        final long nanos = 123456789123456789L;
        final long seconds = DateTimeUtils.nanosToSeconds(nanos);
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());

        TestCase.assertEquals(seconds, DateTimeUtils.epochSeconds(dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochSeconds((DateTime) null));

        TestCase.assertEquals(seconds, DateTimeUtils.epochSeconds(dt2));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochSeconds((Instant) null));

        TestCase.assertEquals(seconds, DateTimeUtils.epochSeconds(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochSeconds((ZonedDateTime) null));
    }

    public void testEpochNanosTo() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());

        TestCase.assertEquals(dt1, DateTimeUtils.epochNanosToDateTime(nanos));
        TestCase.assertNull(DateTimeUtils.epochNanosToDateTime(NULL_LONG));

        TestCase.assertEquals(dt2, DateTimeUtils.epochNanosToInstant(nanos));
        TestCase.assertNull(DateTimeUtils.epochNanosToInstant(NULL_LONG));

        TestCase.assertEquals(dt3, DateTimeUtils.epochNanosToZonedDateTime(nanos, TimeZone.TZ_JP));
        TestCase.assertEquals(dt3, DateTimeUtils.epochNanosToZonedDateTime(nanos, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochNanosToZonedDateTime(NULL_LONG, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochNanosToZonedDateTime(NULL_LONG, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochNanosToZonedDateTime(nanos, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.epochNanosToZonedDateTime(nanos, (ZoneId) null));

        TestCase.assertEquals(DateTimeUtils.epochNanosToZonedDateTime(nanos, TimeZone.TZ_DEFAULT), DateTimeUtils.epochNanosToZonedDateTime(nanos));
    }

    public void testEpochMicrosTo() {
        long nanos = 123456789123456789L;
        final long micros = DateTimeUtils.nanosToMicros(nanos);
        nanos = DateTimeUtils.microsToNanos(micros);
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());

        TestCase.assertEquals(dt1, DateTimeUtils.epochMicrosToDateTime(micros));
        TestCase.assertNull(DateTimeUtils.epochMicrosToDateTime(NULL_LONG));

        TestCase.assertEquals(dt2, DateTimeUtils.epochMicrosToInstant(micros));
        TestCase.assertNull(DateTimeUtils.epochMicrosToInstant(NULL_LONG));

        TestCase.assertEquals(dt3, DateTimeUtils.epochMicrosToZonedDateTime(micros, TimeZone.TZ_JP));
        TestCase.assertEquals(dt3, DateTimeUtils.epochMicrosToZonedDateTime(micros, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochMicrosToZonedDateTime(NULL_LONG, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochMicrosToZonedDateTime(NULL_LONG, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochMicrosToZonedDateTime(micros, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.epochMicrosToZonedDateTime(micros, (ZoneId) null));

        TestCase.assertEquals(DateTimeUtils.epochMicrosToZonedDateTime(micros, TimeZone.TZ_DEFAULT), DateTimeUtils.epochMicrosToZonedDateTime(micros));
    }

    public void testEpochMillisTo() {
        long nanos = 123456789123456789L;
        final long millis = DateTimeUtils.nanosToMillis(nanos);
        nanos = DateTimeUtils.millisToNanos(millis);
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());

        TestCase.assertEquals(dt1, DateTimeUtils.epochMillisToDateTime(millis));
        TestCase.assertNull(DateTimeUtils.epochMillisToDateTime(NULL_LONG));

        TestCase.assertEquals(dt2, DateTimeUtils.epochMillisToInstant(millis));
        TestCase.assertNull(DateTimeUtils.epochMillisToInstant(NULL_LONG));

        TestCase.assertEquals(dt3, DateTimeUtils.epochMillisToZonedDateTime(millis, TimeZone.TZ_JP));
        TestCase.assertEquals(dt3, DateTimeUtils.epochMillisToZonedDateTime(millis, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochMillisToZonedDateTime(NULL_LONG, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochMillisToZonedDateTime(NULL_LONG, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochMillisToZonedDateTime(millis, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.epochMillisToZonedDateTime(millis, (ZoneId) null));

        TestCase.assertEquals(DateTimeUtils.epochMillisToZonedDateTime(millis, TimeZone.TZ_DEFAULT), DateTimeUtils.epochMillisToZonedDateTime(millis));
    }

    public void testEpochSecondsTo() {
        long nanos = 123456789123456789L;
        final long seconds = DateTimeUtils.nanosToSeconds(nanos);
        nanos = DateTimeUtils.secondsToNanos(seconds);
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TimeZone.TZ_JP.getZoneId());

        TestCase.assertEquals(dt1, DateTimeUtils.epochSecondsToDateTime(seconds));
        TestCase.assertNull(DateTimeUtils.epochSecondsToDateTime(NULL_LONG));

        TestCase.assertEquals(dt2, DateTimeUtils.epochSecondsToInstant(seconds));
        TestCase.assertNull(DateTimeUtils.epochSecondsToInstant(NULL_LONG));

        TestCase.assertEquals(dt3, DateTimeUtils.epochSecondsToZonedDateTime(seconds, TimeZone.TZ_JP));
        TestCase.assertEquals(dt3, DateTimeUtils.epochSecondsToZonedDateTime(seconds, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochSecondsToZonedDateTime(NULL_LONG, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochSecondsToZonedDateTime(NULL_LONG, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochSecondsToZonedDateTime(seconds, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.epochSecondsToZonedDateTime(seconds, (ZoneId) null));

        TestCase.assertEquals(DateTimeUtils.epochSecondsToZonedDateTime(seconds, TimeZone.TZ_DEFAULT), DateTimeUtils.epochSecondsToZonedDateTime(seconds));
    }

    public void testEpochAutoTo() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-02T12:13:14.1345 NY");
        final long nanos = DateTimeUtils.epochNanos(dt1);
        final long micros = DateTimeUtils.epochMicros(dt1);
        final long millis = DateTimeUtils.epochMillis(dt1);
        final long seconds = DateTimeUtils.epochSeconds(dt1);
        final DateTime dt1u = DateTimeUtils.epochMicrosToDateTime(micros);
        final DateTime dt1m = DateTimeUtils.epochMillisToDateTime(millis);
        final DateTime dt1s = DateTimeUtils.epochSecondsToDateTime(seconds);

        TestCase.assertEquals(nanos, DateTimeUtils.epochAutoToEpochNanos(nanos));
        TestCase.assertEquals((nanos/DateTimeUtils.MICRO)*DateTimeUtils.MICRO, DateTimeUtils.epochAutoToEpochNanos(micros));
        TestCase.assertEquals((nanos/DateTimeUtils.MILLI)*DateTimeUtils.MILLI, DateTimeUtils.epochAutoToEpochNanos(millis));
        TestCase.assertEquals((nanos/DateTimeUtils.SECOND)*DateTimeUtils.SECOND, DateTimeUtils.epochAutoToEpochNanos(seconds));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochAutoToEpochNanos(NULL_LONG));

        TestCase.assertEquals(dt1, DateTimeUtils.epochAutoToDateTime(nanos));
        TestCase.assertEquals(dt1u, DateTimeUtils.epochAutoToDateTime(micros));
        TestCase.assertEquals(dt1m, DateTimeUtils.epochAutoToDateTime(millis));
        TestCase.assertEquals(dt1s, DateTimeUtils.epochAutoToDateTime(seconds));
        TestCase.assertNull(DateTimeUtils.epochAutoToDateTime(NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.toInstant(dt1), DateTimeUtils.epochAutoToInstant(nanos));
        TestCase.assertEquals(DateTimeUtils.toInstant(dt1u), DateTimeUtils.epochAutoToInstant(micros));
        TestCase.assertEquals(DateTimeUtils.toInstant(dt1m), DateTimeUtils.epochAutoToInstant(millis));
        TestCase.assertEquals(DateTimeUtils.toInstant(dt1s), DateTimeUtils.epochAutoToInstant(seconds));
        TestCase.assertNull(DateTimeUtils.epochAutoToInstant(NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1, TimeZone.TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(nanos, TimeZone.TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1, TimeZone.TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(nanos, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1u, TimeZone.TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(micros, TimeZone.TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1u, TimeZone.TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(micros, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1m, TimeZone.TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(millis, TimeZone.TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1m, TimeZone.TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(millis, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1s, TimeZone.TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(seconds, TimeZone.TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1s, TimeZone.TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(seconds, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochAutoToZonedDateTime(NULL_LONG, TimeZone.TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochAutoToZonedDateTime(NULL_LONG, TimeZone.TZ_JP.getZoneId()));
        TestCase.assertNull(DateTimeUtils.epochAutoToZonedDateTime(nanos, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.epochAutoToZonedDateTime(nanos, (ZoneId) null));

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1, TimeZone.TZ_DEFAULT), DateTimeUtils.epochAutoToZonedDateTime(nanos));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1u, TimeZone.TZ_DEFAULT), DateTimeUtils.epochAutoToZonedDateTime(micros));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1m, TimeZone.TZ_DEFAULT), DateTimeUtils.epochAutoToZonedDateTime(millis));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1s, TimeZone.TZ_DEFAULT), DateTimeUtils.epochAutoToZonedDateTime(seconds));
        TestCase.assertNull(DateTimeUtils.epochAutoToZonedDateTime(NULL_LONG));
    }

    public void testToExcelTime() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2010-06-15T16:00:00 NY");
        final Instant dt2 = DateTimeUtils.toInstant(dt1);
        final ZonedDateTime dt3 = DateTimeUtils.toZonedDateTime(dt1, TimeZone.TZ_AL);

        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.666666666664, DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_NY)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.666666666664, DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_NY.getZoneId())));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_CT)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_CT.getZoneId())));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_MN)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_MN.getZoneId())));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime((DateTime) null, TimeZone.TZ_MN));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime((DateTime) null, TimeZone.TZ_MN.getZoneId()));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(dt1, (TimeZone) null));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(dt1, (ZoneId) null));

        TestCase.assertEquals(DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_DEFAULT), DateTimeUtils.toExcelTime(dt1));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime((DateTime) null));

        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.666666666664, DateTimeUtils.toExcelTime(dt2, TimeZone.TZ_NY)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.666666666664, DateTimeUtils.toExcelTime(dt2, TimeZone.TZ_NY.getZoneId())));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt2, TimeZone.TZ_CT)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt2, TimeZone.TZ_CT.getZoneId())));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt2, TimeZone.TZ_MN)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt2, TimeZone.TZ_MN.getZoneId())));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime((Instant) null, TimeZone.TZ_MN));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime((Instant) null, TimeZone.TZ_MN.getZoneId()));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(dt2, (TimeZone) null));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(dt2, (ZoneId) null));

        TestCase.assertEquals(DateTimeUtils.toExcelTime(dt2, TimeZone.TZ_DEFAULT), DateTimeUtils.toExcelTime(dt2));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime((Instant) null));

        TestCase.assertTrue(
                CompareUtils.doubleEquals(DateTimeUtils.toExcelTime(dt2, TimeZone.TZ_AL), DateTimeUtils.toExcelTime(dt3)));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime((ZonedDateTime) null));
    }

    public void testExcelTimeTo() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2010-06-15T16:23:45.678 NY");
        final Instant dt2 = DateTimeUtils.toInstant(dt1);
        final ZonedDateTime dt3 = DateTimeUtils.toZonedDateTime(dt1, TimeZone.TZ_AL);

        TestCase.assertEquals(dt1, DateTimeUtils.excelToDateTime(DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_AL), TimeZone.TZ_AL));
        TestCase.assertEquals(dt1, DateTimeUtils.excelToDateTime(DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_AL), TimeZone.TZ_AL.getZoneId()));
        TestCase.assertTrue(DateTimeUtils.epochMillis(dt1) - DateTimeUtils.epochMillis(DateTimeUtils.excelToDateTime(DateTimeUtils.toExcelTime(dt1))) <= 1);
        TestCase.assertNull(DateTimeUtils.excelToDateTime(123.4, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.excelToDateTime(123.4, (ZoneId) null));

        TestCase.assertEquals(dt2, DateTimeUtils.excelToInstant(DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_AL), TimeZone.TZ_AL));
        TestCase.assertEquals(dt2, DateTimeUtils.excelToInstant(DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_AL), TimeZone.TZ_AL.getZoneId()));
        TestCase.assertTrue(DateTimeUtils.epochMillis(dt2) - DateTimeUtils.epochMillis(DateTimeUtils.excelToInstant(DateTimeUtils.toExcelTime(dt2))) <= 1);
        TestCase.assertNull(DateTimeUtils.excelToInstant(123.4, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.excelToInstant(123.4, (ZoneId) null));

        TestCase.assertEquals(dt3, DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_AL), TimeZone.TZ_AL));
        TestCase.assertEquals(dt3, DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dt1, TimeZone.TZ_AL), TimeZone.TZ_AL.getZoneId()));
        TestCase.assertTrue(DateTimeUtils.epochMillis(dt3) - DateTimeUtils.epochMillis(DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dt1))) <= 1);
        TestCase.assertNull(DateTimeUtils.excelToZonedDateTime(123.4, (TimeZone) null));
        TestCase.assertNull(DateTimeUtils.excelToZonedDateTime(123.4, (ZoneId) null));
    }

    public void testIsBefore() {
        final DateTime dt1 = new DateTime(123);
        final DateTime dt2 = new DateTime(456);
        final DateTime dt3 = new DateTime(456);

        TestCase.assertTrue(DateTimeUtils.isBefore(dt1, dt2));
        TestCase.assertFalse(DateTimeUtils.isBefore(dt2, dt1));
        TestCase.assertFalse(DateTimeUtils.isBefore(dt2, dt3));
        TestCase.assertFalse(DateTimeUtils.isBefore(dt3, dt2));
        TestCase.assertFalse(DateTimeUtils.isBefore(null, dt2));
        TestCase.assertFalse(DateTimeUtils.isBefore((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isBefore(dt1, null));

        final Instant i1 = DateTimeUtils.toInstant(dt1);
        final Instant i2 = DateTimeUtils.toInstant(dt2);
        final Instant i3 = DateTimeUtils.toInstant(dt3);

        TestCase.assertTrue(DateTimeUtils.isBefore(i1, i2));
        TestCase.assertFalse(DateTimeUtils.isBefore(i2, i1));
        TestCase.assertFalse(DateTimeUtils.isBefore(i2, i3));
        TestCase.assertFalse(DateTimeUtils.isBefore(i3, i2));
        //noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isBefore(null, i2));
        TestCase.assertFalse(DateTimeUtils.isBefore((DateTime) null, null));
        //noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isBefore(i1, null));

        final ZonedDateTime z1 = DateTimeUtils.toZonedDateTime(dt1);
        final ZonedDateTime z2 = DateTimeUtils.toZonedDateTime(dt2);
        final ZonedDateTime z3 = DateTimeUtils.toZonedDateTime(dt3);

        TestCase.assertTrue(DateTimeUtils.isBefore(z1, z2));
        TestCase.assertFalse(DateTimeUtils.isBefore(z2, z1));
        TestCase.assertFalse(DateTimeUtils.isBefore(z2, z3));
        TestCase.assertFalse(DateTimeUtils.isBefore(z3, z2));
        TestCase.assertFalse(DateTimeUtils.isBefore(null, z2));
        TestCase.assertFalse(DateTimeUtils.isBefore((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isBefore(z1, null));
    }

    public void testIsBeforeOrEqual() {
        final DateTime dt1 = new DateTime(123);
        final DateTime dt2 = new DateTime(456);
        final DateTime dt3 = new DateTime(456);

        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(dt1, dt2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(dt2, dt1));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(dt2, dt3));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(dt3, dt2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(null, dt2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(dt1, null));

        final Instant i1 = DateTimeUtils.toInstant(dt1);
        final Instant i2 = DateTimeUtils.toInstant(dt2);
        final Instant i3 = DateTimeUtils.toInstant(dt3);

        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(i1, i2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(i2, i1));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(i2, i3));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(i3, i2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(null, i2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(i1, null));

        final ZonedDateTime z1 = DateTimeUtils.toZonedDateTime(dt1);
        final ZonedDateTime z2 = DateTimeUtils.toZonedDateTime(dt2);
        final ZonedDateTime z3 = DateTimeUtils.toZonedDateTime(dt3);

        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(z1, z2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(z2, z1));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(z2, z3));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(z3, z2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(null, z2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(z1, null));
    }

    public void testIsAfter() {
        final DateTime dt1 = new DateTime(123);
        final DateTime dt2 = new DateTime(456);
        final DateTime dt3 = new DateTime(456);

        TestCase.assertFalse(DateTimeUtils.isAfter(dt1, dt2));
        TestCase.assertTrue(DateTimeUtils.isAfter(dt2, dt1));
        TestCase.assertFalse(DateTimeUtils.isAfter(dt2, dt3));
        TestCase.assertFalse(DateTimeUtils.isAfter(dt3, dt2));
        TestCase.assertFalse(DateTimeUtils.isAfter(null, dt2));
        TestCase.assertFalse(DateTimeUtils.isAfter((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isAfter(dt1, null));

        final Instant i1 = DateTimeUtils.toInstant(dt1);
        final Instant i2 = DateTimeUtils.toInstant(dt2);
        final Instant i3 = DateTimeUtils.toInstant(dt3);

        TestCase.assertFalse(DateTimeUtils.isAfter(i1, i2));
        TestCase.assertTrue(DateTimeUtils.isAfter(i2, i1));
        TestCase.assertFalse(DateTimeUtils.isAfter(i2, i3));
        TestCase.assertFalse(DateTimeUtils.isAfter(i3, i2));
        //noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isAfter(null, i2));
        TestCase.assertFalse(DateTimeUtils.isAfter((DateTime) null, null));
        //noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isAfter(i1, null));

        final ZonedDateTime z1 = DateTimeUtils.toZonedDateTime(dt1);
        final ZonedDateTime z2 = DateTimeUtils.toZonedDateTime(dt2);
        final ZonedDateTime z3 = DateTimeUtils.toZonedDateTime(dt3);

        TestCase.assertFalse(DateTimeUtils.isAfter(z1, z2));
        TestCase.assertTrue(DateTimeUtils.isAfter(z2, z1));
        TestCase.assertFalse(DateTimeUtils.isAfter(z2, z3));
        TestCase.assertFalse(DateTimeUtils.isAfter(z3, z2));
        TestCase.assertFalse(DateTimeUtils.isAfter(null, z2));
        TestCase.assertFalse(DateTimeUtils.isAfter((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isAfter(z1, null));
    }

    public void testIsAfterOrEqual() {
        final DateTime dt1 = new DateTime(123);
        final DateTime dt2 = new DateTime(456);
        final DateTime dt3 = new DateTime(456);

        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(dt1, dt2));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(dt2, dt1));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(dt2, dt3));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(dt3, dt2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(null, dt2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(dt1, null));

        final Instant i1 = DateTimeUtils.toInstant(dt1);
        final Instant i2 = DateTimeUtils.toInstant(dt2);
        final Instant i3 = DateTimeUtils.toInstant(dt3);

        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(i1, i2));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(i2, i1));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(i2, i3));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(i3, i2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(null, i2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(i1, null));

        final ZonedDateTime z1 = DateTimeUtils.toZonedDateTime(dt1);
        final ZonedDateTime z2 = DateTimeUtils.toZonedDateTime(dt2);
        final ZonedDateTime z3 = DateTimeUtils.toZonedDateTime(dt3);

        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(z1, z2));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(z2, z1));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(z2, z3));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(z3, z2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(null, z2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual((DateTime) null, null));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(z1, null));
    }

    public void testClock() {
        final long nanos = 123456789123456789L;
        final @NotNull Clock initial = DateTimeUtils.currentClock();

        try {
            final io.deephaven.base.clock.Clock clock = new io.deephaven.base.clock.Clock() {

                @Override
                public long currentTimeMillis() {
                    return nanos / DateTimeUtils.MILLI;
                }

                @Override
                public long currentTimeMicros() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public long currentTimeNanos() {
                    return nanos;
                }

                @Override
                public Instant instantNanos() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Instant instantMillis() {
                    throw new UnsupportedOperationException();
                }
            };
            DateTimeUtils.setClock(clock);
            TestCase.assertEquals(clock, DateTimeUtils.currentClock());

            TestCase.assertEquals(new DateTime(nanos), DateTimeUtils.now());
            TestCase.assertEquals(new DateTime((nanos/DateTimeUtils.MILLI)*DateTimeUtils.MILLI), DateTimeUtils.nowMillisResolution());

            TestCase.assertTrue(Math.abs(DateTime.now().getNanos() - DateTimeUtils.nowSystem().getNanos()) < 1_000_000L);
            TestCase.assertTrue(Math.abs(DateTime.nowMillis().getNanos() - DateTimeUtils.nowSystemMillisResolution().getNanos()) < 1_000_000L);

            TestCase.assertEquals(DateTimeUtils.formatDate(new DateTime(nanos), TimeZone.TZ_AL), DateTimeUtils.today(TimeZone.TZ_AL));
            TestCase.assertEquals(DateTimeUtils.formatDate(new DateTime(nanos), TimeZone.TZ_AL), DateTimeUtils.today(TimeZone.TZ_AL.getZoneId()));
            TestCase.assertEquals(DateTimeUtils.today(TimeZone.TZ_DEFAULT), DateTimeUtils.today());
        } catch (Exception ex) {
            DateTimeUtils.setClock(initial);
            throw ex;
        }

        DateTimeUtils.setClock(initial);
    }


//    public void testMillis() throws Exception {
//        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
//
//        DateTime dateTime = new DateTime(jodaDateTime.getMillis() * 1000000 + 123456);
//
//        TestCase.assertEquals(jodaDateTime.getMillis(), DateTimeUtils.epochMillis(dateTime));
//
//        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_LONG, DateTimeUtils.epochMillis((DateTime)null));
//        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_LONG, DateTimeUtils.epochMillis((Instant)null));
//        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_LONG, DateTimeUtils.epochMillis((ZonedDateTime) null));
//    }
//
//    public void testNanos() throws Exception {
//        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
//
//        DateTime dateTime = new DateTime(jodaDateTime.getMillis() * 1000000 + 123456);
//
//        TestCase.assertEquals(jodaDateTime.getMillis() * 1000000 + 123456, DateTimeUtils.epochNanos(dateTime));
//
//        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_LONG, DateTimeUtils.epochNanos((DateTime) null));
//    }
//
//    public void testMidnightConversion() throws Exception {
//        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
//        org.joda.time.DateTime jodaMidnight = new org.joda.time.DateTime("2010-01-01T00:00:00.000-05");
//
//        DateTime dateTime = new DateTime(jodaDateTime.getMillis() * 1000000 + 123456);
//        DateTime midnight = DateTimeUtils.dateTimeAtMidnight(dateTime, TimeZone.TZ_NY);
//
//        TestCase.assertEquals(jodaMidnight.getMillis(), DateTimeUtils.epochMillis(midnight));
//    }
//

//    public void testPlus() throws Exception {
//        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
//
//        DateTime dateTime = new DateTime(jodaDateTime.getMillis() * 1000000 + 123456);
//
//        Period period = new Period("T1h");
//
//        TestCase.assertEquals(dateTime.getNanos() + 3600000000000L, DateTimeUtils.plus(dateTime, period).getNanos());
//
//        period = new Period("-T1h");
//
//        TestCase.assertEquals(dateTime.getNanos() - 3600000000000L, DateTimeUtils.plus(dateTime, period).getNanos());
//
//
//        // overflow plus
//        DateTimeUtils.plus(new DateTime(Long.MAX_VALUE - 10), 10); // edge at max
//        try {
//            DateTimeUtils.plus(new DateTime(Long.MAX_VALUE), 1);
//            TestCase.fail("This should have overflowed");
//        } catch (DateTimeUtils.DateTimeOverflowException e) {
//            // ok
//        }
//
//        DateTimeUtils.plus(new DateTime(Long.MIN_VALUE + 10), -10); // edge at min
//        try {
//            DateTimeUtils.plus(new DateTime(Long.MIN_VALUE), -1);
//            TestCase.fail("This should have overflowed");
//        } catch (DateTimeUtils.DateTimeOverflowException e) {
//            // ok
//        }
//    }
//
//    public void testMinus() throws Exception {
//        org.joda.time.DateTime jodaDateTime1 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
//        org.joda.time.DateTime jodaDateTime2 = new org.joda.time.DateTime("2010-01-01T13:13:14.999");
//
//        DateTime dateTime1 = new DateTime(jodaDateTime1.getMillis() * 1000000 + 123456);
//        DateTime dateTime2 = new DateTime(jodaDateTime2.getMillis() * 1000000 + 123456);
//
//        TestCase.assertEquals(-3600000000000L, DateTimeUtils.minus(dateTime1, dateTime2));
//        TestCase.assertEquals(3600000000000L, DateTimeUtils.minus(dateTime2, dateTime1));
//
//        Period period = new Period("T1h");
//
//        TestCase.assertEquals(dateTime1.getNanos() - 3600000000000L, DateTimeUtils.minus(dateTime1, period).getNanos());
//
//        period = new Period("-T1h");
//
//        TestCase.assertEquals(dateTime1.getNanos() + 3600000000000L, DateTimeUtils.minus(dateTime1, period).getNanos());
//
//
//
//        // overflow minus
//        DateTimeUtils.minus(new DateTime(Long.MAX_VALUE - 10), -10); // edge at max
//        try {
//            DateTimeUtils.minus(new DateTime(Long.MAX_VALUE), -1);
//            TestCase.fail("This should have overflowed");
//        } catch (DateTimeUtils.DateTimeOverflowException e) {
//            // ok
//        }
//
//        DateTimeUtils.minus(new DateTime(Long.MIN_VALUE + 10), 10); // edge at min
//        try {
//            DateTimeUtils.minus(new DateTime(Long.MIN_VALUE), 1);
//            TestCase.fail("This should have overflowed");
//        } catch (DateTimeUtils.DateTimeOverflowException e) {
//            // ok
//        }
//    }
//
//    public void testDiff() throws Exception {
//        org.joda.time.DateTime jodaDateTime1 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
//        org.joda.time.DateTime jodaDateTime2 = new org.joda.time.DateTime("2010-01-01T13:13:14.999");
//
//        DateTime dateTime1 = new DateTime(jodaDateTime1.getMillis() * 1000000 + 123456);
//        DateTime dateTime2 = new DateTime(jodaDateTime2.getMillis() * 1000000 + 123456);
//
//        TestCase.assertEquals(3600000000000L, DateTimeUtils.diffNanos(dateTime1, dateTime2));
//        TestCase.assertEquals(-3600000000000L, DateTimeUtils.diffNanos(dateTime2, dateTime1));
//
//        TestCase.assertEquals(3600000000000L, DateTimeUtils.diffNanos(dateTime1, dateTime2));
//        TestCase.assertEquals(-3600000000000L, DateTimeUtils.diffNanos(dateTime2, dateTime1));
//    }
//
//    public void testYearDiff() throws Exception {
//        org.joda.time.DateTime jt1 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
//        org.joda.time.DateTime jt2 = new org.joda.time.DateTime("2011-01-01T13:13:14.999");
//        org.joda.time.DateTime jt3 = new org.joda.time.DateTime("2010-06-30T13:13:14.999");
//
//        DateTime t1 = new DateTime(jt1.getMillis() * 1000000 + 123456);
//        DateTime t2 = new DateTime(jt2.getMillis() * 1000000 + 123456);
//        DateTime t3 = new DateTime(jt3.getMillis() * 1000000 + 123456);
//
//
//        TestCase.assertEquals(1.0, DateTimeUtils.diffYears(t1, t2), 0.01);
//        TestCase.assertEquals(0.5, DateTimeUtils.diffYears(t1, t3), 0.01);
//        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DateTimeUtils.diffYears(null, t1));
//        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DateTimeUtils.diffYears(t1, null));
//
//        TestCase.assertEquals(1.0, DateTimeUtils.diffYears(t1, t2), 0.01);
//        TestCase.assertEquals(0.5, DateTimeUtils.diffYears(t1, t3), 0.01);
//        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DateTimeUtils.diffYears(null, t1));
//        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DateTimeUtils.diffYears(t1, null));
//    }
//
//    public void testMillisToNanos() throws Exception {
//        TestCase.assertEquals(1000000, DateTimeUtils.millisToNanos(1));
//
//        // The next two tests will fail if DateTimeUtils.ENABLE_MICROTIME_HACK is true
//        try {
//            DateTimeUtils.millisToNanos(Long.MAX_VALUE / 1_000_000 + 1);
//            TestCase.fail("Should have thrown a DateTimeUtils.DateTimeOverflowException");
//        } catch (DateTimeUtils.DateTimeOverflowException ignored) {
//            /* Exception is expected. */
//        }
//
//        try {
//            DateTimeUtils.millisToNanos(-Long.MAX_VALUE / 1_000_000 - 1);
//            TestCase.fail("Should have thrown a DateTimeUtils.DateTimeOverflowException");
//        } catch (DateTimeUtils.DateTimeOverflowException ignored) {
//            /* Exception is expected. */
//        }
//    }
//
//    /*
//     * public void testMillisToNanosWithHack() throws Exception { // For this to pass, ENABLE_MICROTIME_HACK in
//     * DateTimeUtils must be true (i.e. you have // to run the tests with -DDateTimeUtil.enableMicrotimeHack=true)
//     * assertEquals(1_000_000, DateTimeUtils.millisToNanos(1)); assertEquals(1_000_000_000,
//     * DateTimeUtils.millisToNanos(1_000)); assertEquals(1531315655_000_000_000L,
//     * DateTimeUtils.millisToNanos(1531315655_000L)); assertEquals(1531315655_000_000_000L,
//     * DateTimeUtils.millisToNanos(1531315655_000_000L)); }
//     */
//
//    public void testNanosToMillis() throws Exception {
//        TestCase.assertEquals(1, DateTimeUtils.nanosToMillis(1000000));
//    }
//
//    public void testMicroToNanos() throws Exception {
//        TestCase.assertEquals(1000, DateTimeUtils.microsToNanos(1));
//
//        try {
//            DateTimeUtils.microsToNanos(Long.MAX_VALUE / 1_000 + 1);
//            TestCase.fail("Should have thrown a DateTimeUtils.DateTimeOverflowException");
//        } catch (DateTimeUtils.DateTimeOverflowException ignored) {
//            /* Exception is expected. */
//        }
//
//        try {
//            DateTimeUtils.microsToNanos(-Long.MAX_VALUE / 1_000 - 1);
//            TestCase.fail("Should have thrown a DateTimeUtils.DateTimeOverflowException");
//        } catch (DateTimeUtils.DateTimeOverflowException ignored) {
//            /* Exception is expected. */
//        }
//    }
//
//    public void testNanosToMicros() throws Exception {
//        TestCase.assertEquals(1, DateTimeUtils.nanosToMicros(1000));
//    }
//
//    public void testConvertDateQuiet() throws Exception {
//        // ISO formats
//        TestCase.assertEquals(LocalDate.of(2018, 1, 1), DateTimeUtils.parseDateQuiet("2018-01-01"));
//        TestCase.assertEquals(LocalDate.of(2018, 12, 31), DateTimeUtils.parseDateQuiet("2018-12-31"));
//        TestCase.assertEquals(LocalDate.of(2018, 1, 1), DateTimeUtils.parseDateQuiet("20180101"));
//        TestCase.assertEquals(LocalDate.of(2018, 12, 31), DateTimeUtils.parseDateQuiet("20181231"));
//
//        // extremities of the format (LocalDate can store a much larger range than this but we aren't that interested)
//        TestCase.assertEquals(LocalDate.of(0, 1, 1), DateTimeUtils.parseDateQuiet("0000-01-01"));
//        TestCase.assertEquals(LocalDate.of(9999, 12, 31), DateTimeUtils.parseDateQuiet("9999-12-31"));
//
//        // other variants
//        TestCase.assertEquals(LocalDate.of(2018, 1, 1),
//                DateTimeUtils.parseDateQuiet("01/01/2018", DateTimeUtils.DateStyle.MDY));
//        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
//                DateTimeUtils.parseDateQuiet("12/31/2018", DateTimeUtils.DateStyle.MDY));
//        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
//                DateTimeUtils.parseDateQuiet("12/31/18", DateTimeUtils.DateStyle.MDY));
//        TestCase.assertEquals(LocalDate.of(2024, 6, 25),
//                DateTimeUtils.parseDateQuiet("6/25/24", DateTimeUtils.DateStyle.MDY));
//        TestCase.assertEquals(LocalDate.of(2024, 6, 2),
//                DateTimeUtils.parseDateQuiet("6/2/24", DateTimeUtils.DateStyle.MDY));
//        TestCase.assertEquals(LocalDate.of(2024, 6, 2),
//                DateTimeUtils.parseDateQuiet("6/2/2024", DateTimeUtils.DateStyle.MDY));
//
//        TestCase.assertEquals(LocalDate.of(2018, 1, 1),
//                DateTimeUtils.parseDateQuiet("01/01/2018", DateTimeUtils.DateStyle.DMY));
//        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
//                DateTimeUtils.parseDateQuiet("31/12/2018", DateTimeUtils.DateStyle.DMY));
//        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
//                DateTimeUtils.parseDateQuiet("31/12/18", DateTimeUtils.DateStyle.DMY));
//        TestCase.assertEquals(LocalDate.of(2024, 6, 25),
//                DateTimeUtils.parseDateQuiet("25/6/24", DateTimeUtils.DateStyle.DMY));
//        TestCase.assertEquals(LocalDate.of(2024, 6, 2),
//                DateTimeUtils.parseDateQuiet("2/6/24", DateTimeUtils.DateStyle.DMY));
//        TestCase.assertEquals(LocalDate.of(2024, 6, 2),
//                DateTimeUtils.parseDateQuiet("2/6/2024", DateTimeUtils.DateStyle.DMY));
//
//
//        TestCase.assertEquals(LocalDate.of(2018, 1, 1),
//                DateTimeUtils.parseDateQuiet("2018/01/01", DateTimeUtils.DateStyle.YMD));
//        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
//                DateTimeUtils.parseDateQuiet("2018/12/31", DateTimeUtils.DateStyle.YMD));
//        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
//                DateTimeUtils.parseDateQuiet("18/12/31", DateTimeUtils.DateStyle.YMD));
//        TestCase.assertEquals(LocalDate.of(2024, 6, 25),
//                DateTimeUtils.parseDateQuiet("24/6/25", DateTimeUtils.DateStyle.YMD));
//        TestCase.assertEquals(LocalDate.of(2024, 6, 2),
//                DateTimeUtils.parseDateQuiet("24/6/2", DateTimeUtils.DateStyle.YMD));
//        TestCase.assertEquals(LocalDate.of(2024, 6, 2),
//                DateTimeUtils.parseDateQuiet("2024/6/2", DateTimeUtils.DateStyle.YMD));
//    }
//

//    public void testConvertDate() throws Exception {
//        DateTimeUtils.parseDate("2010-01-01"); // shouldn't have an exception
//
//        try {
//            DateTimeUtils.parseDate("2010-01-01 NY");
//            TestCase.fail("Should have thrown an exception");
//        } catch (Exception e) {
//        }
//
//        TestCase.assertEquals("DateTimeUtils.convertDate(\"9999-12-31\")",
//                LocalDate.of(9999, 12, 31),
//                DateTimeUtils.parseDate("9999-12-31"));
//    }
//



//
//    public void testFormatDate() throws Exception {
//        TestCase.assertEquals("2010-01-01",
//                DateTimeUtils.formatDate(DateTimeUtils.parseDateTimeQuiet("2010-01-01 NY"), TimeZone.TZ_NY));
//    }
//
//    public void testLowerBin() {
//        final long second = 1000000000L;
//        final long minute = 60 * second;
//        final long hour = 60 * minute;
//        DateTime time = DateTimeUtils.parseDateTime("2010-06-15T06:14:01.2345 NY");
//
//        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:14:01 NY"),
//                DateTimeUtils.lowerBin(time, second));
//        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:10:00 NY"),
//                DateTimeUtils.lowerBin(time, 5 * minute));
//        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:00:00 NY"),
//                DateTimeUtils.lowerBin(time, hour));
//        TestCase.assertEquals(null, DateTimeUtils.lowerBin((DateTime) null, 5 * minute));
//        TestCase.assertEquals(null, DateTimeUtils.lowerBin((Instant) null, 5 * minute));
//        TestCase.assertEquals(null, DateTimeUtils.lowerBin((ZonedDateTime) null, 5 * minute));
//        TestCase.assertEquals(null, DateTimeUtils.lowerBin(time, io.deephaven.util.QueryConstants.NULL_LONG));
//
//        TestCase.assertEquals(DateTimeUtils.lowerBin(time, second),
//                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(time, second), second));
//    }
//
//    public void testLowerBinWithOffset() {
//        final long second = 1000000000L;
//        final long minute = 60 * second;
//        final long hour = 60 * minute;
//        DateTime time = DateTimeUtils.parseDateTime("2010-06-15T06:14:01.2345 NY");
//
//        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:11:00 NY"),
//                DateTimeUtils.lowerBin(time, 5 * minute, minute));
//        TestCase.assertEquals(null, DateTimeUtils.lowerBin((DateTime) null, 5 * minute, minute));
//        TestCase.assertEquals(null, DateTimeUtils.lowerBin((Instant) null, 5 * minute, minute));
//        TestCase.assertEquals(null, DateTimeUtils.lowerBin((ZonedDateTime) null, 5 * minute, minute));
//        TestCase.assertEquals(null, DateTimeUtils.lowerBin(time, QueryConstants.NULL_LONG, minute));
//        TestCase.assertEquals(null, DateTimeUtils.lowerBin(time, 5 * minute, QueryConstants.NULL_LONG));
//
//        TestCase.assertEquals(DateTimeUtils.lowerBin(time, second, second),
//                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(time, second, second), second, second));
//    }
//
//    public void testUpperBin() {
//        final long second = 1000000000L;
//        final long minute = 60 * second;
//        final long hour = 60 * minute;
//        DateTime time = DateTimeUtils.parseDateTime("2010-06-15T06:14:01.2345 NY");
//
//        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:14:02 NY"),
//                DateTimeUtils.upperBin(time, second));
//        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:15:00 NY"),
//                DateTimeUtils.upperBin(time, 5 * minute));
//        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T07:00:00 NY"),
//                DateTimeUtils.upperBin(time, hour));
//        TestCase.assertEquals(null, DateTimeUtils.upperBin((DateTime) null, 5 * minute));
//        TestCase.assertEquals(null, DateTimeUtils.upperBin((Instant) null, 5 * minute));
//        TestCase.assertEquals(null, DateTimeUtils.upperBin((ZonedDateTime) null, 5 * minute));
//        TestCase.assertEquals(null, DateTimeUtils.upperBin(time, io.deephaven.util.QueryConstants.NULL_LONG));
//
//        TestCase.assertEquals(DateTimeUtils.upperBin(time, second),
//                DateTimeUtils.upperBin(DateTimeUtils.upperBin(time, second), second));
//    }
//
//    public void testUpperBinWithOffset() {
//        final long second = 1000000000L;
//        final long minute = 60 * second;
//        final long hour = 60 * minute;
//        DateTime time = DateTimeUtils.parseDateTime("2010-06-15T06:14:01.2345 NY");
//
//        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:16:00 NY"),
//                DateTimeUtils.upperBin(time, 5 * minute, minute));
//        TestCase.assertEquals(null, DateTimeUtils.upperBin((DateTime) null, 5 * minute, minute));
//        TestCase.assertEquals(null, DateTimeUtils.upperBin((Instant) null, 5 * minute, minute));
//        TestCase.assertEquals(null, DateTimeUtils.upperBin((ZonedDateTime) null, 5 * minute, minute));
//        TestCase.assertEquals(null, DateTimeUtils.upperBin(time, io.deephaven.util.QueryConstants.NULL_LONG, minute));
//        TestCase.assertEquals(null, DateTimeUtils.upperBin(time, 5 * minute, QueryConstants.NULL_LONG));
//
//        TestCase.assertEquals(DateTimeUtils.upperBin(time, second, second),
//                DateTimeUtils.upperBin(DateTimeUtils.upperBin(time, second, second), second, second));
//    }
//
//
//    /**
//     * Test autoEpcohTime with the given epoch time.
//     *
//     * @param epoch Epoch time (in seconds)
//     * @return The year (in the New York timezone) in which the given time falls.
//     */
//    public int doTestAutoEpochToTime(long epoch) {
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(epoch).getMillis(), epoch * 1000);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(epoch).getMicros(), epoch * 1000 * 1000);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(epoch).getNanos(), epoch * 1000 * 1000 * 1000);
//
//        final long milliValue = epoch * 1000 + (int) (Math.signum(epoch) * 123);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(milliValue).getMillis(), milliValue);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(milliValue).getMicros(), milliValue * 1000);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(milliValue).getNanos(), milliValue * 1000 * 1000);
//
//        final long microValue = milliValue * 1000 + (int) (Math.signum(milliValue) * 456);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(microValue).getMillis(), milliValue);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(microValue).getMicros(), microValue);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(microValue).getNanos(), microValue * 1000);
//
//        final long nanoValue = microValue * 1000 + (int) (Math.signum(microValue) * 789);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(nanoValue).getMillis(), milliValue);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(nanoValue).getMicros(), microValue);
//        TestCase.assertEquals(DateTimeUtils.epochAutoToDateTime(nanoValue).getNanos(), nanoValue);
//
//        return DateTimeUtils.year(DateTimeUtils.epochAutoToDateTime(nanoValue), TimeZone.TZ_NY);
//    }
//
//    public void testAutoEpochToTime() {
//        long inTheYear2035 = 2057338800;
//        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear2035)", 2035, doTestAutoEpochToTime(inTheYear2035));
//        long inTheYear1993 = 731966400;
//        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear1993)", 1993, doTestAutoEpochToTime(inTheYear1993));
//        long inTheYear2013 = 1363114800;
//        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear2013)", 2013, doTestAutoEpochToTime(inTheYear2013));
//
//        long inTheYear1904 = -2057338800;
//        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear1904)", 1904, doTestAutoEpochToTime(inTheYear1904));
//        long inTheYear1946 = -731966400;
//        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear1946)", 1946, doTestAutoEpochToTime(inTheYear1946));
//        long inTheYear1926 = -1363114800;
//        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear1926)", 1926, doTestAutoEpochToTime(inTheYear1926));
//    }
//
//    public void testMicrosOfMilli() {
//        TestCase.assertEquals(0,
//                DateTimeUtils.microsOfMilli(DateTimeUtils.parseDateTime("2015-07-31T20:40 NY")));
//        TestCase.assertEquals(0,
//                DateTimeUtils.microsOfMilli(DateTimeUtils.parseDateTime("2015-07-31T20:40:00 NY")));
//        TestCase.assertEquals(0,
//                DateTimeUtils.microsOfMilli(DateTimeUtils.parseDateTime("2015-07-31T20:40:00.123 NY")
//                ));
//        TestCase.assertEquals(400,
//                DateTimeUtils.microsOfMilli(DateTimeUtils.parseDateTime("2015-07-31T20:40:00.1234 NY")
//                ));
//        TestCase.assertEquals(456,
//                DateTimeUtils.microsOfMilli(DateTimeUtils.parseDateTime("2015-07-31T20:40:00.123456 NY")
//                ));
//        // this one should round up
//        TestCase.assertEquals(457,
//                DateTimeUtils.microsOfMilli(DateTimeUtils.parseDateTime("2015-07-31T20:40:00.1234567 NY")
//                ));
//        // this one should round up
//        TestCase.assertEquals(457,
//                DateTimeUtils.microsOfMilli(DateTimeUtils.parseDateTime("2015-07-31T20:40:00.123456789 NY")
//                ));
//    }
//
//    public void testZonedDateTime() {
//        final DateTime dateTime1 = DateTimeUtils.parseDateTime("2015-07-31T20:40 NY");
//        final ZonedDateTime zonedDateTime1 =
//                ZonedDateTime.of(2015, 7, 31, 20, 40, 0, 0, TimeZone.TZ_NY.getZoneId());
//        TestCase.assertEquals(zonedDateTime1, DateTimeUtils.toZonedDateTime(dateTime1, TimeZone.TZ_NY));
//        TestCase.assertEquals(dateTime1, DateTimeUtils.toDateTime(zonedDateTime1));
//
//        final DateTime dateTime2 = DateTimeUtils.parseDateTime("2020-07-31T20:40 NY");
//        TestCase.assertEquals(dateTime2,
//                DateTimeUtils.toDateTime(DateTimeUtils.toZonedDateTime(dateTime2, TimeZone.TZ_NY)));
//
//        final DateTime dateTime3 = DateTimeUtils.parseDateTime("2050-07-31T20:40 NY");
//        TestCase.assertEquals(dateTime3,
//                DateTimeUtils.toDateTime(DateTimeUtils.toZonedDateTime(dateTime3, TimeZone.TZ_NY)));
//    }
//
//    public void testISO8601() {
//        final String iso8601 = "2022-04-26T00:30:31.087360Z";
//        assertEquals(DateTime.of(Instant.parse(iso8601)), DateTimeUtils.parseDateTime(iso8601));
//    }
//
//
//    public void testISO8601_druation() {
//        final long dayNanos = 1_000_000_000L * 60 * 60 * 24;
//
//        assertEquals(7 * dayNanos, DateTimeUtils.parseNanos("1W"));
//        assertEquals(-7 * dayNanos, DateTimeUtils.parseNanos("-1W"));
//    }
}
