/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.time;

import io.deephaven.base.CompareUtils;
import io.deephaven.base.clock.TimeZones;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.util.DateUtil;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;

import java.time.LocalDate;
import java.time.ZonedDateTime;

public class TestDateTimeUtil extends BaseArrayTestCase {

    public void testMillis() throws Exception {
        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");

        DateTime dateTime = new DateTime(jodaDateTime.getMillis() * 1000000 + 123456);

        TestCase.assertEquals(jodaDateTime.getMillis(), DateTimeUtil.millis(dateTime));

        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_LONG, DateTimeUtil.millis(null));
    }

    public void testNanos() throws Exception {
        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");

        DateTime dateTime = new DateTime(jodaDateTime.getMillis() * 1000000 + 123456);

        TestCase.assertEquals(jodaDateTime.getMillis() * 1000000 + 123456, DateTimeUtil.nanos(dateTime));

        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_LONG, DateTimeUtil.nanos((DateTime) null));
    }

    public void testMidnightConversion() throws Exception {
        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
        org.joda.time.DateTime jodaMidnight = new org.joda.time.DateTime("2010-01-01T00:00:00.000-05");

        DateTime dateTime = new DateTime(jodaDateTime.getMillis() * 1000000 + 123456);
        DateTime midnight = DateTimeUtil.dateAtMidnight(dateTime, TimeZone.TZ_NY);

        TestCase.assertEquals(jodaMidnight.getMillis(), DateTimeUtil.millis(midnight));
        TestCase.assertEquals(jodaMidnight.getMillis(),
                DateTimeUtil.millisToDateAtMidnightNy(dateTime.getMillis()).getMillis());

        TestCase.assertNull(DateTimeUtil.millisToDateAtMidnightNy(io.deephaven.util.QueryConstants.NULL_LONG));
    }

    public void testIsBefore() throws Exception {
        org.joda.time.DateTime jodaDateTime1 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
        org.joda.time.DateTime jodaDateTime2 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");

        DateTime dateTime1 = new DateTime(jodaDateTime1.getMillis() * 1000000 + 123456);
        DateTime dateTime2 = new DateTime(jodaDateTime2.getMillis() * 1000000 + 123457);

        TestCase.assertTrue(DateTimeUtil.isBefore(dateTime1, dateTime2));
        TestCase.assertFalse(DateTimeUtil.isBefore(dateTime2, dateTime1));
        TestCase.assertFalse(DateTimeUtil.isBefore(null, dateTime2));
        TestCase.assertFalse(DateTimeUtil.isBefore(null, null));
        TestCase.assertFalse(DateTimeUtil.isBefore(dateTime1, null));
    }

    public void testIsAfter() throws Exception {
        org.joda.time.DateTime jodaDateTime1 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
        org.joda.time.DateTime jodaDateTime2 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");

        DateTime dateTime1 = new DateTime(jodaDateTime1.getMillis() * 1000000 + 123456);
        DateTime dateTime2 = new DateTime(jodaDateTime2.getMillis() * 1000000 + 123457);

        TestCase.assertFalse(DateTimeUtil.isAfter(dateTime1, dateTime2));
        TestCase.assertTrue(DateTimeUtil.isAfter(dateTime2, dateTime1));
        TestCase.assertFalse(DateTimeUtil.isAfter(null, dateTime2));
        TestCase.assertFalse(DateTimeUtil.isAfter(null, null));
        TestCase.assertFalse(DateTimeUtil.isAfter(dateTime1, null));
    }

    public void testPlus() throws Exception {
        org.joda.time.DateTime jodaDateTime = new org.joda.time.DateTime("2010-01-01T12:13:14.999");

        DateTime dateTime = new DateTime(jodaDateTime.getMillis() * 1000000 + 123456);

        Period period = new Period("T1h");

        TestCase.assertEquals(dateTime.getNanos() + 3600000000000L, DateTimeUtil.plus(dateTime, period).getNanos());

        period = new Period("-T1h");

        TestCase.assertEquals(dateTime.getNanos() - 3600000000000L, DateTimeUtil.plus(dateTime, period).getNanos());


        // overflow plus
        DateTimeUtil.plus(new DateTime(Long.MAX_VALUE - 10), 10); // edge at max
        try {
            DateTimeUtil.plus(new DateTime(Long.MAX_VALUE), 1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtil.DateTimeOverflowException e) {
            // ok
        }

        DateTimeUtil.plus(new DateTime(Long.MIN_VALUE + 10), -10); // edge at min
        try {
            DateTimeUtil.plus(new DateTime(Long.MIN_VALUE), -1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtil.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testMinus() throws Exception {
        org.joda.time.DateTime jodaDateTime1 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
        org.joda.time.DateTime jodaDateTime2 = new org.joda.time.DateTime("2010-01-01T13:13:14.999");

        DateTime dateTime1 = new DateTime(jodaDateTime1.getMillis() * 1000000 + 123456);
        DateTime dateTime2 = new DateTime(jodaDateTime2.getMillis() * 1000000 + 123456);

        TestCase.assertEquals(-3600000000000L, DateTimeUtil.minus(dateTime1, dateTime2));
        TestCase.assertEquals(3600000000000L, DateTimeUtil.minus(dateTime2, dateTime1));

        Period period = new Period("T1h");

        TestCase.assertEquals(dateTime1.getNanos() - 3600000000000L, DateTimeUtil.minus(dateTime1, period).getNanos());

        period = new Period("-T1h");

        TestCase.assertEquals(dateTime1.getNanos() + 3600000000000L, DateTimeUtil.minus(dateTime1, period).getNanos());



        // overflow minus
        DateTimeUtil.minus(new DateTime(Long.MAX_VALUE - 10), -10); // edge at max
        try {
            DateTimeUtil.minus(new DateTime(Long.MAX_VALUE), -1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtil.DateTimeOverflowException e) {
            // ok
        }

        DateTimeUtil.minus(new DateTime(Long.MIN_VALUE + 10), 10); // edge at min
        try {
            DateTimeUtil.minus(new DateTime(Long.MIN_VALUE), 1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtil.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testDiff() throws Exception {
        org.joda.time.DateTime jodaDateTime1 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
        org.joda.time.DateTime jodaDateTime2 = new org.joda.time.DateTime("2010-01-01T13:13:14.999");

        DateTime dateTime1 = new DateTime(jodaDateTime1.getMillis() * 1000000 + 123456);
        DateTime dateTime2 = new DateTime(jodaDateTime2.getMillis() * 1000000 + 123456);

        TestCase.assertEquals(3600000000000L, DateTimeUtil.diff(dateTime1, dateTime2));
        TestCase.assertEquals(-3600000000000L, DateTimeUtil.diff(dateTime2, dateTime1));

        TestCase.assertEquals(3600000000000L, DateTimeUtil.diffNanos(dateTime1, dateTime2));
        TestCase.assertEquals(-3600000000000L, DateTimeUtil.diffNanos(dateTime2, dateTime1));
    }

    public void testYearDiff() throws Exception {
        org.joda.time.DateTime jt1 = new org.joda.time.DateTime("2010-01-01T12:13:14.999");
        org.joda.time.DateTime jt2 = new org.joda.time.DateTime("2011-01-01T13:13:14.999");
        org.joda.time.DateTime jt3 = new org.joda.time.DateTime("2010-06-30T13:13:14.999");

        DateTime t1 = new DateTime(jt1.getMillis() * 1000000 + 123456);
        DateTime t2 = new DateTime(jt2.getMillis() * 1000000 + 123456);
        DateTime t3 = new DateTime(jt3.getMillis() * 1000000 + 123456);


        TestCase.assertEquals(1.0, DateTimeUtil.yearDiff(t1, t2), 0.01);
        TestCase.assertEquals(0.5, DateTimeUtil.yearDiff(t1, t3), 0.01);
        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DateTimeUtil.yearDiff(null, t1));
        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DateTimeUtil.yearDiff(t1, null));

        TestCase.assertEquals(1.0, DateTimeUtil.diffYear(t1, t2), 0.01);
        TestCase.assertEquals(0.5, DateTimeUtil.diffYear(t1, t3), 0.01);
        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DateTimeUtil.diffYear(null, t1));
        TestCase.assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DateTimeUtil.diffYear(t1, null));
    }

    public void testMillisToNanos() throws Exception {
        TestCase.assertEquals(1000000, DateTimeUtil.millisToNanos(1));

        // The next two tests will fail if DateTimeUtil.ENABLE_MICROTIME_HACK is true
        try {
            DateTimeUtil.millisToNanos(Long.MAX_VALUE / 1_000_000 + 1);
            TestCase.fail("Should have thrown a DateTimeUtil.DateTimeOverflowException");
        } catch (DateTimeUtil.DateTimeOverflowException ignored) {
            /* Exception is expected. */
        }

        try {
            DateTimeUtil.millisToNanos(-Long.MAX_VALUE / 1_000_000 - 1);
            TestCase.fail("Should have thrown a DateTimeUtil.DateTimeOverflowException");
        } catch (DateTimeUtil.DateTimeOverflowException ignored) {
            /* Exception is expected. */
        }
    }

    /*
     * public void testMillisToNanosWithHack() throws Exception { // For this to pass, ENABLE_MICROTIME_HACK in
     * DateTimeUtil must be true (i.e. you have // to run the tests with -DDateTimeUtils.enableMicrotimeHack=true)
     * assertEquals(1_000_000, DateTimeUtil.millisToNanos(1)); assertEquals(1_000_000_000,
     * DateTimeUtil.millisToNanos(1_000)); assertEquals(1531315655_000_000_000L,
     * DateTimeUtil.millisToNanos(1531315655_000L)); assertEquals(1531315655_000_000_000L,
     * DateTimeUtil.millisToNanos(1531315655_000_000L)); }
     */

    public void testNanosToMillis() throws Exception {
        TestCase.assertEquals(1, DateTimeUtil.nanosToMillis(1000000));
    }

    public void testMicroToNanos() throws Exception {
        TestCase.assertEquals(1000, DateTimeUtil.microsToNanos(1));

        try {
            DateTimeUtil.microsToNanos(Long.MAX_VALUE / 1_000 + 1);
            TestCase.fail("Should have thrown a DateTimeUtil.DateTimeOverflowException");
        } catch (DateTimeUtil.DateTimeOverflowException ignored) {
            /* Exception is expected. */
        }

        try {
            DateTimeUtil.microsToNanos(-Long.MAX_VALUE / 1_000 - 1);
            TestCase.fail("Should have thrown a DateTimeUtil.DateTimeOverflowException");
        } catch (DateTimeUtil.DateTimeOverflowException ignored) {
            /* Exception is expected. */
        }
    }

    public void testNanosToMicros() throws Exception {
        TestCase.assertEquals(1, DateTimeUtil.nanosToMicros(1000));
    }

    public void testConvertDateQuiet() throws Exception {
        // ISO formats
        TestCase.assertEquals(LocalDate.of(2018, 1, 1), DateTimeUtil.convertDateQuiet("2018-01-01"));
        TestCase.assertEquals(LocalDate.of(2018, 12, 31), DateTimeUtil.convertDateQuiet("2018-12-31"));
        TestCase.assertEquals(LocalDate.of(2018, 1, 1), DateTimeUtil.convertDateQuiet("20180101"));
        TestCase.assertEquals(LocalDate.of(2018, 12, 31), DateTimeUtil.convertDateQuiet("20181231"));

        // extremities of the format (LocalDate can store a much larger range than this but we aren't that interested)
        TestCase.assertEquals(LocalDate.of(0, 1, 1), DateTimeUtil.convertDateQuiet("0000-01-01"));
        TestCase.assertEquals(LocalDate.of(9999, 12, 31), DateTimeUtil.convertDateQuiet("9999-12-31"));

        // other variants
        TestCase.assertEquals(LocalDate.of(2018, 1, 1),
                DateTimeUtil.convertDateQuiet("01/01/2018", DateTimeUtil.DateStyle.MDY));
        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
                DateTimeUtil.convertDateQuiet("12/31/2018", DateTimeUtil.DateStyle.MDY));
        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
                DateTimeUtil.convertDateQuiet("12/31/18", DateTimeUtil.DateStyle.MDY));
        TestCase.assertEquals(LocalDate.of(2024, 6, 25), DateTimeUtil.convertDateQuiet("6/25/24", DateTimeUtil.DateStyle.MDY));
        TestCase.assertEquals(LocalDate.of(2024, 6, 2), DateTimeUtil.convertDateQuiet("6/2/24", DateTimeUtil.DateStyle.MDY));
        TestCase.assertEquals(LocalDate.of(2024, 6, 2), DateTimeUtil.convertDateQuiet("6/2/2024", DateTimeUtil.DateStyle.MDY));

        TestCase.assertEquals(LocalDate.of(2018, 1, 1),
                DateTimeUtil.convertDateQuiet("01/01/2018", DateTimeUtil.DateStyle.DMY));
        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
                DateTimeUtil.convertDateQuiet("31/12/2018", DateTimeUtil.DateStyle.DMY));
        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
                DateTimeUtil.convertDateQuiet("31/12/18", DateTimeUtil.DateStyle.DMY));
        TestCase.assertEquals(LocalDate.of(2024, 6, 25), DateTimeUtil.convertDateQuiet("25/6/24", DateTimeUtil.DateStyle.DMY));
        TestCase.assertEquals(LocalDate.of(2024, 6, 2), DateTimeUtil.convertDateQuiet("2/6/24", DateTimeUtil.DateStyle.DMY));
        TestCase.assertEquals(LocalDate.of(2024, 6, 2), DateTimeUtil.convertDateQuiet("2/6/2024", DateTimeUtil.DateStyle.DMY));


        TestCase.assertEquals(LocalDate.of(2018, 1, 1),
                DateTimeUtil.convertDateQuiet("2018/01/01", DateTimeUtil.DateStyle.YMD));
        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
                DateTimeUtil.convertDateQuiet("2018/12/31", DateTimeUtil.DateStyle.YMD));
        TestCase.assertEquals(LocalDate.of(2018, 12, 31),
                DateTimeUtil.convertDateQuiet("18/12/31", DateTimeUtil.DateStyle.YMD));
        TestCase.assertEquals(LocalDate.of(2024, 6, 25), DateTimeUtil.convertDateQuiet("24/6/25", DateTimeUtil.DateStyle.YMD));
        TestCase.assertEquals(LocalDate.of(2024, 6, 2), DateTimeUtil.convertDateQuiet("24/6/2", DateTimeUtil.DateStyle.YMD));
        TestCase.assertEquals(LocalDate.of(2024, 6, 2), DateTimeUtil.convertDateQuiet("2024/6/2", DateTimeUtil.DateStyle.YMD));
    }

    public void testConvertLocalTimeQuiet() throws Exception {

        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59), DateTimeUtil.convertLocalTimeQuiet("L12:59:59"));
        TestCase.assertEquals(java.time.LocalTime.of(0, 0, 0), DateTimeUtil.convertLocalTimeQuiet("L00:00:00"));
        TestCase.assertEquals(java.time.LocalTime.of(23, 59, 59), DateTimeUtil.convertLocalTimeQuiet("L23:59:59"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59), DateTimeUtil.convertLocalTimeQuiet("L125959"));
        TestCase.assertEquals(java.time.LocalTime.of(0, 0, 0), DateTimeUtil.convertLocalTimeQuiet("L000000"));
        TestCase.assertEquals(java.time.LocalTime.of(23, 59, 59), DateTimeUtil.convertLocalTimeQuiet("L235959"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 0, 0), DateTimeUtil.convertLocalTimeQuiet("L12"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 0), DateTimeUtil.convertLocalTimeQuiet("L12:59"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000),
                DateTimeUtil.convertLocalTimeQuiet("L12:59:59.123"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000),
                DateTimeUtil.convertLocalTimeQuiet("L12:59:59.123456"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789),
                DateTimeUtil.convertLocalTimeQuiet("L12:59:59.123456789"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 0, 0), DateTimeUtil.convertLocalTimeQuiet("L12"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 0), DateTimeUtil.convertLocalTimeQuiet("L1259"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000),
                DateTimeUtil.convertLocalTimeQuiet("L125959.123"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000),
                DateTimeUtil.convertLocalTimeQuiet("L125959.123456"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789),
                DateTimeUtil.convertLocalTimeQuiet("L125959.123456789"));
    }

    public void testConvertDate() throws Exception {
        DateTimeUtil.convertDate("2010-01-01"); // shouldn't have an exception

        try {
            DateTimeUtil.convertDate("2010-01-01 NY");
            TestCase.fail("Should have thrown an exception");
        } catch (Exception e) {
        }

        TestCase.assertEquals("DateTimeUtil.convertDate(\"9999-12-31\")",
                LocalDate.of(9999, 12, 31),
                DateTimeUtil.convertDate("9999-12-31"));
    }

    public void testConvertDateTimeQuiet() throws Exception {
        TestCase.assertEquals(
                new DateTime(
                        new org.joda.time.DateTime("2010-01-01", DateTimeZone.forID("America/New_York")).getMillis()
                                * 1000000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01 NY"));
        TestCase.assertEquals(new DateTime(
                new org.joda.time.DateTime("2010-01-01T12:00:00", DateTimeZone.forID("America/New_York")).getMillis()
                        * 1000000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00 NY"));
        TestCase.assertEquals(new DateTime(
                new org.joda.time.DateTime("2010-01-01T12:00:00.1", DateTimeZone.forID("America/New_York")).getMillis()
                        * 1000000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.1 NY"));
        TestCase.assertEquals(new DateTime(
                new org.joda.time.DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/New_York"))
                        .getMillis() * 1000000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.123 NY"));
        TestCase.assertEquals(new DateTime(
                new org.joda.time.DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/New_York"))
                        .getMillis() * 1000000
                        + 400000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.1234 NY"));
        TestCase.assertEquals(
                new DateTime(
                        new org.joda.time.DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/New_York"))
                                .getMillis()
                                * 1000000 + 456789),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.123456789 NY"));

        TestCase.assertEquals(
                new DateTime(new org.joda.time.DateTime("2010-01-01", DateTimeZone.forID("America/Chicago")).getMillis()
                        * 1000000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01 MN"));
        TestCase.assertEquals(new DateTime(
                new org.joda.time.DateTime("2010-01-01T12:00:00", DateTimeZone.forID("America/Chicago")).getMillis()
                        * 1000000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00 MN"));
        TestCase.assertEquals(new DateTime(
                new org.joda.time.DateTime("2010-01-01T12:00:00.1", DateTimeZone.forID("America/Chicago")).getMillis()
                        * 1000000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.1 MN"));
        TestCase.assertEquals(new DateTime(
                new org.joda.time.DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/Chicago")).getMillis()
                        * 1000000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.123 MN"));
        TestCase.assertEquals(new DateTime(
                new org.joda.time.DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/Chicago")).getMillis()
                        * 1000000
                        + 400000),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.1234 MN"));
        TestCase.assertEquals(
                new DateTime(
                        new org.joda.time.DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/Chicago"))
                                .getMillis()
                                * 1000000 + 456789),
                DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.123456789 MN"));

        TestCase.assertEquals(new DateTime(1503343549064106107L),
                DateTimeUtil.convertDateTimeQuiet("2017-08-21T15:25:49.064106107 NY"));

        // assertEquals(new DateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.UTC).getMillis()*1000000),
        // DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.123+0000"));
        // assertEquals(new DateTime(new DateTime("2010-01-01T12:00:00.123",
        // DateTimeZone.forID("America/New_York")).getMillis()*1000000),
        // DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.123-0400"));
        // assertEquals(new DateTime(new DateTime("2010-01-01T12:00:00.123",
        // DateTimeZone.forID("Asia/Seoul")).getMillis()*1000000),
        // DateTimeUtil.convertDateTimeQuiet("2010-01-01T12:00:00.123+0900"));
    }

    public void testConvertDateTime() throws Exception {
        DateTimeUtil.convertDateTime("2010-01-01 NY"); // shouldn't have an exception

        try {
            DateTimeUtil.convertDateTime("2010-01-01");
            TestCase.fail("Should have thrown an exception");
        } catch (Exception e) {
        }

        TestCase.assertEquals("DateTimeUtil.convertDateTime(\"2262-04-11T19:47:16.854775807 NY\").getNanos()",
                Long.MAX_VALUE,
                DateTimeUtil.convertDateTime("2262-04-11T19:47:16.854775807 NY").getNanos());
    }

    public void testConvertTimeQuiet() throws Exception {
        TestCase.assertEquals(new LocalTime("12:00").getMillisOfDay() * 1000000L, DateTimeUtil.convertTimeQuiet("12:00"));
        TestCase.assertEquals(new LocalTime("12:00:00").getMillisOfDay() * 1000000L, DateTimeUtil.convertTimeQuiet("12:00:00"));
        TestCase.assertEquals(new LocalTime("12:00:00.123").getMillisOfDay() * 1000000L,
                DateTimeUtil.convertTimeQuiet("12:00:00.123"));
        TestCase.assertEquals(new LocalTime("12:00:00.123").getMillisOfDay() * 1000000L + 400000,
                DateTimeUtil.convertTimeQuiet("12:00:00.1234"));
        TestCase.assertEquals(new LocalTime("12:00:00.123").getMillisOfDay() * 1000000L + 456789,
                DateTimeUtil.convertTimeQuiet("12:00:00.123456789"));

        TestCase.assertEquals(new LocalTime("2:00").getMillisOfDay() * 1000000L, DateTimeUtil.convertTimeQuiet("2:00"));
        TestCase.assertEquals(new LocalTime("2:00:00").getMillisOfDay() * 1000000L, DateTimeUtil.convertTimeQuiet("2:00:00"));
        TestCase.assertEquals(new LocalTime("2:00:00.123").getMillisOfDay() * 1000000L,
                DateTimeUtil.convertTimeQuiet("2:00:00.123"));
        TestCase.assertEquals(new LocalTime("2:00:00.123").getMillisOfDay() * 1000000L + 400000,
                DateTimeUtil.convertTimeQuiet("2:00:00.1234"));
        TestCase.assertEquals(new LocalTime("2:00:00.123").getMillisOfDay() * 1000000L + 456789,
                DateTimeUtil.convertTimeQuiet("2:00:00.123456789"));

        TestCase.assertEquals(new LocalTime("2:00").getMillisOfDay() * 1000000L + 3L * 1000000 * DateUtil.MILLIS_PER_DAY,
                DateTimeUtil.convertTimeQuiet("3T2:00"));
        TestCase.assertEquals(new LocalTime("2:00:00").getMillisOfDay() * 1000000L + 3L * 1000000 * DateUtil.MILLIS_PER_DAY,
                DateTimeUtil.convertTimeQuiet("3T2:00:00"));
        TestCase.assertEquals(new LocalTime("2:00:00.123").getMillisOfDay() * 1000000L + 3L * 1000000 * DateUtil.MILLIS_PER_DAY,
                DateTimeUtil.convertTimeQuiet("3T2:00:00.123"));
        TestCase.assertEquals(new LocalTime("2:00:00.123").getMillisOfDay() * 1000000L + 400000
                + 3L * 1000000 * DateUtil.MILLIS_PER_DAY, DateTimeUtil.convertTimeQuiet("3T2:00:00.1234"));
        TestCase.assertEquals(new LocalTime("2:00:00.123").getMillisOfDay() * 1000000L + 456789
                + 3L * 1000000 * DateUtil.MILLIS_PER_DAY, DateTimeUtil.convertTimeQuiet("3T2:00:00.123456789"));

        TestCase.assertEquals(55549064106107L, DateTimeUtil.convertTimeQuiet("15:25:49.064106107"));
    }

    public void testConvertTime() throws Exception {
        DateTimeUtil.convertTime("12:00"); // shouldn't have an exception

        try {
            DateTimeUtil.convertTime("12");
            TestCase.fail("Should have thrown an exception");
        } catch (Exception e) {
        }
    }

    public void testConvertPeriodQuiet() throws Exception {
        TestCase.assertEquals(new org.joda.time.Period("PT1s"), DateTimeUtil.convertPeriodQuiet("T1S").getJodaPeriod());
        TestCase.assertEquals(new org.joda.time.Period("P1wT1m"), DateTimeUtil.convertPeriodQuiet("1WT1M").getJodaPeriod());
        TestCase.assertEquals(new org.joda.time.Period("P1w"), DateTimeUtil.convertPeriodQuiet("1W").getJodaPeriod());

        TestCase.assertEquals(null, DateTimeUtil.convertPeriodQuiet("-"));
    }

    public void testConvertPeriod() throws Exception {
        DateTimeUtil.convertPeriod("T1S"); // shouldn't have an exception

        try {
            DateTimeUtil.convertPeriod("1S");
            TestCase.fail("Should have thrown an exception");
        } catch (Exception e) {
        }
    }

    public void testTimeFormat() throws Exception {
        TestCase.assertEquals("12:00:00", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("12:00")));
        TestCase.assertEquals("12:00:00", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("12:00:00")));
        TestCase.assertEquals("12:00:00.123000000", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("12:00:00.123")));
        TestCase.assertEquals("12:00:00.123400000", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("12:00:00.1234")));
        TestCase.assertEquals("12:00:00.123456789", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("12:00:00.123456789")));

        TestCase.assertEquals("2:00:00", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("2:00")));
        TestCase.assertEquals("2:00:00", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("2:00:00")));
        TestCase.assertEquals("2:00:00.123000000", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("2:00:00.123")));
        TestCase.assertEquals("2:00:00.123400000", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("2:00:00.1234")));
        TestCase.assertEquals("2:00:00.123456789", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("2:00:00.123456789")));

        TestCase.assertEquals("3T2:00:00", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("3T2:00")));
        TestCase.assertEquals("3T2:00:00", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("3T2:00:00")));
        TestCase.assertEquals("3T2:00:00.123000000", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("3T2:00:00.123")));
        TestCase.assertEquals("3T2:00:00.123400000", DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("3T2:00:00.1234")));
        TestCase.assertEquals("3T2:00:00.123456789",
                DateTimeUtil.format(DateTimeUtil.convertTimeQuiet("3T2:00:00.123456789")));
    }

    public void testFormatDate() throws Exception {
        TestCase.assertEquals("2010-01-01",
                DateTimeUtil.formatDate(DateTimeUtil.convertDateTimeQuiet("2010-01-01 NY"), TimeZone.TZ_NY));
    }

    public void testLowerBin() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DateTime time = DateTimeUtil.convertDateTime("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtil.convertDateTime("2010-06-15T06:14:01 NY"), DateTimeUtil.lowerBin(time, second));
        TestCase.assertEquals(DateTimeUtil.convertDateTime("2010-06-15T06:10:00 NY"), DateTimeUtil.lowerBin(time, 5 * minute));
        TestCase.assertEquals(DateTimeUtil.convertDateTime("2010-06-15T06:00:00 NY"), DateTimeUtil.lowerBin(time, hour));
        TestCase.assertEquals(null, DateTimeUtil.lowerBin(null, 5 * minute));
        TestCase.assertEquals(null, DateTimeUtil.lowerBin(time, io.deephaven.util.QueryConstants.NULL_LONG));

        TestCase.assertEquals(DateTimeUtil.lowerBin(time, second),
                DateTimeUtil.lowerBin(DateTimeUtil.lowerBin(time, second), second));
    }

    public void testLowerBinWithOffset() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DateTime time = DateTimeUtil.convertDateTime("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtil.convertDateTime("2010-06-15T06:11:00 NY"),
                DateTimeUtil.lowerBin(time, 5 * minute, minute));
        TestCase.assertEquals(null, DateTimeUtil.lowerBin(null, 5 * minute, minute));
        TestCase.assertEquals(null, DateTimeUtil.lowerBin(time, QueryConstants.NULL_LONG, minute));
        TestCase.assertEquals(null, DateTimeUtil.lowerBin(time, 5 * minute, QueryConstants.NULL_LONG));

        TestCase.assertEquals(DateTimeUtil.lowerBin(time, second, second),
                DateTimeUtil.lowerBin(DateTimeUtil.lowerBin(time, second, second), second, second));
    }

    public void testUpperBin() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DateTime time = DateTimeUtil.convertDateTime("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtil.convertDateTime("2010-06-15T06:14:02 NY"), DateTimeUtil.upperBin(time, second));
        TestCase.assertEquals(DateTimeUtil.convertDateTime("2010-06-15T06:15:00 NY"), DateTimeUtil.upperBin(time, 5 * minute));
        TestCase.assertEquals(DateTimeUtil.convertDateTime("2010-06-15T07:00:00 NY"), DateTimeUtil.upperBin(time, hour));
        TestCase.assertEquals(null, DateTimeUtil.upperBin(null, 5 * minute));
        TestCase.assertEquals(null, DateTimeUtil.upperBin(time, io.deephaven.util.QueryConstants.NULL_LONG));

        TestCase.assertEquals(DateTimeUtil.upperBin(time, second),
                DateTimeUtil.upperBin(DateTimeUtil.upperBin(time, second), second));
    }

    public void testUpperBinWithOffset() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DateTime time = DateTimeUtil.convertDateTime("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtil.convertDateTime("2010-06-15T06:16:00 NY"),
                DateTimeUtil.upperBin(time, 5 * minute, minute));
        TestCase.assertEquals(null, DateTimeUtil.upperBin(null, 5 * minute, minute));
        TestCase.assertEquals(null, DateTimeUtil.upperBin(time, io.deephaven.util.QueryConstants.NULL_LONG, minute));
        TestCase.assertEquals(null, DateTimeUtil.upperBin(time, 5 * minute, QueryConstants.NULL_LONG));

        TestCase.assertEquals(DateTimeUtil.upperBin(time, second, second),
                DateTimeUtil.upperBin(DateTimeUtil.upperBin(time, second, second), second, second));
    }

    public void testConvertJimDateTimeQuiet() {
        String s = "2010-09-02T08:17:17.502-0400";
        DateTime known = DateTimeUtil.convertDateTimeQuiet(s);
        DateTime trial = DateTimeUtil.convertJimDateTimeQuiet(s);
        TestCase.assertEquals(known, trial);
    }

    public void testGetExcelDate() {
        DateTime time = DateTimeUtil.convertDateTime("2010-06-15T16:00:00 NY");
        TestCase.assertTrue(CompareUtils.doubleEquals(40344.666666666664, DateTimeUtil.getExcelDateTime(time)));
        TestCase.assertTrue(CompareUtils.doubleEquals(40344.625, DateTimeUtil.getExcelDateTime(time, TimeZones.TZ_CHICAGO)));
        TestCase.assertTrue(CompareUtils.doubleEquals(40344.625, DateTimeUtil.getExcelDateTime(time, TimeZone.TZ_MN)));
    }

    /**
     * Test autoEpcohTime with the given epoch time.
     * 
     * @param epoch Epoch time (in seconds)
     * @return The year (in the New York timezone) in which the given time falls.
     */
    public int doTestAutoEpochToTime(long epoch) {
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(epoch).getMillis(), epoch * 1000);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(epoch).getMicros(), epoch * 1000 * 1000);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(epoch).getNanos(), epoch * 1000 * 1000 * 1000);

        final long milliValue = epoch * 1000 + (int) (Math.signum(epoch) * 123);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(milliValue).getMillis(), milliValue);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(milliValue).getMicros(), milliValue * 1000);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(milliValue).getNanos(), milliValue * 1000 * 1000);

        final long microValue = milliValue * 1000 + (int) (Math.signum(milliValue) * 456);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(microValue).getMillis(), milliValue);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(microValue).getMicros(), microValue);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(microValue).getNanos(), microValue * 1000);

        final long nanoValue = microValue * 1000 + (int) (Math.signum(microValue) * 789);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(nanoValue).getMillis(), milliValue);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(nanoValue).getMicros(), microValue);
        TestCase.assertEquals(DateTimeUtil.autoEpochToTime(nanoValue).getNanos(), nanoValue);

        return DateTimeUtil.yearNy(DateTimeUtil.autoEpochToTime(nanoValue));
    }

    public void testAutoEpochToTime() {
        long inTheYear2035 = 2057338800;
        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear2035)", 2035, doTestAutoEpochToTime(inTheYear2035));
        long inTheYear1993 = 731966400;
        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear1993)", 1993, doTestAutoEpochToTime(inTheYear1993));
        long inTheYear2013 = 1363114800;
        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear2013)", 2013, doTestAutoEpochToTime(inTheYear2013));

        long inTheYear1904 = -2057338800;
        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear1904)", 1904, doTestAutoEpochToTime(inTheYear1904));
        long inTheYear1946 = -731966400;
        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear1946)", 1946, doTestAutoEpochToTime(inTheYear1946));
        long inTheYear1926 = -1363114800;
        TestCase.assertEquals("doTestAutoEpochToTime(inTheYear1926)", 1926, doTestAutoEpochToTime(inTheYear1926));
    }

    public void testConvertExpression() throws Exception {
        TestCase.assertEquals("_date0", DateTimeUtil.convertExpression("'2010-01-01 NY'").getConvertedFormula());
        TestCase.assertEquals("_time0", DateTimeUtil.convertExpression("'12:00'").getConvertedFormula());
        TestCase.assertEquals("_period0", DateTimeUtil.convertExpression("'T1S'").getConvertedFormula());
        TestCase.assertEquals("'g'", DateTimeUtil.convertExpression("'g'").getConvertedFormula());
    }

    public void testMicrosOfMilli() {
        TestCase.assertEquals(0, DateTimeUtil.microsOfMilliNy(DateTimeUtil.convertDateTime("2015-07-31T20:40 NY")));
        TestCase.assertEquals(0, DateTimeUtil.microsOfMilliNy(DateTimeUtil.convertDateTime("2015-07-31T20:40:00 NY")));
        TestCase.assertEquals(0, DateTimeUtil.microsOfMilliNy(DateTimeUtil.convertDateTime("2015-07-31T20:40:00.123 NY")));
        TestCase.assertEquals(400, DateTimeUtil.microsOfMilliNy(DateTimeUtil.convertDateTime("2015-07-31T20:40:00.1234 NY")));
        TestCase.assertEquals(456,
                DateTimeUtil.microsOfMilliNy(DateTimeUtil.convertDateTime("2015-07-31T20:40:00.123456 NY")));
        TestCase.assertEquals(457,
                DateTimeUtil.microsOfMilliNy(DateTimeUtil.convertDateTime("2015-07-31T20:40:00.1234567 NY"))); // this
        // one
        // should
        // round
        // up
        TestCase.assertEquals(457,
                DateTimeUtil.microsOfMilliNy(DateTimeUtil.convertDateTime("2015-07-31T20:40:00.123456789 NY"))); // this
        // one
        // should
        // round
        // up

    }

    public void testZonedDateTime() {
        final DateTime dateTime1 = DateTimeUtil.convertDateTime("2015-07-31T20:40 NY");
        final ZonedDateTime zonedDateTime1 =
                ZonedDateTime.of(2015, 7, 31, 20, 40, 0, 0, TimeZone.TZ_NY.getTimeZone().toTimeZone().toZoneId());
        TestCase.assertEquals(zonedDateTime1, DateTimeUtil.getZonedDateTime(dateTime1, TimeZone.TZ_NY));
        TestCase.assertEquals(dateTime1, DateTimeUtil.toDateTime(zonedDateTime1));

        final DateTime dateTime2 = DateTimeUtil.convertDateTime("2020-07-31T20:40 NY");
        TestCase.assertEquals(dateTime2, DateTimeUtil.toDateTime(DateTimeUtil.getZonedDateTime(dateTime2, TimeZone.TZ_NY)));

        final DateTime dateTime3 = DateTimeUtil.convertDateTime("2050-07-31T20:40 NY");
        TestCase.assertEquals(dateTime3, DateTimeUtil.toDateTime(DateTimeUtil.getZonedDateTime(dateTime3, TimeZone.TZ_NY)));
    }
}
