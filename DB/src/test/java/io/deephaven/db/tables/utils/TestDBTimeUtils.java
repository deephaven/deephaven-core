/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.CompareUtils;
import io.deephaven.base.clock.TimeZones;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.util.DateUtil;
import io.deephaven.util.QueryConstants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import org.joda.time.Period;

import java.time.LocalDate;
import java.time.ZonedDateTime;

public class TestDBTimeUtils extends BaseArrayTestCase {

    public void testMillis() throws Exception{
        DateTime jodaDateTime = new DateTime("2010-01-01T12:13:14.999");

        DBDateTime dateTime = new DBDateTime(jodaDateTime.getMillis()*1000000+123456);

        assertEquals(jodaDateTime.getMillis(), DBTimeUtils.millis(dateTime));

        assertEquals(io.deephaven.util.QueryConstants.NULL_LONG, DBTimeUtils.millis(null));
    }

    public void testNanos() throws Exception{
        DateTime jodaDateTime = new DateTime("2010-01-01T12:13:14.999");

        DBDateTime dateTime = new DBDateTime(jodaDateTime.getMillis()*1000000+123456);

        assertEquals(jodaDateTime.getMillis()*1000000+123456, DBTimeUtils.nanos(dateTime));

        assertEquals(io.deephaven.util.QueryConstants.NULL_LONG, DBTimeUtils.nanos((DBDateTime) null));
    }

    public void testMidnightConversion() throws Exception{
        DateTime jodaDateTime = new DateTime("2010-01-01T12:13:14.999");
        DateTime jodaMidnight = new DateTime("2010-01-01T00:00:00.000-05");

        DBDateTime dateTime = new DBDateTime(jodaDateTime.getMillis()*1000000+123456);
        DBDateTime midnight = DBTimeUtils.dateAtMidnight(dateTime, DBTimeZone.TZ_NY);

        assertEquals(jodaMidnight.getMillis(), DBTimeUtils.millis(midnight));
        assertEquals(jodaMidnight.getMillis(), DBTimeUtils.millisToDateAtMidnightNy(dateTime.getMillis()).getMillis());

        assertNull(DBTimeUtils.millisToDateAtMidnightNy(io.deephaven.util.QueryConstants.NULL_LONG));
    }

    public void testIsBefore() throws Exception{
        DateTime jodaDateTime1 = new DateTime("2010-01-01T12:13:14.999");
        DateTime jodaDateTime2 = new DateTime("2010-01-01T12:13:14.999");

        DBDateTime dateTime1 = new DBDateTime(jodaDateTime1.getMillis()*1000000+123456);
        DBDateTime dateTime2 = new DBDateTime(jodaDateTime2.getMillis()*1000000+123457);

        assertTrue(DBTimeUtils.isBefore(dateTime1, dateTime2));
        assertFalse(DBTimeUtils.isBefore(dateTime2, dateTime1));
        assertFalse(DBTimeUtils.isBefore(null, dateTime2));
        assertFalse(DBTimeUtils.isBefore(null, null));
        assertFalse(DBTimeUtils.isBefore(dateTime1, null));
    }

    public void testIsAfter() throws Exception{
        DateTime jodaDateTime1 = new DateTime("2010-01-01T12:13:14.999");
        DateTime jodaDateTime2 = new DateTime("2010-01-01T12:13:14.999");

        DBDateTime dateTime1 = new DBDateTime(jodaDateTime1.getMillis()*1000000+123456);
        DBDateTime dateTime2 = new DBDateTime(jodaDateTime2.getMillis()*1000000+123457);

        assertFalse(DBTimeUtils.isAfter(dateTime1, dateTime2));
        assertTrue(DBTimeUtils.isAfter(dateTime2, dateTime1));
        assertFalse(DBTimeUtils.isAfter(null, dateTime2));
        assertFalse(DBTimeUtils.isAfter(null, null));
        assertFalse(DBTimeUtils.isAfter(dateTime1, null));
    }

    public void testPlus() throws Exception{
        DateTime jodaDateTime = new DateTime("2010-01-01T12:13:14.999");

        DBDateTime dateTime = new DBDateTime(jodaDateTime.getMillis()*1000000+123456);

        DBPeriod period = new DBPeriod("T1h");

        assertEquals(dateTime.getNanos()+3600000000000L, DBTimeUtils.plus(dateTime, period).getNanos());

        period = new DBPeriod("-T1h");

        assertEquals(dateTime.getNanos()-3600000000000L, DBTimeUtils.plus(dateTime, period).getNanos());


        //overflow plus
        DBTimeUtils.plus(new DBDateTime(Long.MAX_VALUE - 10), 10); //edge at max
        try {
            DBTimeUtils.plus(new DBDateTime(Long.MAX_VALUE), 1);
            fail("This should have overflowed");
        } catch (DBTimeUtils.DBDateTimeOverflowException e) {
            //ok
        }

        DBTimeUtils.plus(new DBDateTime(Long.MIN_VALUE + 10), -10); //edge at min
        try {
            DBTimeUtils.plus(new DBDateTime(Long.MIN_VALUE), -1);
            fail("This should have overflowed");
        } catch (DBTimeUtils.DBDateTimeOverflowException e) {
            //ok
        }
    }

    public void testMinus() throws Exception{
        DateTime jodaDateTime1 = new DateTime("2010-01-01T12:13:14.999");
        DateTime jodaDateTime2 = new DateTime("2010-01-01T13:13:14.999");

        DBDateTime dateTime1 = new DBDateTime(jodaDateTime1.getMillis()*1000000+123456);
        DBDateTime dateTime2 = new DBDateTime(jodaDateTime2.getMillis()*1000000+123456);

        assertEquals(-3600000000000L, DBTimeUtils.minus(dateTime1, dateTime2));
        assertEquals(3600000000000L, DBTimeUtils.minus(dateTime2, dateTime1));

        DBPeriod period = new DBPeriod("T1h");

        assertEquals(dateTime1.getNanos()-3600000000000L, DBTimeUtils.minus(dateTime1, period).getNanos());

        period = new DBPeriod("-T1h");

        assertEquals(dateTime1.getNanos()+3600000000000L, DBTimeUtils.minus(dateTime1, period).getNanos());



        //overflow minus
        DBTimeUtils.minus(new DBDateTime(Long.MAX_VALUE - 10), -10); //edge at max
        try {
            DBTimeUtils.minus(new DBDateTime(Long.MAX_VALUE), -1);
            fail("This should have overflowed");
        } catch (DBTimeUtils.DBDateTimeOverflowException e) {
            //ok
        }

        DBTimeUtils.minus(new DBDateTime(Long.MIN_VALUE + 10), 10); //edge at min
        try {
            DBTimeUtils.minus(new DBDateTime(Long.MIN_VALUE), 1);
            fail("This should have overflowed");
        } catch (DBTimeUtils.DBDateTimeOverflowException e) {
            //ok
        }
    }

    public void testDiff() throws Exception{
        DateTime jodaDateTime1 = new DateTime("2010-01-01T12:13:14.999");
        DateTime jodaDateTime2 = new DateTime("2010-01-01T13:13:14.999");

        DBDateTime dateTime1 = new DBDateTime(jodaDateTime1.getMillis()*1000000+123456);
        DBDateTime dateTime2 = new DBDateTime(jodaDateTime2.getMillis()*1000000+123456);

        assertEquals(3600000000000L, DBTimeUtils.diff(dateTime1, dateTime2));
        assertEquals(-3600000000000L, DBTimeUtils.diff(dateTime2, dateTime1));

        assertEquals(3600000000000L, DBTimeUtils.diffNanos(dateTime1, dateTime2));
        assertEquals(-3600000000000L, DBTimeUtils.diffNanos(dateTime2, dateTime1));
    }

    public void testYearDiff() throws Exception {
        DateTime jt1 = new DateTime("2010-01-01T12:13:14.999");
        DateTime jt2 = new DateTime("2011-01-01T13:13:14.999");
        DateTime jt3 = new DateTime("2010-06-30T13:13:14.999");

        DBDateTime t1 = new DBDateTime(jt1.getMillis()*1000000+123456);
        DBDateTime t2 = new DBDateTime(jt2.getMillis()*1000000+123456);
        DBDateTime t3 = new DBDateTime(jt3.getMillis()*1000000+123456);


        assertEquals(1.0, DBTimeUtils.yearDiff(t1, t2), 0.01);
        assertEquals(0.5, DBTimeUtils.yearDiff(t1, t3), 0.01);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DBTimeUtils.yearDiff(null, t1));
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DBTimeUtils.yearDiff(t1, null));

        assertEquals(1.0, DBTimeUtils.diffYear(t1, t2), 0.01);
        assertEquals(0.5, DBTimeUtils.diffYear(t1, t3), 0.01);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DBTimeUtils.diffYear(null, t1));
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE, DBTimeUtils.diffYear(t1, null));
    }

    public void testMillisToNanos() throws Exception {
        assertEquals(1000000, DBTimeUtils.millisToNanos(1));

        // The next two tests will fail if DBTimeUtils.ENABLE_MICROTIME_HACK is true
        try {
            DBTimeUtils.millisToNanos(Long.MAX_VALUE / 1_000_000 + 1);
            fail("Should have thrown a DBTimeUtils.DBDateTimeOverflowException");
        } catch (DBTimeUtils.DBDateTimeOverflowException ignored) {
            /* Exception is expected. */
        }

        try {
            DBTimeUtils.millisToNanos(-Long.MAX_VALUE / 1_000_000 - 1);
            fail("Should have thrown a DBTimeUtils.DBDateTimeOverflowException");
        } catch (DBTimeUtils.DBDateTimeOverflowException ignored) {
            /* Exception is expected. */
        }
    }

    /*public void testMillisToNanosWithHack() throws Exception {
        // For this to pass, ENABLE_MICROTIME_HACK in DBTimeUtils must be true (i.e. you have
        //  to run the tests with -DDBTimeUtils.enableMicrotimeHack=true)
        assertEquals(1_000_000, DBTimeUtils.millisToNanos(1));
        assertEquals(1_000_000_000, DBTimeUtils.millisToNanos(1_000));
        assertEquals(1531315655_000_000_000L, DBTimeUtils.millisToNanos(1531315655_000L));
        assertEquals(1531315655_000_000_000L, DBTimeUtils.millisToNanos(1531315655_000_000L));
    }*/

    public void testNanosToMillis() throws Exception {
        assertEquals(1, DBTimeUtils.nanosToMillis(1000000));
    }

    public void testMicroToNanos() throws Exception {
        assertEquals(1000, DBTimeUtils.microsToNanos(1));

        try {
            DBTimeUtils.microsToNanos(Long.MAX_VALUE / 1_000 + 1);
            fail("Should have thrown a DBTimeUtils.DBDateTimeOverflowException");
        } catch (DBTimeUtils.DBDateTimeOverflowException ignored) {
            /* Exception is expected. */
        }

        try {
            DBTimeUtils.microsToNanos(-Long.MAX_VALUE / 1_000 - 1);
            fail("Should have thrown a DBTimeUtils.DBDateTimeOverflowException");
        } catch (DBTimeUtils.DBDateTimeOverflowException ignored) {
            /* Exception is expected. */
        }
    }

    public void testNanosToMicros() throws Exception {
        assertEquals(1, DBTimeUtils.nanosToMicros(1000));
    }

    public void testConvertDateQuiet() throws Exception {
        // ISO formats
        assertEquals(LocalDate.of(2018,1,1), DBTimeUtils.convertDateQuiet("2018-01-01"));
        assertEquals(LocalDate.of(2018,12,31), DBTimeUtils.convertDateQuiet("2018-12-31"));
        assertEquals(LocalDate.of(2018,1,1), DBTimeUtils.convertDateQuiet("20180101"));
        assertEquals(LocalDate.of(2018,12,31), DBTimeUtils.convertDateQuiet("20181231"));

        // extremities of the format (LocalDate can store a much larger range than this but we aren't that interested)
        assertEquals(LocalDate.of(0,1,1), DBTimeUtils.convertDateQuiet("0000-01-01"));
        assertEquals(LocalDate.of(9999,12,31), DBTimeUtils.convertDateQuiet("9999-12-31"));

        // other variants
        assertEquals(LocalDate.of(2018,1,1), DBTimeUtils.convertDateQuiet("01/01/2018", DBTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2018,12,31), DBTimeUtils.convertDateQuiet("12/31/2018", DBTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2018,12,31), DBTimeUtils.convertDateQuiet("12/31/18", DBTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2024,6,25), DBTimeUtils.convertDateQuiet("6/25/24", DBTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2024,6,2), DBTimeUtils.convertDateQuiet("6/2/24", DBTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2024,6,2), DBTimeUtils.convertDateQuiet("6/2/2024", DBTimeUtils.DateStyle.MDY));

        assertEquals(LocalDate.of(2018,1,1), DBTimeUtils.convertDateQuiet("01/01/2018", DBTimeUtils.DateStyle.DMY));
        assertEquals(LocalDate.of(2018,12,31), DBTimeUtils.convertDateQuiet("31/12/2018", DBTimeUtils.DateStyle.DMY));
        assertEquals(LocalDate.of(2018,12,31), DBTimeUtils.convertDateQuiet("31/12/18", DBTimeUtils.DateStyle.DMY));
        assertEquals(LocalDate.of(2024,6,25), DBTimeUtils.convertDateQuiet("25/6/24", DBTimeUtils.DateStyle.DMY));
        assertEquals(LocalDate.of(2024,6,2), DBTimeUtils.convertDateQuiet("2/6/24", DBTimeUtils.DateStyle.DMY));
        assertEquals(LocalDate.of(2024,6,2), DBTimeUtils.convertDateQuiet("2/6/2024", DBTimeUtils.DateStyle.DMY));


        assertEquals(LocalDate.of(2018,1,1), DBTimeUtils.convertDateQuiet("2018/01/01", DBTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2018,12,31), DBTimeUtils.convertDateQuiet("2018/12/31", DBTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2018,12,31), DBTimeUtils.convertDateQuiet("18/12/31", DBTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2024,6,25), DBTimeUtils.convertDateQuiet("24/6/25", DBTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2024,6,2), DBTimeUtils.convertDateQuiet("24/6/2", DBTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2024,6,2), DBTimeUtils.convertDateQuiet("2024/6/2", DBTimeUtils.DateStyle.YMD));
    }

    public void testConvertLocalTimeQuiet() throws Exception {

        assertEquals(java.time.LocalTime.of(12, 59, 59), DBTimeUtils.convertLocalTimeQuiet("L12:59:59"));
        assertEquals(java.time.LocalTime.of(0, 0, 0), DBTimeUtils.convertLocalTimeQuiet("L00:00:00"));
        assertEquals(java.time.LocalTime.of(23, 59, 59), DBTimeUtils.convertLocalTimeQuiet("L23:59:59"));

        assertEquals(java.time.LocalTime.of(12, 59, 59), DBTimeUtils.convertLocalTimeQuiet("L125959"));
        assertEquals(java.time.LocalTime.of(0, 0, 0), DBTimeUtils.convertLocalTimeQuiet("L000000"));
        assertEquals(java.time.LocalTime.of(23, 59, 59), DBTimeUtils.convertLocalTimeQuiet("L235959"));

        assertEquals(java.time.LocalTime.of(12, 0, 0), DBTimeUtils.convertLocalTimeQuiet("L12"));
        assertEquals(java.time.LocalTime.of(12, 59, 0), DBTimeUtils.convertLocalTimeQuiet("L12:59"));
        assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000), DBTimeUtils.convertLocalTimeQuiet("L12:59:59.123"));
        assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000), DBTimeUtils.convertLocalTimeQuiet("L12:59:59.123456"));
        assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789), DBTimeUtils.convertLocalTimeQuiet("L12:59:59.123456789"));

        assertEquals(java.time.LocalTime.of(12, 0, 0), DBTimeUtils.convertLocalTimeQuiet("L12"));
        assertEquals(java.time.LocalTime.of(12, 59, 0), DBTimeUtils.convertLocalTimeQuiet("L1259"));
        assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000), DBTimeUtils.convertLocalTimeQuiet("L125959.123"));
        assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000), DBTimeUtils.convertLocalTimeQuiet("L125959.123456"));
        assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789), DBTimeUtils.convertLocalTimeQuiet("L125959.123456789"));
    }

    public void testConvertDate() throws Exception {
        DBTimeUtils.convertDate("2010-01-01");  //shouldn't have an exception

        try{
            DBTimeUtils.convertDate("2010-01-01 NY");
            fail("Should have thrown an exception");
        }
        catch(Exception e){}

        assertEquals("DBTimeUtils.convertDate(\"9999-12-31\")",
                LocalDate.of(9999,12,31),
                DBTimeUtils.convertDate("9999-12-31")
        );
    }

    public void testConvertDateTimeQuiet() throws Exception {
        assertEquals(new DBDateTime(new DateTime("2010-01-01", DateTimeZone.forID("America/New_York")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01 NY"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00", DateTimeZone.forID("America/New_York")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00 NY"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.1", DateTimeZone.forID("America/New_York")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.1 NY"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/New_York")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.123 NY"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/New_York")).getMillis()*1000000+400000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.1234 NY"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/New_York")).getMillis()*1000000+456789), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.123456789 NY"));

        assertEquals(new DBDateTime(new DateTime("2010-01-01", DateTimeZone.forID("America/Chicago")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01 MN"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00", DateTimeZone.forID("America/Chicago")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00 MN"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.1", DateTimeZone.forID("America/Chicago")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.1 MN"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/Chicago")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.123 MN"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/Chicago")).getMillis()*1000000+400000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.1234 MN"));
        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/Chicago")).getMillis()*1000000+456789), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.123456789 MN"));

        assertEquals(new DBDateTime(1503343549064106107L), DBTimeUtils.convertDateTimeQuiet("2017-08-21T15:25:49.064106107 NY"));

//        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.UTC).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.123+0000"));
//        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("America/New_York")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.123-0400"));
//        assertEquals(new DBDateTime(new DateTime("2010-01-01T12:00:00.123", DateTimeZone.forID("Asia/Seoul")).getMillis()*1000000), DBTimeUtils.convertDateTimeQuiet("2010-01-01T12:00:00.123+0900"));
    }

    public void testConvertDateTime() throws Exception {
        DBTimeUtils.convertDateTime("2010-01-01 NY");  //shouldn't have an exception

        try{
            DBTimeUtils.convertDateTime("2010-01-01");
            fail("Should have thrown an exception");
        }
        catch(Exception e){}

        assertEquals("DBTimeUtils.convertDateTime(\"2262-04-11T19:47:16.854775807 NY\").getNanos()",
                Long.MAX_VALUE,
                DBTimeUtils.convertDateTime("2262-04-11T19:47:16.854775807 NY").getNanos()
        );
    }

    public void testConvertTimeQuiet() throws Exception {
        assertEquals(new LocalTime("12:00").getMillisOfDay()*1000000L, DBTimeUtils.convertTimeQuiet("12:00"));
        assertEquals(new LocalTime("12:00:00").getMillisOfDay()*1000000L, DBTimeUtils.convertTimeQuiet("12:00:00"));
        assertEquals(new LocalTime("12:00:00.123").getMillisOfDay()*1000000L, DBTimeUtils.convertTimeQuiet("12:00:00.123"));
        assertEquals(new LocalTime("12:00:00.123").getMillisOfDay()*1000000L+400000, DBTimeUtils.convertTimeQuiet("12:00:00.1234"));
        assertEquals(new LocalTime("12:00:00.123").getMillisOfDay()*1000000L+456789, DBTimeUtils.convertTimeQuiet("12:00:00.123456789"));

        assertEquals(new LocalTime("2:00").getMillisOfDay()*1000000L, DBTimeUtils.convertTimeQuiet("2:00"));
        assertEquals(new LocalTime("2:00:00").getMillisOfDay()*1000000L, DBTimeUtils.convertTimeQuiet("2:00:00"));
        assertEquals(new LocalTime("2:00:00.123").getMillisOfDay()*1000000L, DBTimeUtils.convertTimeQuiet("2:00:00.123"));
        assertEquals(new LocalTime("2:00:00.123").getMillisOfDay()*1000000L+400000, DBTimeUtils.convertTimeQuiet("2:00:00.1234"));
        assertEquals(new LocalTime("2:00:00.123").getMillisOfDay()*1000000L+456789, DBTimeUtils.convertTimeQuiet("2:00:00.123456789"));

        assertEquals(new LocalTime("2:00").getMillisOfDay()*1000000L + 3L*1000000*DateUtil.MILLIS_PER_DAY, DBTimeUtils.convertTimeQuiet("3T2:00"));
        assertEquals(new LocalTime("2:00:00").getMillisOfDay()*1000000L + 3L*1000000*DateUtil.MILLIS_PER_DAY, DBTimeUtils.convertTimeQuiet("3T2:00:00"));
        assertEquals(new LocalTime("2:00:00.123").getMillisOfDay()*1000000L + 3L*1000000*DateUtil.MILLIS_PER_DAY, DBTimeUtils.convertTimeQuiet("3T2:00:00.123"));
        assertEquals(new LocalTime("2:00:00.123").getMillisOfDay()*1000000L+400000 + 3L*1000000*DateUtil.MILLIS_PER_DAY, DBTimeUtils.convertTimeQuiet("3T2:00:00.1234"));
        assertEquals(new LocalTime("2:00:00.123").getMillisOfDay()*1000000L+456789 + 3L*1000000*DateUtil.MILLIS_PER_DAY, DBTimeUtils.convertTimeQuiet("3T2:00:00.123456789"));

        assertEquals(55549064106107L, DBTimeUtils.convertTimeQuiet("15:25:49.064106107"));
    }

     public void testConvertTime() throws Exception {
        DBTimeUtils.convertTime("12:00");  //shouldn't have an exception

        try{
            DBTimeUtils.convertTime("12");
            fail("Should have thrown an exception");
        }
        catch(Exception e){}
    }

    public void testConvertPeriodQuiet() throws Exception {
        assertEquals(new Period("PT1s"), DBTimeUtils.convertPeriodQuiet("T1S").getJodaPeriod());
        assertEquals(new Period("P1wT1m"), DBTimeUtils.convertPeriodQuiet("1WT1M").getJodaPeriod());
        assertEquals(new Period("P1w"), DBTimeUtils.convertPeriodQuiet("1W").getJodaPeriod());

        assertEquals(null, DBTimeUtils.convertPeriodQuiet("-"));
    }

    public void testConvertPeriod() throws Exception {
        DBTimeUtils.convertPeriod("T1S");  //shouldn't have an exception

        try{
            DBTimeUtils.convertPeriod("1S");
            fail("Should have thrown an exception");
        }
        catch(Exception e){}
    }

    public void testTimeFormat() throws Exception {
        assertEquals("12:00:00", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("12:00")));
        assertEquals("12:00:00", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("12:00:00")));
        assertEquals("12:00:00.123000000", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("12:00:00.123")));
        assertEquals("12:00:00.123400000", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("12:00:00.1234")));
        assertEquals("12:00:00.123456789", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("12:00:00.123456789")));

        assertEquals("2:00:00", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("2:00")));
        assertEquals("2:00:00", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("2:00:00")));
        assertEquals("2:00:00.123000000", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("2:00:00.123")));
        assertEquals("2:00:00.123400000", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("2:00:00.1234")));
        assertEquals("2:00:00.123456789", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("2:00:00.123456789")));

        assertEquals("3T2:00:00", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("3T2:00")));
        assertEquals("3T2:00:00", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("3T2:00:00")));
        assertEquals("3T2:00:00.123000000", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("3T2:00:00.123")));
        assertEquals("3T2:00:00.123400000", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("3T2:00:00.1234")));
        assertEquals("3T2:00:00.123456789", DBTimeUtils.format(DBTimeUtils.convertTimeQuiet("3T2:00:00.123456789")));
    }

    public void testFormatDate() throws Exception {
        assertEquals("2010-01-01", DBTimeUtils.formatDate(DBTimeUtils.convertDateTimeQuiet("2010-01-01 NY"), DBTimeZone.TZ_NY));
    }

    public void testLowerBin() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DBDateTime time = DBTimeUtils.convertDateTime("2010-06-15T06:14:01.2345 NY");

        assertEquals(DBTimeUtils.convertDateTime("2010-06-15T06:14:01 NY"), DBTimeUtils.lowerBin(time, second));
        assertEquals(DBTimeUtils.convertDateTime("2010-06-15T06:10:00 NY"), DBTimeUtils.lowerBin(time, 5 * minute));
        assertEquals(DBTimeUtils.convertDateTime("2010-06-15T06:00:00 NY"), DBTimeUtils.lowerBin(time, hour));
        assertEquals(null, DBTimeUtils.lowerBin(null, 5 * minute));
        assertEquals(null, DBTimeUtils.lowerBin(time, io.deephaven.util.QueryConstants.NULL_LONG));

        assertEquals(DBTimeUtils.lowerBin(time, second), DBTimeUtils.lowerBin(DBTimeUtils.lowerBin(time, second), second));
    }

    public void testLowerBinWithOffset(){
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DBDateTime time = DBTimeUtils.convertDateTime("2010-06-15T06:14:01.2345 NY");

        assertEquals(DBTimeUtils.convertDateTime("2010-06-15T06:11:00 NY"), DBTimeUtils.lowerBin(time, 5 * minute, minute));
        assertEquals(null, DBTimeUtils.lowerBin(null, 5 * minute, minute));
        assertEquals(null, DBTimeUtils.lowerBin(time, QueryConstants.NULL_LONG, minute));
        assertEquals(null, DBTimeUtils.lowerBin(time, 5 * minute, QueryConstants.NULL_LONG));

        assertEquals(DBTimeUtils.lowerBin(time, second, second), DBTimeUtils.lowerBin(DBTimeUtils.lowerBin(time, second, second), second, second));
    }

    public void testUpperBin(){
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DBDateTime time = DBTimeUtils.convertDateTime("2010-06-15T06:14:01.2345 NY");

        assertEquals(DBTimeUtils.convertDateTime("2010-06-15T06:14:02 NY"), DBTimeUtils.upperBin(time, second));
        assertEquals(DBTimeUtils.convertDateTime("2010-06-15T06:15:00 NY"), DBTimeUtils.upperBin(time, 5 * minute));
        assertEquals(DBTimeUtils.convertDateTime("2010-06-15T07:00:00 NY"), DBTimeUtils.upperBin(time, hour));
        assertEquals(null, DBTimeUtils.upperBin(null, 5 * minute));
        assertEquals(null, DBTimeUtils.upperBin(time, io.deephaven.util.QueryConstants.NULL_LONG));

        assertEquals(DBTimeUtils.upperBin(time, second), DBTimeUtils.upperBin(DBTimeUtils.upperBin(time, second), second));
    }

    public void testUpperBinWithOffset(){
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DBDateTime time = DBTimeUtils.convertDateTime("2010-06-15T06:14:01.2345 NY");

        assertEquals(DBTimeUtils.convertDateTime("2010-06-15T06:16:00 NY"), DBTimeUtils.upperBin(time, 5 * minute, minute));
        assertEquals(null, DBTimeUtils.upperBin(null, 5 * minute, minute));
        assertEquals(null, DBTimeUtils.upperBin(time, io.deephaven.util.QueryConstants.NULL_LONG, minute));
        assertEquals(null, DBTimeUtils.upperBin(time, 5 * minute, QueryConstants.NULL_LONG));

        assertEquals(DBTimeUtils.upperBin(time, second, second), DBTimeUtils.upperBin(DBTimeUtils.upperBin(time, second, second), second, second));
    }

    public void testConvertJimDateTimeQuiet() {
        String s="2010-09-02T08:17:17.502-0400";
        DBDateTime known=DBTimeUtils.convertDateTimeQuiet(s);
        DBDateTime trial=DBTimeUtils.convertJimDateTimeQuiet(s);
        assertEquals(known, trial);
    }

    public void testGetExcelDate() {
        DBDateTime time = DBTimeUtils.convertDateTime("2010-06-15T16:00:00 NY");
        assertTrue(CompareUtils.doubleEquals(40344.666666666664, DBTimeUtils.getExcelDateTime(time)));
        assertTrue(CompareUtils.doubleEquals(40344.625, DBTimeUtils.getExcelDateTime(time, TimeZones.TZ_CHICAGO)));
        assertTrue(CompareUtils.doubleEquals(40344.625, DBTimeUtils.getExcelDateTime(time, DBTimeZone.TZ_MN)));
    }

    /**
     * Test autoEpcohTime with the given epoch time.
     * @param epoch Epoch time (in seconds)
     * @return The year (in the New York timezone) in which the given time falls.
     */
    public int doTestAutoEpochToTime(long epoch)
    {
        assertEquals(DBTimeUtils.autoEpochToTime(epoch).getMillis(), epoch * 1000);
        assertEquals(DBTimeUtils.autoEpochToTime(epoch).getMicros(), epoch * 1000 * 1000);
        assertEquals(DBTimeUtils.autoEpochToTime(epoch).getNanos(), epoch * 1000 * 1000 * 1000);

        final long milliValue = epoch * 1000 + (int) (Math.signum(epoch) * 123);
        assertEquals(DBTimeUtils.autoEpochToTime(milliValue).getMillis(), milliValue);
        assertEquals(DBTimeUtils.autoEpochToTime(milliValue).getMicros(), milliValue * 1000);
        assertEquals(DBTimeUtils.autoEpochToTime(milliValue).getNanos(), milliValue * 1000 * 1000);

        final long microValue = milliValue * 1000 + (int) (Math.signum(milliValue) * 456);
        assertEquals(DBTimeUtils.autoEpochToTime(microValue).getMillis(), milliValue);
        assertEquals(DBTimeUtils.autoEpochToTime(microValue).getMicros(), microValue);
        assertEquals(DBTimeUtils.autoEpochToTime(microValue).getNanos(), microValue * 1000);

        final long nanoValue = microValue * 1000 + (int) (Math.signum(microValue) * 789);
        assertEquals(DBTimeUtils.autoEpochToTime(nanoValue).getMillis(), milliValue);
        assertEquals(DBTimeUtils.autoEpochToTime(nanoValue).getMicros(), microValue);
        assertEquals(DBTimeUtils.autoEpochToTime(nanoValue).getNanos(), nanoValue);

        return DBTimeUtils.yearNy(DBTimeUtils.autoEpochToTime(nanoValue));
    }

    public void testAutoEpochToTime() {
        long inTheYear2035 = 2057338800;
        assertEquals("doTestAutoEpochToTime(inTheYear2035)", 2035, doTestAutoEpochToTime(inTheYear2035));
        long inTheYear1993 =  731966400;
        assertEquals("doTestAutoEpochToTime(inTheYear1993)", 1993, doTestAutoEpochToTime(inTheYear1993));
        long inTheYear2013 =  1363114800;
        assertEquals("doTestAutoEpochToTime(inTheYear2013)", 2013, doTestAutoEpochToTime(inTheYear2013));

        long inTheYear1904= -2057338800;
        assertEquals("doTestAutoEpochToTime(inTheYear1904)", 1904, doTestAutoEpochToTime(inTheYear1904));
        long inTheYear1946 =  -731966400;
        assertEquals("doTestAutoEpochToTime(inTheYear1946)", 1946, doTestAutoEpochToTime(inTheYear1946));
        long inTheYear1926 = -1363114800;
        assertEquals("doTestAutoEpochToTime(inTheYear1926)", 1926, doTestAutoEpochToTime(inTheYear1926));
    }

    public void testConvertExpression() throws Exception {
        assertEquals("_date0", DBTimeUtils.convertExpression("'2010-01-01 NY'").getConvertedFormula());
        assertEquals("_time0", DBTimeUtils.convertExpression("'12:00'").getConvertedFormula());
        assertEquals("_period0", DBTimeUtils.convertExpression("'T1S'").getConvertedFormula());
        assertEquals("'g'", DBTimeUtils.convertExpression("'g'").getConvertedFormula());
    }

    public void testMicrosOfMilli() {
        assertEquals(0, DBTimeUtils.microsOfMilliNy(DBTimeUtils.convertDateTime("2015-07-31T20:40 NY")));
        assertEquals(0, DBTimeUtils.microsOfMilliNy(DBTimeUtils.convertDateTime("2015-07-31T20:40:00 NY")));
        assertEquals(0, DBTimeUtils.microsOfMilliNy(DBTimeUtils.convertDateTime("2015-07-31T20:40:00.123 NY")));
        assertEquals(400, DBTimeUtils.microsOfMilliNy(DBTimeUtils.convertDateTime("2015-07-31T20:40:00.1234 NY")));
        assertEquals(456, DBTimeUtils.microsOfMilliNy(DBTimeUtils.convertDateTime("2015-07-31T20:40:00.123456 NY")));
        assertEquals(457, DBTimeUtils.microsOfMilliNy(DBTimeUtils.convertDateTime("2015-07-31T20:40:00.1234567 NY"))); // this one should round up
        assertEquals(457, DBTimeUtils.microsOfMilliNy(DBTimeUtils.convertDateTime("2015-07-31T20:40:00.123456789 NY"))); // this one should round up

    }

    public void testZonedDateTime() {
        final DBDateTime dateTime1 = DBTimeUtils.convertDateTime("2015-07-31T20:40 NY");
        final ZonedDateTime zonedDateTime1 = ZonedDateTime.of(2015, 7, 31, 20, 40, 0, 0, DBTimeZone.TZ_NY.getTimeZone().toTimeZone().toZoneId());
        assertEquals(zonedDateTime1, DBTimeUtils.getZonedDateTime(dateTime1, DBTimeZone.TZ_NY));
        assertEquals(dateTime1, DBTimeUtils.toDateTime(zonedDateTime1));

        final DBDateTime dateTime2 = DBTimeUtils.convertDateTime("2020-07-31T20:40 NY");
        assertEquals(dateTime2, DBTimeUtils.toDateTime(DBTimeUtils.getZonedDateTime(dateTime2, DBTimeZone.TZ_NY)));

        final DBDateTime dateTime3 = DBTimeUtils.convertDateTime("2050-07-31T20:40 NY");
        assertEquals(dateTime3, DBTimeUtils.toDateTime(DBTimeUtils.getZonedDateTime(dateTime3, DBTimeZone.TZ_NY)));
    }
}
