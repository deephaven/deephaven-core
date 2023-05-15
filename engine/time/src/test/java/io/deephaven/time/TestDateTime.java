/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

import java.io.*;
import java.time.*;
import java.util.Date;

//TODO: kill joda
public class TestDateTime extends BaseArrayTestCase {

    public void testConstructor() {
        final ZonedDateTime zdt = LocalDateTime.parse("2010-01-01T12:13:14.999123456").atZone(ZoneId.of("Asia/Tokyo"));
        final long nanos = DateTimeUtils.epochNanos(zdt);

        final DateTime dateTime = new DateTime(nanos);
        TestCase.assertEquals(nanos, dateTime.getNanos());
        TestCase.assertEquals(nanos/1_000, dateTime.getMicros());
        TestCase.assertEquals(nanos/1_000_000, dateTime.getMillis());
        TestCase.assertEquals(nanos%1_000_000, dateTime.getNanosPartial());
        TestCase.assertEquals(123456, dateTime.getNanosPartial());
    }

    public void testHashCompare() {
        final DateTime dt1 = new DateTime(123);
        final DateTime dt2 = new DateTime(123);
        final DateTime dt3 = new DateTime(456);

        TestCase.assertEquals(dt1, dt1);
        TestCase.assertEquals(dt1, dt2);
        //noinspection SimplifiableAssertion
        TestCase.assertFalse(dt1.equals(dt3));
        //noinspection ConstantConditions,SimplifiableAssertion
        TestCase.assertFalse(dt1.equals(null));
        //noinspection EqualsBetweenInconvertibleTypes,SimplifiableAssertion
        TestCase.assertFalse(dt1.equals(1));

        //noinspection EqualsWithItself
        TestCase.assertEquals(0, dt1.compareTo(dt1));
        TestCase.assertEquals(0, dt1.compareTo(dt2));
        TestCase.assertEquals(-1, dt1.compareTo(dt3));
        TestCase.assertEquals(1, dt3.compareTo(dt1));

        TestCase.assertEquals(dt1.hashCode(), dt2.hashCode());
        TestCase.assertFalse(dt1.hashCode() == dt3.hashCode());
    }

    public void testToInstant() {
        final ZonedDateTime zdt = LocalDateTime.parse("2010-01-01T12:13:14.999123456").atZone(ZoneId.of("Asia/Tokyo"));
        final long nanos = DateTimeUtils.epochNanos(zdt);
        final DateTime dateTime = new DateTime(nanos);
        TestCase.assertEquals(zdt.toInstant(), dateTime.toInstant());
    }

    public void testToZonedDateTime() {
        final ZonedDateTime zdt = LocalDateTime.parse("2010-01-01T12:13:14.999123456").atZone(ZoneId.of("Asia/Tokyo"));
        final long nanos = DateTimeUtils.epochNanos(zdt);
        final DateTime dateTime = new DateTime(nanos);
        TestCase.assertEquals(zdt, dateTime.toZonedDateTime(ZoneId.of("Asia/Tokyo")));
        TestCase.assertEquals(zdt, dateTime.toZonedDateTime(TimeZone.TZ_JP));
        TestCase.assertEquals(zdt.withZoneSameInstant(TimeZone.TZ_DEFAULT.getZoneId()), dateTime.toZonedDateTime());

        try{
            //noinspection ConstantConditions
            dateTime.toZonedDateTime((ZoneId) null);
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }

        try{
            //noinspection ConstantConditions
            dateTime.toZonedDateTime((TimeZone) null);
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }
    }

    public void testToLocalDate() {
        final ZonedDateTime zdt = LocalDateTime.parse("2010-01-01T12:13:14.999123456").atZone(ZoneId.of("Asia/Tokyo"));
        final LocalDate ld = zdt.toLocalDate();
        final long nanos = DateTimeUtils.epochNanos(zdt);
        final DateTime dateTime = new DateTime(nanos);
        TestCase.assertEquals(ld, dateTime.toLocalDate(ZoneId.of("Asia/Tokyo")));
        TestCase.assertEquals(ld, dateTime.toLocalDate(TimeZone.TZ_JP));
        TestCase.assertEquals(zdt.withZoneSameInstant(TimeZone.TZ_DEFAULT.getZoneId()).toLocalDate(), dateTime.toLocalDate());

        try{
            //noinspection ConstantConditions
            dateTime.toLocalDate((ZoneId) null);
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }

        try{
            //noinspection ConstantConditions
            dateTime.toLocalDate((TimeZone) null);
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }
    }

    public void testToLocalTime() {
        final ZonedDateTime zdt = LocalDateTime.parse("2010-01-01T12:13:14.999123456").atZone(ZoneId.of("Asia/Tokyo"));
        final LocalTime lt = zdt.toLocalTime();
        final long nanos = DateTimeUtils.epochNanos(zdt);
        final DateTime dateTime = new DateTime(nanos);
        TestCase.assertEquals(lt, dateTime.toLocalTime(ZoneId.of("Asia/Tokyo")));
        TestCase.assertEquals(lt, dateTime.toLocalTime(TimeZone.TZ_JP));
        TestCase.assertEquals(zdt.withZoneSameInstant(TimeZone.TZ_DEFAULT.getZoneId()).toLocalTime(), dateTime.toLocalTime());

        try{
            //noinspection ConstantConditions
            dateTime.toLocalTime((ZoneId) null);
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }

        try{
            //noinspection ConstantConditions
            dateTime.toLocalTime((TimeZone) null);
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }
    }

    public void testToDate() {
        final ZonedDateTime zdt = LocalDateTime.parse("2010-01-01T12:13:14.999123456").atZone(ZoneId.of("Asia/Tokyo"));
        final long nanos = DateTimeUtils.epochNanos(zdt);
        final DateTime dateTime = new DateTime(nanos);
        TestCase.assertEquals(new Date(nanos/1_000_000), dateTime.toDate());
    }

    public void testOf() {
        final ZoneId tz = ZoneId.of("Asia/Tokyo");
        final ZonedDateTime zdt = LocalDateTime.parse("2010-01-01T12:13:14.999123456").atZone(tz);
        final long nanos = DateTimeUtils.epochNanos(zdt);
        final DateTime dateTime = new DateTime(nanos);

        final io.deephaven.base.clock.Clock clock = new io.deephaven.base.clock.Clock() {

            @Override
            public long currentTimeMillis() {
                return DateTimeUtils.epochMillis(zdt);
            }

            @Override
            public long currentTimeMicros() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long currentTimeNanos() {
                return DateTimeUtils.epochNanos(zdt);
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

        TestCase.assertEquals(dateTime, DateTime.of(zdt.toInstant()));
        TestCase.assertEquals(dateTime, DateTime.of(clock));
        TestCase.assertEquals(new DateTime(DateTimeUtils.millisToNanos(DateTimeUtils.epochMillis(zdt))), DateTime.ofMillis(clock));
    }

    public void testNow() {
        long now = io.deephaven.base.clock.Clock.system().currentTimeNanos();
        long delta = DateTime.now().getNanos() - now;
        TestCase.assertTrue("Delta: " + delta, delta >= -DateTimeUtils.MILLI && delta < DateTimeUtils.SECOND);

        now = io.deephaven.base.clock.Clock.system().currentTimeNanos();
        delta = DateTime.nowMillis().getNanos() - now;
        TestCase.assertTrue("Delta: " + delta, delta >= -DateTimeUtils.MILLI && delta < DateTimeUtils.SECOND);
    }

    public void testToDateString() {
        DateTime dateTime = DateTimeUtils.parseDateTime("2016-11-06T04:00 UTC"); // 11/6 is the last day of DST

        {
            TestCase.assertEquals(dateTime.toDateString(), dateTime.toDateString(TimeZone.TZ_DEFAULT));
        }

        { // America/New_York
            String zoneId = "America/New_York";
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(TimeZone.TZ_NY));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // UTC
            String zoneId = "UTC";
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(TimeZone.TZ_UTC));
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+2 - 2 hours *EAST*
            String zoneId = "Etc/GMT+2";
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+4 -- 4 hours *WEST*
            String zoneId = "Etc/GMT+4";
            TestCase.assertEquals("2016-11-06", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+2 -- 5 hours *WEST*
            String zoneId = "Etc/GMT+5";
            TestCase.assertEquals("2016-11-05", dateTime.toDateString(ZoneId.of(zoneId)));
        }

        try{
            //noinspection ConstantConditions
            dateTime.toDateString((ZoneId) null);
            TestCase.fail("Should throw an exception");
        }catch (Exception e){
            //pass
        }

        try{
            //noinspection ConstantConditions
            dateTime.toDateString((TimeZone) null);
            TestCase.fail("Should throw an exception");
        }catch (Exception e){
            //pass
        }
    }

    public void testToString() {
        final DateTime utc = DateTimeUtils.parseDateTime("2016-11-06T04:00 UTC"); // 11/6 is the last day of DST
        final DateTime ny = DateTimeUtils.parseDateTime("2016-11-06T04:00 NY"); // 11/6 is the last day of DST

        { // UTC
            String zoneId = "UTC";
            TestCase.assertEquals("2016-11-06T04:00 UTC", utc.toString(TimeZone.TZ_UTC));
            TestCase.assertEquals("2016-11-06T04:00 UTC", utc.toString(ZoneId.of(zoneId)));
        }

        { // America/New_York
            String zoneId = "America/New_York";
            TestCase.assertEquals("2016-11-06T04:00 NY", ny.toString(TimeZone.TZ_NY));
            TestCase.assertEquals("2016-11-06T04:00 NY", ny.toString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+2 - 2 hours *EAST*
            String zoneId = "Etc/GMT+2";
            TestCase.assertEquals("2016-11-06T06:00 xx", utc.toString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+4 -- 4 hours *WEST*
            String zoneId = "Etc/GMT+4";
            TestCase.assertEquals("2016-11-06T08:00 xx", utc.toString(ZoneId.of(zoneId)));
        }

        { // Etc/GMT+2 -- 5 hours *WEST*
            String zoneId = "Etc/GMT+5";
            TestCase.assertEquals("2016-11-05T12:00 xx", utc.toString(ZoneId.of(zoneId)));
        }

        {
            TestCase.assertEquals(utc.toString(), utc.toString(TimeZone.TZ_DEFAULT));
            TestCase.assertEquals(ny.toString(), ny.toString(TimeZone.TZ_DEFAULT));
        }

        try{
            //noinspection ConstantConditions
            utc.toString((ZoneId) null);
            TestCase.fail("Should throw an exception");
        }catch (Exception e){
            //pass
        }

        try{
            //noinspection ConstantConditions
            utc.toString((TimeZone) null);
            TestCase.fail("Should throw an exception");
        }catch (Exception e){
            //pass
        }

    }

    public void testSerialization() throws IOException, ClassNotFoundException {
        final DateTime obj = DateTimeUtils.parseDateTime("2016-11-06T04:00 UTC");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        final byte[] b = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(b);
        ObjectInputStream ois = new ObjectInputStream(bais);
        DateTime o = (DateTime) ois.readObject();
        assertEquals(obj, o);
    }

}
