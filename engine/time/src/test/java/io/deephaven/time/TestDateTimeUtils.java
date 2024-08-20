//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import io.deephaven.base.CompareUtils;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.time.*;
import java.time.temporal.ChronoField;
import java.util.Date;

import static io.deephaven.util.QueryConstants.*;

@SuppressWarnings({"deprecation", "ConstantConditions"})
public class TestDateTimeUtils extends BaseArrayTestCase {
    private static final ZoneId TZ_NY = ZoneId.of("America/New_York");
    private static final ZoneId TZ_JP = ZoneId.of("Asia/Tokyo");
    private static final ZoneId TZ_AL = ZoneId.of("America/Anchorage");
    private static final ZoneId TZ_CT = ZoneId.of("America/Chicago");
    private static final ZoneId TZ_MN = ZoneId.of("America/Chicago");

    public void testConstants() {
        TestCase.assertEquals(0, DateTimeUtils.ZERO_LENGTH_INSTANT_ARRAY.length);

        TestCase.assertEquals(1_000L, DateTimeUtils.MICRO);
        TestCase.assertEquals(1_000_000L, DateTimeUtils.MILLI);
        TestCase.assertEquals(1_000_000_000L, DateTimeUtils.SECOND);
        TestCase.assertEquals(60_000_000_000L, DateTimeUtils.MINUTE);
        TestCase.assertEquals(60 * 60_000_000_000L, DateTimeUtils.HOUR);
        TestCase.assertEquals(24 * 60 * 60_000_000_000L, DateTimeUtils.DAY);
        TestCase.assertEquals(7 * 24 * 60 * 60_000_000_000L, DateTimeUtils.WEEK);
        TestCase.assertEquals(365 * 24 * 60 * 60_000_000_000L, DateTimeUtils.YEAR_365);
        TestCase.assertEquals(31556952000000000L, DateTimeUtils.YEAR_AVG);

        TestCase.assertEquals(1.0, DateTimeUtils.SECONDS_PER_NANO * DateTimeUtils.SECOND);
        TestCase.assertEquals(1.0, DateTimeUtils.MINUTES_PER_NANO * DateTimeUtils.MINUTE);
        TestCase.assertEquals(1.0, DateTimeUtils.HOURS_PER_NANO * DateTimeUtils.HOUR);
        TestCase.assertEquals(1.0, DateTimeUtils.DAYS_PER_NANO * DateTimeUtils.DAY);
        assertEquals(1.0, DateTimeUtils.YEARS_PER_NANO_365 * DateTimeUtils.YEAR_365, 1e-10);
        assertEquals(1.0, DateTimeUtils.YEARS_PER_NANO_AVG * DateTimeUtils.YEAR_AVG, 1e-10);
    }

    public void testParseLocalDate() {
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseLocalDate("2010-01-02"));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseLocalDate("2010-1-02"));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseLocalDate("2010-01-2"));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseLocalDate("2010-1-2"));

        try {
            DateTimeUtils.parseLocalDate("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseLocalDate(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }
    }

    public void testParseLocalDateQuiet() {
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseLocalDateQuiet("2010-01-02"));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseLocalDateQuiet("2010-1-02"));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseLocalDateQuiet("2010-01-2"));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseLocalDateQuiet("2010-1-2"));

        assertNull(DateTimeUtils.parseLocalDateQuiet("JUNK"));
        assertNull(DateTimeUtils.parseLocalDateQuiet(null));
    }

    public void testParseLocalTime() {
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59),
                DateTimeUtils.parseLocalTime("12:59:59"));
        TestCase.assertEquals(java.time.LocalTime.of(0, 0, 0),
                DateTimeUtils.parseLocalTime("00:00:00"));
        TestCase.assertEquals(java.time.LocalTime.of(23, 59, 59),
                DateTimeUtils.parseLocalTime("23:59:59"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 0),
                DateTimeUtils.parseLocalTime("12:59"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000),
                DateTimeUtils.parseLocalTime("12:59:59.123"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000),
                DateTimeUtils.parseLocalTime("12:59:59.123456"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789),
                DateTimeUtils.parseLocalTime("12:59:59.123456789"));

        TestCase.assertEquals(java.time.LocalTime.of(3, 4, 5),
                DateTimeUtils.parseLocalTime("3:4:5"));

        try {
            DateTimeUtils.parseLocalTime("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseLocalTime(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }
    }

    public void testParseLocalTimeQuiet() {
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59),
                DateTimeUtils.parseLocalTimeQuiet("12:59:59"));
        TestCase.assertEquals(java.time.LocalTime.of(0, 0, 0),
                DateTimeUtils.parseLocalTimeQuiet("00:00:00"));
        TestCase.assertEquals(java.time.LocalTime.of(23, 59, 59),
                DateTimeUtils.parseLocalTimeQuiet("23:59:59"));

        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 0),
                DateTimeUtils.parseLocalTimeQuiet("12:59"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_000_000),
                DateTimeUtils.parseLocalTimeQuiet("12:59:59.123"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_000),
                DateTimeUtils.parseLocalTimeQuiet("12:59:59.123456"));
        TestCase.assertEquals(java.time.LocalTime.of(12, 59, 59, 123_456_789),
                DateTimeUtils.parseLocalTimeQuiet("12:59:59.123456789"));

        TestCase.assertEquals(java.time.LocalTime.of(3, 4, 5),
                DateTimeUtils.parseLocalTimeQuiet("3:4:5"));

        TestCase.assertNull(DateTimeUtils.parseLocalTimeQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseLocalTimeQuiet(null));
    }

    public void testParseTimeZoneId() {
        TestCase.assertEquals(ZoneId.of("America/Denver"), DateTimeUtils.parseTimeZone("America/Denver"));
        TestCase.assertEquals(ZoneId.of("America/New_York"), DateTimeUtils.parseTimeZone("NY"));
        TestCase.assertEquals(ZoneId.of("Asia/Yerevan"), DateTimeUtils.parseTimeZone("Asia/Yerevan"));
        TestCase.assertEquals(ZoneId.of("GMT+2"), DateTimeUtils.parseTimeZone("GMT+2"));
        TestCase.assertEquals(ZoneId.of("UTC+01:00"), DateTimeUtils.parseTimeZone("UTC+01:00"));

        try {
            DateTimeUtils.parseTimeZone("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseTimeZone(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }
    }

    public void testParseTimeZoneIdQuiet() {
        TestCase.assertEquals(ZoneId.of("America/Denver"), DateTimeUtils.parseTimeZoneQuiet("America/Denver"));
        TestCase.assertEquals(ZoneId.of("America/New_York"), DateTimeUtils.parseTimeZoneQuiet("NY"));
        TestCase.assertEquals(ZoneId.of("Asia/Yerevan"), DateTimeUtils.parseTimeZoneQuiet("Asia/Yerevan"));
        TestCase.assertEquals(ZoneId.of("GMT+2"), DateTimeUtils.parseTimeZoneQuiet("GMT+2"));
        TestCase.assertEquals(ZoneId.of("UTC+01:00"), DateTimeUtils.parseTimeZoneQuiet("UTC+01:00"));

        TestCase.assertNull(DateTimeUtils.parseTimeZoneQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseTimeZoneQuiet(null));
    }

    public void testParseEpochNanos() {
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

        for (String tz : tzs) {
            for (String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = LocalDateTime.parse(root).atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", DateTimeUtils.epochNanos(zdt.toInstant()),
                        DateTimeUtils.parseEpochNanos(s));
            }
        }

        final String[] uglyRoots = {
                "2023-04-30",
                "2023-04-30T",
                "2023-04-30t",
                "2023-04-30T9:30:00",
                "2023-4-3T9:3:6",
                "2023-4-3T9:3",
                "2023-4-3T9:3:6.1",
                "2023-4-3T9:3:6.123",
                "2023-4-3T9:3:6.123456789",
        };

        final LocalDateTime[] uglyLDTs = {
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 9, 30, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6),
                LocalDateTime.of(2023, 4, 3, 9, 3, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 100_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123456789),
        };

        for (String tz : tzs) {
            for (int i = 0; i < uglyRoots.length; i++) {
                final String root = uglyRoots[i];
                final LocalDateTime ldt = uglyLDTs[i];
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = ldt.atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", DateTimeUtils.epochNanos(zdt),
                        DateTimeUtils.parseEpochNanos(s));
            }
        }

        try {
            DateTimeUtils.parseEpochNanos("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            DateTimeUtils.parseEpochNanos("2010-01-01T12:11");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            DateTimeUtils.parseEpochNanos("2010-01-01T12:11 JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseEpochNanos(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(DateTimeUtils.epochNanos(Instant.parse(iso8601)), DateTimeUtils.parseEpochNanos(iso8601));

        final Instant dt1 = DateTimeUtils.parseInstant("2023-02-02T12:13:14.1345 NY");
        final long nanos = DateTimeUtils.epochNanos(dt1);
        final long micros = DateTimeUtils.epochMicros(dt1);
        final long millis = DateTimeUtils.epochMillis(dt1);
        final long seconds = DateTimeUtils.epochSeconds(dt1);
        TestCase.assertEquals(nanos, DateTimeUtils.parseEpochNanos(Long.toString(nanos)));
        TestCase.assertEquals(micros * 1_000L, DateTimeUtils.parseEpochNanos(Long.toString(micros)));
        TestCase.assertEquals(millis * 1_000_000L, DateTimeUtils.parseEpochNanos(Long.toString(millis)));
        TestCase.assertEquals(seconds * 1_000_000_000L, DateTimeUtils.parseEpochNanos(Long.toString(seconds)));
    }

    public void testParseEpochNanosQuiet() {
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

        for (String tz : tzs) {
            for (String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = LocalDateTime.parse(root).atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", DateTimeUtils.epochNanos(zdt.toInstant()),
                        DateTimeUtils.parseEpochNanosQuiet(s));
            }
        }

        final String[] uglyRoots = {
                "2023-04-30",
                "2023-04-30T",
                "2023-04-30t",
                "2023-04-30T9:30:00",
                "2023-4-3T9:3:6",
                "2023-4-3T9:3",
                "2023-4-3T9:3:6.1",
                "2023-4-3T9:3:6.123",
                "2023-4-3T9:3:6.123456789",
        };

        final LocalDateTime[] uglyLDTs = {
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 9, 30, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6),
                LocalDateTime.of(2023, 4, 3, 9, 3, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 100_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123456789),
        };

        for (String tz : tzs) {
            for (int i = 0; i < uglyRoots.length; i++) {
                final String root = uglyRoots[i];
                final LocalDateTime ldt = uglyLDTs[i];
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = ldt.atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", DateTimeUtils.epochNanos(zdt),
                        DateTimeUtils.parseEpochNanosQuiet(s));
            }
        }

        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseEpochNanosQuiet("JUNK"));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseEpochNanosQuiet("2010-01-01T12:11"));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseEpochNanosQuiet("2010-01-01T12:11 JUNK"));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseEpochNanosQuiet(null));

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(DateTimeUtils.epochNanos(Instant.parse(iso8601)), DateTimeUtils.parseEpochNanosQuiet(iso8601));

        final Instant dt1 = DateTimeUtils.parseInstant("2023-02-02T12:13:14.1345 NY");
        final long nanos = DateTimeUtils.epochNanos(dt1);
        final long micros = DateTimeUtils.epochMicros(dt1);
        final long millis = DateTimeUtils.epochMillis(dt1);
        final long seconds = DateTimeUtils.epochSeconds(dt1);
        TestCase.assertEquals(nanos, DateTimeUtils.parseEpochNanosQuiet(Long.toString(nanos)));
        TestCase.assertEquals(micros * 1_000L, DateTimeUtils.parseEpochNanosQuiet(Long.toString(micros)));
        TestCase.assertEquals(millis * 1_000_000L, DateTimeUtils.parseEpochNanosQuiet(Long.toString(millis)));
        TestCase.assertEquals(seconds * 1_000_000_000L, DateTimeUtils.parseEpochNanosQuiet(Long.toString(seconds)));
    }

    public void testParseInstant() {
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

        for (String tz : tzs) {
            for (String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = LocalDateTime.parse(root).atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", zdt.toInstant(), DateTimeUtils.parseInstant(s));
            }
        }

        final String[] uglyRoots = {
                "2023-04-30",
                "2023-04-30T",
                "2023-04-30t",
                "2023-04-30T9:30:00",
                "2023-4-3T9:3:6",
                "2023-4-3T9:3",
                "2023-4-3T9:3:6.1",
                "2023-4-3T9:3:6.123",
                "2023-4-3T9:3:6.123456789",
        };

        final LocalDateTime[] uglyLDTs = {
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 9, 30, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6),
                LocalDateTime.of(2023, 4, 3, 9, 3, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 100_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123456789),
        };

        for (String tz : tzs) {
            for (int i = 0; i < uglyRoots.length; i++) {
                final String root = uglyRoots[i];
                final LocalDateTime ldt = uglyLDTs[i];
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = ldt.atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", zdt.toInstant(), DateTimeUtils.parseInstant(s));
            }
        }

        try {
            DateTimeUtils.parseInstant("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            DateTimeUtils.parseInstant("2010-01-01T12:11");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            DateTimeUtils.parseInstant("2010-01-01T12:11 JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseInstant(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(Instant.parse(iso8601), DateTimeUtils.parseInstant(iso8601));

        final String isoOffset = "2022-04-26T00:30:31.087360+01:00";
        assertEquals(ZonedDateTime.parse(isoOffset).toInstant(), DateTimeUtils.parseInstant(isoOffset));

        final String isoOffset2 = "2022-11-06T02:59:49.999999999-04:00";
        assertEquals(ZonedDateTime.parse(isoOffset2).toInstant(), DateTimeUtils.parseInstant(isoOffset2));

        final Instant dt1 = DateTimeUtils.parseInstant("2023-02-02T12:13:14.1345 NY");
        final long nanos = DateTimeUtils.epochNanos(dt1);
        final long micros = DateTimeUtils.epochMicros(dt1);
        final long millis = DateTimeUtils.epochMillis(dt1);
        final long seconds = DateTimeUtils.epochSeconds(dt1);
        final Instant dt1u = DateTimeUtils.epochMicrosToInstant(micros);
        final Instant dt1m = DateTimeUtils.epochMillisToInstant(millis);
        final Instant dt1s = DateTimeUtils.epochSecondsToInstant(seconds);
        TestCase.assertEquals(dt1, DateTimeUtils.parseInstant(Long.toString(nanos)));
        TestCase.assertEquals(dt1u, DateTimeUtils.parseInstant(Long.toString(micros)));
        TestCase.assertEquals(dt1m, DateTimeUtils.parseInstant(Long.toString(millis)));
        TestCase.assertEquals(dt1s, DateTimeUtils.parseInstant(Long.toString(seconds)));
    }

    public void testParseInstantQuiet() {
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

        for (String tz : tzs) {
            for (String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = LocalDateTime.parse(root).atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", zdt.toInstant(),
                        DateTimeUtils.parseInstantQuiet(s));
            }
        }

        final String[] uglyRoots = {
                "2023-04-30",
                "2023-04-30T",
                "2023-04-30t",
                "2023-04-30T9:30:00",
                "2023-4-3T9:3:6",
                "2023-4-3T9:3",
                "2023-4-3T9:3:6.1",
                "2023-4-3T9:3:6.123",
                "2023-4-3T9:3:6.123456789",
        };

        final LocalDateTime[] uglyLDTs = {
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 9, 30, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6),
                LocalDateTime.of(2023, 4, 3, 9, 3, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 100_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123456789),
        };

        for (String tz : tzs) {
            for (int i = 0; i < uglyRoots.length; i++) {
                final String root = uglyRoots[i];
                final LocalDateTime ldt = uglyLDTs[i];
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = ldt.atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", zdt.toInstant(),
                        DateTimeUtils.parseInstantQuiet(s));
            }
        }

        TestCase.assertNull(DateTimeUtils.parseInstantQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseInstantQuiet("2010-01-01T12:11"));
        TestCase.assertNull(DateTimeUtils.parseInstantQuiet("2010-01-01T12:11 JUNK"));
        TestCase.assertNull(DateTimeUtils.parseInstantQuiet(null));

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(Instant.parse(iso8601), DateTimeUtils.parseInstantQuiet(iso8601));

        final String isoOffset = "2022-04-26T00:30:31.087360+01:00";
        assertEquals(ZonedDateTime.parse(isoOffset).toInstant(), DateTimeUtils.parseInstantQuiet(isoOffset));

        final String isoOffset2 = "2022-11-06T02:59:49.999999999-04:00";
        assertEquals(ZonedDateTime.parse(isoOffset2).toInstant(), DateTimeUtils.parseInstantQuiet(isoOffset2));

        final Instant dt1 = DateTimeUtils.parseInstant("2023-02-02T12:13:14.1345 NY");
        final long nanos = DateTimeUtils.epochNanos(dt1);
        final long micros = DateTimeUtils.epochMicros(dt1);
        final long millis = DateTimeUtils.epochMillis(dt1);
        final long seconds = DateTimeUtils.epochSeconds(dt1);
        final Instant dt1u = DateTimeUtils.epochMicrosToInstant(micros);
        final Instant dt1m = DateTimeUtils.epochMillisToInstant(millis);
        final Instant dt1s = DateTimeUtils.epochSecondsToInstant(seconds);
        TestCase.assertEquals(dt1, DateTimeUtils.parseInstantQuiet(Long.toString(nanos)));
        TestCase.assertEquals(dt1u, DateTimeUtils.parseInstantQuiet(Long.toString(micros)));
        TestCase.assertEquals(dt1m, DateTimeUtils.parseInstantQuiet(Long.toString(millis)));
        TestCase.assertEquals(dt1s, DateTimeUtils.parseInstantQuiet(Long.toString(seconds)));
    }

    public void testParseLocalDateTime() {
        final String[] roots = {
                "2010-01-01T12:11",
                "2010-01-01T12:00:02",
                "2010-01-01T12:00:00.1",
                "2010-01-01T12:00:00.123",
                "2010-01-01T12:00:00.123",
                "2010-01-01T12:00:00.123456789",
        };

        for (String root : roots) {
            final LocalDateTime ldt = LocalDateTime.parse(root);
            TestCase.assertEquals("LocalDateTime string: " + root, ldt, DateTimeUtils.parseLocalDateTime(root));
        }

        final String[] uglyRoots = {
                "2023-04-30",
                "2023-04-30T",
                "2023-04-30t",
                "2023-04-30T9:30:00",
                "2023-4-3T9:3:6",
                "2023-4-3T9:3",
                "2023-4-3T9:3:6.1",
                "2023-4-3T9:3:6.123",
                "2023-4-3T9:3:6.123456789",
        };

        final LocalDateTime[] uglyLDTs = {
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 9, 30, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6),
                LocalDateTime.of(2023, 4, 3, 9, 3, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 100_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123456789),
        };

        for (int i = 0; i < uglyRoots.length; i++) {
            final String root = uglyRoots[i];
            final LocalDateTime ldt = uglyLDTs[i];
            TestCase.assertEquals("LocalDateTime string: " + root, ldt, DateTimeUtils.parseLocalDateTime(root));
        }

        try {
            DateTimeUtils.parseLocalDateTime("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            DateTimeUtils.parseLocalDateTime("2010-01-01JUNK12:11");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            DateTimeUtils.parseLocalDateTime("2010-01-01T12:11 JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseLocalDateTime(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        final String iso8601 = "2022-04-26T00:30:31.087360";
        assertEquals(LocalDateTime.parse(iso8601), DateTimeUtils.parseLocalDateTime(iso8601));
    }

    public void testParseLocalDateTimeQuiet() {
        final String[] roots = {
                "2010-01-01T12:11",
                "2010-01-01T12:00:02",
                "2010-01-01T12:00:00.1",
                "2010-01-01T12:00:00.123",
                "2010-01-01T12:00:00.123",
                "2010-01-01T12:00:00.123456789",
        };

        for (String root : roots) {
            final LocalDateTime ldt = LocalDateTime.parse(root);
            TestCase.assertEquals("LocalDateTime string: " + root, ldt, DateTimeUtils.parseLocalDateTime(root));
        }

        final String[] uglyRoots = {
                "2023-04-30",
                "2023-04-30T",
                "2023-04-30t",
                "2023-04-30T9:30:00",
                "2023-4-3T9:3:6",
                "2023-4-3T9:3",
                "2023-4-3T9:3:6.1",
                "2023-4-3T9:3:6.123",
                "2023-4-3T9:3:6.123456789",
        };

        final LocalDateTime[] uglyLDTs = {
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 9, 30, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6),
                LocalDateTime.of(2023, 4, 3, 9, 3, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 100_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123456789),
        };

        for (int i = 0; i < uglyRoots.length; i++) {
            final String root = uglyRoots[i];
            final LocalDateTime ldt = uglyLDTs[i];
            TestCase.assertEquals("LocalDateTime string: " + root, ldt, DateTimeUtils.parseLocalDateTime(root));
        }

        TestCase.assertNull(DateTimeUtils.parseLocalDateTimeQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseLocalDateTimeQuiet("2010-01-01JUNK12:11"));
        TestCase.assertNull(DateTimeUtils.parseLocalDateTimeQuiet("2010-01-01T12:11 JUNK"));
        TestCase.assertNull(DateTimeUtils.parseLocalDateTimeQuiet(null));

        final String iso8601 = "2022-04-26T00:30:31.087360";
        assertEquals(LocalDateTime.parse(iso8601), DateTimeUtils.parseLocalDateTime(iso8601));
    }

    public void testParseZonedDateTime() {
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

        for (String tz : tzs) {
            for (String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = LocalDateTime.parse(root).atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", zdt, DateTimeUtils.parseZonedDateTime(s));
            }
        }

        final String[] uglyRoots = {
                "2023-04-30",
                "2023-04-30T",
                "2023-04-30t",
                "2023-04-30T9:30:00",
                "2023-4-3T9:3:6",
                "2023-4-3T9:3",
                "2023-4-3T9:3:6.1",
                "2023-4-3T9:3:6.123",
                "2023-4-3T9:3:6.123456789",
        };

        final LocalDateTime[] uglyLDTs = {
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 9, 30, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6),
                LocalDateTime.of(2023, 4, 3, 9, 3, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 100_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123456789),
        };

        for (String tz : tzs) {
            for (int i = 0; i < uglyRoots.length; i++) {
                final String root = uglyRoots[i];
                final LocalDateTime ldt = uglyLDTs[i];
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = ldt.atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", zdt, DateTimeUtils.parseZonedDateTime(s));
            }
        }

        try {
            DateTimeUtils.parseZonedDateTime("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            DateTimeUtils.parseZonedDateTime("2010-01-01T12:11");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            DateTimeUtils.parseZonedDateTime("2010-01-01T12:11 JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseZonedDateTime(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(ZonedDateTime.parse(iso8601), DateTimeUtils.parseZonedDateTime(iso8601));

        final String isoOffset = "2022-04-26T00:30:31.087360+01:00";
        assertEquals(ZonedDateTime.parse(isoOffset), DateTimeUtils.parseZonedDateTime(isoOffset));

        final String isoOffset2 = "2022-11-06T02:59:49.999999999-04:00";
        assertEquals(ZonedDateTime.parse(isoOffset2), DateTimeUtils.parseZonedDateTime(isoOffset2));
    }

    public void testParseZonedDateTimeQuiet() {
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

        for (String tz : tzs) {
            for (String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = LocalDateTime.parse(root).atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", zdt, DateTimeUtils.parseZonedDateTimeQuiet(s));
            }
        }

        final String[] uglyRoots = {
                "2023-04-30",
                "2023-04-30T",
                "2023-04-30t",
                "2023-04-30T9:30:00",
                "2023-4-3T9:3:6",
                "2023-4-3T9:3",
                "2023-4-3T9:3:6.1",
                "2023-4-3T9:3:6.123",
                "2023-4-3T9:3:6.123456789",
        };

        final LocalDateTime[] uglyLDTs = {
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 0, 0),
                LocalDateTime.of(2023, 4, 30, 9, 30, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6),
                LocalDateTime.of(2023, 4, 3, 9, 3, 0),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 100_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123_000_000),
                LocalDateTime.of(2023, 4, 3, 9, 3, 6, 123456789),
        };

        for (String tz : tzs) {
            for (int i = 0; i < uglyRoots.length; i++) {
                final String root = uglyRoots[i];
                final LocalDateTime ldt = uglyLDTs[i];
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = ldt.atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", zdt, DateTimeUtils.parseZonedDateTimeQuiet(s));
            }
        }

        TestCase.assertNull(DateTimeUtils.parseZonedDateTimeQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseZonedDateTimeQuiet("2010-01-01T12:11"));
        TestCase.assertNull(DateTimeUtils.parseZonedDateTimeQuiet("2010-01-01T12:11 JUNK"));
        TestCase.assertNull(DateTimeUtils.parseZonedDateTimeQuiet(null));

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(ZonedDateTime.parse(iso8601), DateTimeUtils.parseZonedDateTimeQuiet(iso8601));

        final String isoOffset = "2022-04-26T00:30:31.087360+01:00";
        assertEquals(ZonedDateTime.parse(isoOffset), DateTimeUtils.parseZonedDateTimeQuiet(isoOffset));

        final String isoOffset2 = "2022-11-06T02:59:49.999999999-04:00";
        assertEquals(ZonedDateTime.parse(isoOffset2), DateTimeUtils.parseZonedDateTimeQuiet(isoOffset2));
    }

    public void testParseDurationNanos() {
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
                "15:25:49.064106107",
        };

        for (boolean isNegOuter : new boolean[] {false, true}) {
            for (boolean isNegInner : new boolean[] {false, true}) {
                for (String t : times) {
                    long offset = 0;
                    String lts = t;

                    if (lts.indexOf(":") == 1) {
                        lts = "0" + lts;
                    }

                    t = (isNegOuter ? "-" : "") + "PT" + (isNegInner ? "-" : "") + t;

                    final long sign = (isNegOuter ? -1 : 1) * (isNegInner ? -1 : 1);
                    TestCase.assertEquals(sign * (LocalTime.parse(lts).toNanoOfDay() + offset),
                            DateTimeUtils.parseDurationNanos(t));
                }
            }
        }

        TestCase.assertEquals(LocalTime.of(3, 4, 5).toNanoOfDay(), DateTimeUtils.parseDurationNanos("PT3:4:5"));
        TestCase.assertEquals(530000 * DateTimeUtils.HOUR + 59 * DateTimeUtils.MINUTE + 39 * DateTimeUtils.SECOND,
                DateTimeUtils.parseDurationNanos("PT530000:59:39"));

        final String[] durations = {
                "PT1h43s",
                "-PT1h43s",
                "PT2H",
                "PT2H3M",
                "PT3M4S",
                "P2DT3M4S",
                "P2DT3M4.23456789S",
                "-P-2DT-3M-4.23456789S",
        };

        for (String d : durations) {
            final Duration dd = DateTimeUtils.parseDuration(d);
            TestCase.assertEquals(dd.toNanos(), DateTimeUtils.parseDurationNanos(d));
        }

        try {
            DateTimeUtils.parseDurationNanos("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseDurationNanos(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

    }

    public void testParseDurationNanosQuiet() {
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
                "15:25:49.064106107",
        };

        for (boolean isNegOuter : new boolean[] {false, true}) {
            for (boolean isNegInner : new boolean[] {false, true}) {
                for (String t : times) {
                    long offset = 0;
                    String lts = t;

                    if (lts.indexOf(":") == 1) {
                        lts = "0" + lts;
                    }

                    t = (isNegOuter ? "-" : "") + "PT" + (isNegInner ? "-" : "") + t;

                    final long sign = (isNegOuter ? -1 : 1) * (isNegInner ? -1 : 1);
                    TestCase.assertEquals(sign * (LocalTime.parse(lts).toNanoOfDay() + offset),
                            DateTimeUtils.parseDurationNanosQuiet(t));
                }
            }
        }

        TestCase.assertEquals(LocalTime.of(3, 4, 5).toNanoOfDay(), DateTimeUtils.parseDurationNanosQuiet("PT3:4:5"));
        TestCase.assertEquals(530000 * DateTimeUtils.HOUR + 59 * DateTimeUtils.MINUTE + 39 * DateTimeUtils.SECOND,
                DateTimeUtils.parseDurationNanosQuiet("PT530000:59:39"));

        final String[] durations = {
                "PT1h43s",
                "-PT1h43s",
        };

        for (String d : durations) {
            final Duration dd = DateTimeUtils.parseDuration(d);
            TestCase.assertEquals(dd.toNanos(), DateTimeUtils.parseDurationNanos(d));
        }

        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseDurationNanosQuiet("JUNK"));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseDurationNanosQuiet(null));
    }

    public void testParsePeriod() {
        final String[] periods = {
                "P2Y",
                "P3M",
                "P4W",
                "P5D",
                "P1Y2M3D",
                "P1Y2M3W4D",
                "P-1Y2M",
                "-P1Y2M",
                "P2Y5D",
        };

        for (String p : periods) {
            TestCase.assertEquals(Period.parse(p), DateTimeUtils.parsePeriod(p));
        }

        try {
            DateTimeUtils.parsePeriod("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parsePeriod(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }
    }

    public void testParsePeriodQuiet() {
        final String[] periods = {
                "P2Y",
                "P3M",
                "P4W",
                "P5D",
                "P1Y2M3D",
                "P1Y2M3W4D",
                "P-1Y2M",
                "-P1Y2M",
                "P2Y5D",
        };

        for (String p : periods) {
            TestCase.assertEquals(Period.parse(p), DateTimeUtils.parsePeriodQuiet(p));
        }

        TestCase.assertNull(DateTimeUtils.parsePeriodQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parsePeriodQuiet(null));
    }

    public void testParseDuration() {
        final String[] periods = {
                "PT20.345S",
                "PT15M",
                "PT10H",
                "P2D",
                "P2DT3H4M",
                "PT-6H3M",
                "-PT6H3M",
                "-PT-6H+3M",
                "PT1S",
                "PT4H1S",
                "P4DT1S",
        };

        for (String p : periods) {
            TestCase.assertEquals(Duration.parse(p), DateTimeUtils.parseDuration(p));
        }

        final String[][] timeFormats = {
                {"PT12:00", "PT12h"},
                {"PT12:00:00", "PT12h"},
                {"PT12:00:00.123", "PT12h0.123s"},
                {"PT12:00:00.1234", "PT12h0.1234s"},
                {"PT12:00:00.123456789", "PT12h0.123456789s"},
                {"PT2:00", "PT2h"},
                {"PT2:00:00", "PT2h"},
                {"PT2:00:00", "PT2h"},
                {"PT2:00:00.123", "PT2h0.123s"},
                {"PT2:00:00.1234", "PT2h0.1234s"},
                {"PT2:00:00.123456789", "PT2h0.123456789s"},
                {"PT15:25:49.064106107", "PT15h25m49.064106107s"},
        };

        for (String[] tf : timeFormats) {
            TestCase.assertEquals(Duration.parse(tf[1]), DateTimeUtils.parseDuration(tf[0]));
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseDuration(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            // pass
        }
    }

    public void testParseDurationQuiet() {
        final String[] periods = {
                "PT20.345S",
                "PT15M",
                "PT10H",
                "P2D",
                "P2DT3H4M",
                "PT-6H3M",
                "-PT6H3M",
                "-PT-6H+3M",
                "PT1S",
                "PT4H1S",
                "P4DT1S",
        };

        for (String p : periods) {
            TestCase.assertEquals(Duration.parse(p), DateTimeUtils.parseDurationQuiet(p));
        }


        final String[][] timeFormats = {
                {"PT12:00", "PT12h"},
                {"PT12:00:00", "PT12h"},
                {"PT12:00:00.123", "PT12h0.123s"},
                {"PT12:00:00.1234", "PT12h0.1234s"},
                {"PT12:00:00.123456789", "PT12h0.123456789s"},
                {"PT2:00", "PT2h"},
                {"PT2:00:00", "PT2h"},
                {"PT2:00:00", "PT2h"},
                {"PT2:00:00.123", "PT2h0.123s"},
                {"PT2:00:00.1234", "PT2h0.1234s"},
                {"PT2:00:00.123456789", "PT2h0.123456789s"},
                {"PT15:25:49.064106107", "PT15h25m49.064106107s"},
        };

        for (String[] tf : timeFormats) {
            TestCase.assertEquals(Duration.parse(tf[1]), DateTimeUtils.parseDurationQuiet(tf[0]));
        }

        TestCase.assertNull(DateTimeUtils.parseDurationQuiet(null));
        TestCase.assertNull(DateTimeUtils.parseDurationQuiet("JUNK"));
    }

    public void testParseTimePrecision() {
        TestCase.assertEquals(ChronoField.DAY_OF_MONTH, DateTimeUtils.parseTimePrecision("2021-02-03"));
        TestCase.assertEquals(ChronoField.HOUR_OF_DAY, DateTimeUtils.parseTimePrecision("2021-02-03T11"));
        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecision("2021-02-03T11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND,
                DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32.1234"));

        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecision("11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecision("11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.1234"));

        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecision("PT11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecision("PT11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("PT11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("PT11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("PT11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("PT11:14:32.1234"));

        try {
            DateTimeUtils.parseTimePrecision("JUNK");
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            // pass
        }

        try {
            // noinspection ConstantConditions
            DateTimeUtils.parseTimePrecision(null);
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            // pass
        }
    }

    public void testParseTimePrecisionQuiet() {
        TestCase.assertEquals(ChronoField.DAY_OF_MONTH, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03"));
        TestCase.assertEquals(ChronoField.HOUR_OF_DAY, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11"));
        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE,
                DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND,
                DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND,
                DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND,
                DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND,
                DateTimeUtils.parseTimePrecisionQuiet("2021-02-03T11:14:32.1234"));

        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecisionQuiet("11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecisionQuiet("11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("11:14:32.1234"));


        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecisionQuiet("PT11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecisionQuiet("PT11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("PT11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("PT11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("PT11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecisionQuiet("PT11:14:32.1234"));

        TestCase.assertNull(DateTimeUtils.parseTimePrecisionQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseTimePrecisionQuiet(null));
    }

    public void testFormatDate() {
        final Instant dt2 = DateTimeUtils.parseInstant("2021-02-03T11:23:32.456789 NY");
        final ZonedDateTime dt3 = dt2.atZone(TZ_NY);
        final ZonedDateTime dt4 = dt2.atZone(TZ_JP);

        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt2, TZ_NY));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt2, TZ_JP));

        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt3));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt4));

        TestCase.assertNull(DateTimeUtils.formatDate(null, TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDate(dt2, null));

        TestCase.assertNull(DateTimeUtils.formatDate((ZonedDateTime) null));

        final LocalDateTime localDateTime = LocalDateTime.of(2021, 2, 3, 4, 5, 6, 7);
        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(localDateTime));
        TestCase.assertNull(DateTimeUtils.formatDate((LocalDateTime) null));

        final LocalDate localDate = LocalDate.of(2021, 2, 3);
        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(localDate));
        TestCase.assertNull(DateTimeUtils.formatDate((LocalDate) null));
    }

    public void testFormatDateTime() {
        final Instant dt2 = DateTimeUtils.parseInstant("2021-02-03T11:23:32.45678912 NY");
        final ZonedDateTime dt3 = dt2.atZone(TZ_NY);
        final ZonedDateTime dt4 = dt2.atZone(TZ_JP);

        TestCase.assertEquals("2021-02-04T01:00:00.000000000 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:00 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:00.000000000 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:23 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:01.000000000 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:23:01 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:01.300000000 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:23:01.3 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456700000 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:23:32.4567 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456780000 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:23:32.45678 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789000 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:23:32.456789 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789100 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:23:32.4567891 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:23:32.45678912 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789123 JP",
                DateTimeUtils.formatDateTime(DateTimeUtils.parseInstant("2021-02-03T11:23:32.456789123 NY"), TZ_JP));

        TestCase.assertEquals("2021-02-03T11:23:32.456789120 NY", DateTimeUtils.formatDateTime(dt2, TZ_NY));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP", DateTimeUtils.formatDateTime(dt2, TZ_JP));

        TestCase.assertEquals("2021-02-03T11:23:32.456789120 NY", DateTimeUtils.formatDateTime(dt3));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP", DateTimeUtils.formatDateTime(dt4));


        TestCase.assertEquals("2021-02-03T20:23:32.456789120 Asia/Yerevan",
                DateTimeUtils.formatDateTime(dt2, ZoneId.of("Asia/Yerevan")));
        TestCase.assertEquals("2021-02-03T20:23:32.456789120 Asia/Yerevan",
                DateTimeUtils.formatDateTime(dt3.withZoneSameInstant(ZoneId.of("Asia/Yerevan"))));

        TestCase.assertNull(DateTimeUtils.formatDateTime(null, TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDateTime(dt2, null));

        TestCase.assertNull(DateTimeUtils.formatDateTime(null));
    }

    public void testFormatDurationNanos() {

        TestCase.assertEquals("PT2:00:00", DateTimeUtils.formatDurationNanos(2 * DateTimeUtils.HOUR));
        TestCase.assertEquals("PT0:02:00", DateTimeUtils.formatDurationNanos(2 * DateTimeUtils.MINUTE));
        TestCase.assertEquals("PT0:00:02", DateTimeUtils.formatDurationNanos(2 * DateTimeUtils.SECOND));
        TestCase.assertEquals("PT0:00:00.002000000", DateTimeUtils.formatDurationNanos(2 * DateTimeUtils.MILLI));
        TestCase.assertEquals("PT0:00:00.000000002", DateTimeUtils.formatDurationNanos(2));
        TestCase.assertEquals("PT23:45:39.123456789", DateTimeUtils.formatDurationNanos(
                23 * DateTimeUtils.HOUR + 45 * DateTimeUtils.MINUTE + 39 * DateTimeUtils.SECOND + 123456789));
        TestCase.assertEquals("PT123:45:39.123456789", DateTimeUtils.formatDurationNanos(
                123 * DateTimeUtils.HOUR + 45 * DateTimeUtils.MINUTE + 39 * DateTimeUtils.SECOND + 123456789));

        TestCase.assertEquals("-PT2:00:00", DateTimeUtils.formatDurationNanos(-2 * DateTimeUtils.HOUR));
        TestCase.assertEquals("-PT0:02:00", DateTimeUtils.formatDurationNanos(-2 * DateTimeUtils.MINUTE));
        TestCase.assertEquals("-PT0:00:02", DateTimeUtils.formatDurationNanos(-2 * DateTimeUtils.SECOND));
        TestCase.assertEquals("-PT0:00:00.002000000", DateTimeUtils.formatDurationNanos(-2 * DateTimeUtils.MILLI));
        TestCase.assertEquals("-PT0:00:00.000000002", DateTimeUtils.formatDurationNanos(-2));
        TestCase.assertEquals("-PT23:45:39.123456789", DateTimeUtils.formatDurationNanos(
                -23 * DateTimeUtils.HOUR - 45 * DateTimeUtils.MINUTE - 39 * DateTimeUtils.SECOND - 123456789));
        TestCase.assertEquals("-PT123:45:39.123456789", DateTimeUtils.formatDurationNanos(
                -123 * DateTimeUtils.HOUR - 45 * DateTimeUtils.MINUTE - 39 * DateTimeUtils.SECOND - 123456789));

        TestCase.assertNull(DateTimeUtils.formatDurationNanos(NULL_LONG));
    }

    public void testMicrosToMillis() {
        final long v = 1234567890;
        TestCase.assertEquals(v / 1_000L, DateTimeUtils.microsToMillis(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsToMillis(NULL_LONG));
    }

    public void testMicrosToNanos() {
        final long v = 1234567890;
        TestCase.assertEquals(v * 1_000L, DateTimeUtils.microsToNanos(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsToNanos(NULL_LONG));

        try {
            DateTimeUtils.millisToNanos(Long.MAX_VALUE / 2);
            TestCase.fail("Should throw an exception");
        } catch (DateTimeUtils.DateTimeOverflowException ex) {
            // pass
        }

        try {
            DateTimeUtils.microsToNanos(-Long.MAX_VALUE / 2);
            TestCase.fail("Should throw an exception");
        } catch (DateTimeUtils.DateTimeOverflowException ex) {
            // pass
        }
    }

    public void testMicrosToSeconds() {
        final long v = 1234567890;
        TestCase.assertEquals(v / 1_000_000L, DateTimeUtils.microsToSeconds(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsToSeconds(NULL_LONG));
    }

    public void testMillisToMicros() {
        final long v = 1234567890;
        TestCase.assertEquals(v * 1_000L, DateTimeUtils.millisToMicros(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.millisToMicros(NULL_LONG));
    }

    public void testMillisToNanos() {
        final long v = 1234567890;
        TestCase.assertEquals(v * 1_000_000L, DateTimeUtils.millisToNanos(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.millisToNanos(NULL_LONG));

        try {
            DateTimeUtils.millisToNanos(Long.MAX_VALUE / 2);
            TestCase.fail("Should throw an exception");
        } catch (DateTimeUtils.DateTimeOverflowException ex) {
            // pass
        }

        try {
            DateTimeUtils.millisToNanos(-Long.MAX_VALUE / 2);
            TestCase.fail("Should throw an exception");
        } catch (DateTimeUtils.DateTimeOverflowException ex) {
            // pass
        }
    }

    public void testMillisToSeconds() {
        final long v = 1234567890;
        TestCase.assertEquals(v / 1_000L, DateTimeUtils.millisToSeconds(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.millisToSeconds(NULL_LONG));
    }

    public void testNanosToMicros() {
        final long v = 1234567890;
        TestCase.assertEquals(v / 1_000L, DateTimeUtils.nanosToMicros(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosToMicros(NULL_LONG));
    }

    public void testNanosToMillis() {
        final long v = 1234567890;
        TestCase.assertEquals(v / 1_000_000L, DateTimeUtils.nanosToMillis(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosToMillis(NULL_LONG));
    }

    public void testNanosToSeconds() {
        final long v = 1234567890;
        TestCase.assertEquals(v / 1_000_000_000L, DateTimeUtils.nanosToSeconds(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosToSeconds(NULL_LONG));
    }

    public void testSecondsToNanos() {
        final long v = 1234567890;
        TestCase.assertEquals(v * 1_000_000_000L, DateTimeUtils.secondsToNanos(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.secondsToNanos(NULL_LONG));

        try {
            DateTimeUtils.secondsToNanos(Long.MAX_VALUE / 2);
            TestCase.fail("Should throw an exception");
        } catch (DateTimeUtils.DateTimeOverflowException ex) {
            // pass
        }

        try {
            DateTimeUtils.secondsToNanos(-Long.MAX_VALUE / 2);
            TestCase.fail("Should throw an exception");
        } catch (DateTimeUtils.DateTimeOverflowException ex) {
            // pass
        }
    }

    public void testSecondsToMicros() {
        final long v = 1234567890;
        TestCase.assertEquals(v * 1_000_000L, DateTimeUtils.secondsToMicros(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.secondsToMicros(NULL_LONG));
    }

    public void testSecondsToMillis() {
        final long v = 1234567890;
        TestCase.assertEquals(v * 1_000L, DateTimeUtils.secondsToMillis(v));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.secondsToMillis(NULL_LONG));
    }

    public void testToDate() {
        final long millis = 123456789;
        final Instant dt2 = Instant.ofEpochSecond(0, millis * DateTimeUtils.MILLI);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(new Date(millis), DateTimeUtils.toDate(dt2));
        TestCase.assertNull(DateTimeUtils.toDate((Instant) null));

        TestCase.assertEquals(new Date(millis), DateTimeUtils.toDate(dt3));
        TestCase.assertNull(DateTimeUtils.toDate((ZonedDateTime) null));
    }

    public void testToInstant() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDate ld = LocalDate.of(1973, 11, 30);
        final LocalTime lt = LocalTime.of(6, 33, 9, 123456789);
        final LocalDateTime ldt = LocalDateTime.of(ld, lt);
        final Date d = new Date(DateTimeUtils.nanosToMillis(nanos));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(dt3));
        TestCase.assertNull(DateTimeUtils.toInstant((ZonedDateTime) null));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(ldt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(null, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(ldt, null));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(ld, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(null, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(ld, null, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(ld, lt, null));

        TestCase.assertEquals(Instant.ofEpochSecond(0, (nanos / DateTimeUtils.MILLI) * DateTimeUtils.MILLI),
                DateTimeUtils.toInstant(d));
        TestCase.assertNull(DateTimeUtils.toInstant((Date) null));
    }

    public void testToLocalDateTime() {
        final long nanos = 123456789123456789L;
        final Instant instant = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime zdt = instant.atZone(TZ_JP);
        final LocalDate ld = LocalDate.of(1973, 11, 30);
        final LocalTime lt = LocalTime.of(6, 33, 9, 123456789);
        final LocalDateTime ldt = LocalDateTime.of(ld, lt);

        TestCase.assertEquals(ldt, DateTimeUtils.toLocalDateTime(instant, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalDateTime(null, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalDateTime(instant, null));

        TestCase.assertEquals(ldt, DateTimeUtils.toLocalDateTime(zdt));
        TestCase.assertNull(DateTimeUtils.toLocalDateTime(null));

        TestCase.assertEquals(ldt, DateTimeUtils.toLocalDateTime(ld, lt));
        TestCase.assertNull(DateTimeUtils.toLocalDateTime(null, lt));
        TestCase.assertNull(DateTimeUtils.toLocalDateTime(ld, null));
    }

    public void testToLocalDate() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDate ld = LocalDate.of(1973, 11, 30);
        final LocalDateTime ldt = LocalDateTime.of(ld, LocalTime.now());

        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt2, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalDate(null, TZ_JP));

        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt3));
        // noinspection ConstantConditions
        TestCase.assertNull(DateTimeUtils.toLocalDate((ZonedDateTime) null));

        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(ldt));
        // noinspection ConstantConditions
        TestCase.assertNull(DateTimeUtils.toLocalDate((LocalDateTime) null));
    }

    public void testToLocalTime() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalTime lt = LocalTime.of(6, 33, 9, 123456789);
        final LocalDateTime ldt = LocalDateTime.of(LocalDate.now(), lt);

        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt2, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalTime(null, TZ_JP));

        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt3));
        // noinspection ConstantConditions
        TestCase.assertNull(DateTimeUtils.toLocalTime((ZonedDateTime) null));

        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(ldt));
        // noinspection ConstantConditions
        TestCase.assertNull(DateTimeUtils.toLocalTime((LocalDateTime) null));

        final LocalTime someTimeInMillis = LocalTime.of(6, 33, 9, (int) (123 * DateTimeUtils.MILLI));
        TestCase.assertEquals(someTimeInMillis,
                DateTimeUtils.millisOfDayToLocalTime((int) (someTimeInMillis.toNanoOfDay() / DateTimeUtils.MILLI)));
        TestCase.assertNull(DateTimeUtils.millisOfDayToLocalTime(NULL_INT));

        final LocalTime someTimeInMicros = LocalTime.of(6, 33, 9, (int) (123456 * DateTimeUtils.MICRO));
        TestCase.assertEquals(someTimeInMicros,
                DateTimeUtils.microsOfDayToLocalTime(someTimeInMicros.toNanoOfDay() / DateTimeUtils.MICRO));
        TestCase.assertNull(DateTimeUtils.microsOfDayToLocalTime(NULL_LONG));

        TestCase.assertEquals(lt, DateTimeUtils.nanosOfDayToLocalTime(lt.toNanoOfDay()));
        TestCase.assertNull(DateTimeUtils.nanosOfDayToLocalTime(NULL_LONG));
    }

    public void testToZonedDateTime() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDate ld = LocalDate.of(1973, 11, 30);
        final LocalTime lt = LocalTime.of(6, 33, 9, 123456789);
        final LocalDateTime ldt = LocalDateTime.of(ld, lt);

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(dt2, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((Instant) null, TZ_JP));

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(ldt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((LocalDateTime) null, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ldt, null));

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(ld, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(null, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ld, null, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ld, lt, null));
    }

    public void testEpochNanos() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(nanos, DateTimeUtils.epochNanos(dt2));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochNanos((Instant) null));

        TestCase.assertEquals(nanos, DateTimeUtils.epochNanos(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochNanos((ZonedDateTime) null));
    }

    public void testEpochMicros() {
        final long nanos = 123456789123456789L;
        final long micros = DateTimeUtils.nanosToMicros(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(micros, DateTimeUtils.epochMicros(dt2));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMicros((Instant) null));

        TestCase.assertEquals(micros, DateTimeUtils.epochMicros(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMicros((ZonedDateTime) null));
    }

    public void testEpochMillis() {
        final long nanos = 123456789123456789L;
        final long millis = DateTimeUtils.nanosToMillis(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(millis, DateTimeUtils.epochMillis(dt2));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMillis((Instant) null));

        TestCase.assertEquals(millis, DateTimeUtils.epochMillis(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochMillis((ZonedDateTime) null));
    }

    public void testEpochSeconds() {
        final long nanos = 123456789123456789L;
        final long seconds = DateTimeUtils.nanosToSeconds(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(seconds, DateTimeUtils.epochSeconds(dt2));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochSeconds((Instant) null));

        TestCase.assertEquals(seconds, DateTimeUtils.epochSeconds(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochSeconds((ZonedDateTime) null));
    }

    public void testEpochDays() {
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochDays((LocalDate) null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.epochDaysAsInt((LocalDate) null));

        final LocalDate OneYearFromEpoch = LocalDate.of(1971, 1, 1);
        TestCase.assertEquals(365, DateTimeUtils.epochDays(OneYearFromEpoch));
        TestCase.assertEquals(365, DateTimeUtils.epochDaysAsInt(OneYearFromEpoch));

        final LocalDate today = LocalDate.now();
        TestCase.assertEquals(today.toEpochDay(), DateTimeUtils.epochDays(today));
        TestCase.assertEquals((int) today.toEpochDay(), DateTimeUtils.epochDaysAsInt(today));
    }

    public void testEpochNanosTo() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(dt2, DateTimeUtils.epochNanosToInstant(nanos));
        TestCase.assertNull(DateTimeUtils.epochNanosToInstant(NULL_LONG));

        TestCase.assertEquals(dt3, DateTimeUtils.epochNanosToZonedDateTime(nanos, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochNanosToZonedDateTime(NULL_LONG, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochNanosToZonedDateTime(nanos, null));
    }

    public void testEpochMicrosTo() {
        long nanos = 123456789123456789L;
        final long micros = DateTimeUtils.nanosToMicros(nanos);
        nanos = DateTimeUtils.microsToNanos(micros);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(dt2, DateTimeUtils.epochMicrosToInstant(micros));
        TestCase.assertNull(DateTimeUtils.epochMicrosToInstant(NULL_LONG));

        TestCase.assertEquals(dt3, DateTimeUtils.epochMicrosToZonedDateTime(micros, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochMicrosToZonedDateTime(NULL_LONG, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochMicrosToZonedDateTime(micros, null));
    }

    public void testEpochMillisTo() {
        long nanos = 123456789123456789L;
        final long millis = DateTimeUtils.nanosToMillis(nanos);
        nanos = DateTimeUtils.millisToNanos(millis);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(dt2, DateTimeUtils.epochMillisToInstant(millis));
        TestCase.assertNull(DateTimeUtils.epochMillisToInstant(NULL_LONG));

        TestCase.assertEquals(dt3, DateTimeUtils.epochMillisToZonedDateTime(millis, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochMillisToZonedDateTime(NULL_LONG, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochMillisToZonedDateTime(millis, null));
    }

    public void testEpochSecondsTo() {
        long nanos = 123456789123456789L;
        final long seconds = DateTimeUtils.nanosToSeconds(nanos);
        nanos = DateTimeUtils.secondsToNanos(seconds);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(dt2, DateTimeUtils.epochSecondsToInstant(seconds));
        TestCase.assertNull(DateTimeUtils.epochSecondsToInstant(NULL_LONG));

        TestCase.assertEquals(dt3, DateTimeUtils.epochSecondsToZonedDateTime(seconds, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochSecondsToZonedDateTime(NULL_LONG, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochSecondsToZonedDateTime(seconds, null));
    }

    public void testEpochAutoTo() {
        final Instant dt1 = DateTimeUtils.parseInstant("2023-02-02T12:13:14.1345 NY");
        final long nanos = DateTimeUtils.epochNanos(dt1);
        final long micros = DateTimeUtils.epochMicros(dt1);
        final long millis = DateTimeUtils.epochMillis(dt1);
        final long seconds = DateTimeUtils.epochSeconds(dt1);
        final Instant dt1u = DateTimeUtils.epochMicrosToInstant(micros);
        final Instant dt1m = DateTimeUtils.epochMillisToInstant(millis);
        final Instant dt1s = DateTimeUtils.epochSecondsToInstant(seconds);

        TestCase.assertEquals(nanos, DateTimeUtils.epochAutoToEpochNanos(nanos));
        TestCase.assertEquals((nanos / DateTimeUtils.MICRO) * DateTimeUtils.MICRO,
                DateTimeUtils.epochAutoToEpochNanos(micros));
        TestCase.assertEquals((nanos / DateTimeUtils.MILLI) * DateTimeUtils.MILLI,
                DateTimeUtils.epochAutoToEpochNanos(millis));
        TestCase.assertEquals((nanos / DateTimeUtils.SECOND) * DateTimeUtils.SECOND,
                DateTimeUtils.epochAutoToEpochNanos(seconds));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.epochAutoToEpochNanos(NULL_LONG));

        TestCase.assertEquals(dt1, DateTimeUtils.epochAutoToInstant(nanos));
        TestCase.assertEquals(dt1u, DateTimeUtils.epochAutoToInstant(micros));
        TestCase.assertEquals(dt1m, DateTimeUtils.epochAutoToInstant(millis));
        TestCase.assertEquals(dt1s, DateTimeUtils.epochAutoToInstant(seconds));
        TestCase.assertNull(DateTimeUtils.epochAutoToInstant(NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1, TZ_JP),
                DateTimeUtils.epochAutoToZonedDateTime(nanos, TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1u, TZ_JP),
                DateTimeUtils.epochAutoToZonedDateTime(micros, TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1m, TZ_JP),
                DateTimeUtils.epochAutoToZonedDateTime(millis, TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1s, TZ_JP),
                DateTimeUtils.epochAutoToZonedDateTime(seconds, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochAutoToZonedDateTime(NULL_LONG, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochAutoToZonedDateTime(nanos, null));
    }

    public void testEpochDaysTo() {
        TestCase.assertNull(DateTimeUtils.epochDaysAsIntToLocalDate(NULL_INT));
        TestCase.assertNull(DateTimeUtils.epochDaysToLocalDate(NULL_LONG));

        final long numDaysInYear = 365;
        final LocalDate OneYearFromEpoch = LocalDate.of(1971, 1, 1);
        TestCase.assertEquals(OneYearFromEpoch, DateTimeUtils.epochDaysToLocalDate(numDaysInYear));
        TestCase.assertEquals(OneYearFromEpoch, DateTimeUtils.epochDaysAsIntToLocalDate((int) numDaysInYear));

        final LocalDate today = LocalDate.now();
        TestCase.assertEquals(today, DateTimeUtils.epochDaysToLocalDate(today.toEpochDay()));
        TestCase.assertEquals(today, DateTimeUtils.epochDaysAsIntToLocalDate((int) today.toEpochDay()));
    }

    public void testToExcelTime() {
        final Instant dt2 = DateTimeUtils.parseInstant("2010-06-15T16:00:00 NY");
        final ZonedDateTime dt3 = dt2.atZone(TZ_AL);

        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.666666666664, DateTimeUtils.toExcelTime(dt2, TZ_NY)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt2, TZ_CT)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt2, TZ_MN)));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(null, TZ_MN));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(dt2, null));

        TestCase.assertTrue(
                CompareUtils.doubleEquals(DateTimeUtils.toExcelTime(dt2, TZ_AL), DateTimeUtils.toExcelTime(dt3)));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(null));
    }

    public void testExcelTimeTo() {
        final Instant dt2 = DateTimeUtils.parseInstant("2010-06-15T16:23:45.678 NY");
        final ZonedDateTime dt3 = dt2.atZone(TZ_AL);

        TestCase.assertEquals(dt2, DateTimeUtils.excelToInstant(DateTimeUtils.toExcelTime(dt2, TZ_AL), TZ_AL));
        TestCase.assertTrue(DateTimeUtils.epochMillis(dt2) - DateTimeUtils
                .epochMillis(DateTimeUtils.excelToInstant(DateTimeUtils.toExcelTime(dt2, TZ_AL), TZ_AL)) <= 1);
        TestCase.assertNull(DateTimeUtils.excelToInstant(123.4, null));

        TestCase.assertEquals(dt3, DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dt2, TZ_AL), TZ_AL));
        TestCase.assertTrue(DateTimeUtils.epochMillis(dt3) - DateTimeUtils
                .epochMillis(DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dt2, TZ_AL), TZ_AL)) <= 1);
        TestCase.assertNull(DateTimeUtils.excelToZonedDateTime(123.4, null));

        // Test daylight savings time

        final Instant dstI1 = DateTimeUtils.parseInstant("2023-03-12T01:00:00 America/Denver");
        final Instant dstI2 = DateTimeUtils.parseInstant("2023-11-05T01:00:00 America/Denver");
        final ZonedDateTime dstZdt1 = DateTimeUtils.toZonedDateTime(dstI1, ZoneId.of("America/Denver"));
        final ZonedDateTime dstZdt2 = DateTimeUtils.toZonedDateTime(dstI2, ZoneId.of("America/Denver"));

        TestCase.assertEquals(dstI1, DateTimeUtils.excelToInstant(
                DateTimeUtils.toExcelTime(dstI1, ZoneId.of("America/Denver")), ZoneId.of("America/Denver")));
        TestCase.assertEquals(dstI2, DateTimeUtils.excelToInstant(
                DateTimeUtils.toExcelTime(dstI2, ZoneId.of("America/Denver")), ZoneId.of("America/Denver")));

        TestCase.assertEquals(dstZdt1,
                DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dstZdt1), ZoneId.of("America/Denver")));
        TestCase.assertEquals(dstZdt2,
                DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dstZdt2), ZoneId.of("America/Denver")));
    }

    public void testIsBefore() {
        final Instant i1 = Instant.ofEpochSecond(0, 123);
        final Instant i2 = Instant.ofEpochSecond(0, 456);
        final Instant i3 = Instant.ofEpochSecond(0, 456);

        TestCase.assertTrue(DateTimeUtils.isBefore(i1, i2));
        TestCase.assertFalse(DateTimeUtils.isBefore(i2, i1));
        TestCase.assertFalse(DateTimeUtils.isBefore(i2, i3));
        TestCase.assertFalse(DateTimeUtils.isBefore(i3, i2));
        // noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isBefore(null, i2));
        // noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isBefore((Instant) null, null));
        // noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isBefore(i1, null));

        final ZonedDateTime z1 = i1.atZone(TZ_AL);
        final ZonedDateTime z2 = i2.atZone(TZ_AL);
        final ZonedDateTime z3 = i3.atZone(TZ_AL);

        TestCase.assertTrue(DateTimeUtils.isBefore(z1, z2));
        TestCase.assertFalse(DateTimeUtils.isBefore(z2, z1));
        TestCase.assertFalse(DateTimeUtils.isBefore(z2, z3));
        TestCase.assertFalse(DateTimeUtils.isBefore(z3, z2));
        TestCase.assertFalse(DateTimeUtils.isBefore(null, z2));
        // noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isBefore((Instant) null, null));
        TestCase.assertFalse(DateTimeUtils.isBefore(z1, null));
    }

    public void testIsBeforeOrEqual() {
        final Instant i1 = Instant.ofEpochSecond(0, 123);
        final Instant i2 = Instant.ofEpochSecond(0, 456);
        final Instant i3 = Instant.ofEpochSecond(0, 456);

        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(i1, i2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(i2, i1));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(i2, i3));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(i3, i2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(null, i2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual((Instant) null, null));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(i1, null));

        final ZonedDateTime z1 = i1.atZone(TZ_AL);
        final ZonedDateTime z2 = i2.atZone(TZ_AL);
        final ZonedDateTime z3 = i3.atZone(TZ_AL);

        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(z1, z2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(z2, z1));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(z2, z3));
        TestCase.assertTrue(DateTimeUtils.isBeforeOrEqual(z3, z2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(null, z2));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual((Instant) null, null));
        TestCase.assertFalse(DateTimeUtils.isBeforeOrEqual(z1, null));
    }

    public void testIsAfter() {
        final Instant i1 = Instant.ofEpochSecond(0, 123);
        final Instant i2 = Instant.ofEpochSecond(0, 456);
        final Instant i3 = Instant.ofEpochSecond(0, 456);

        TestCase.assertFalse(DateTimeUtils.isAfter(i1, i2));
        TestCase.assertTrue(DateTimeUtils.isAfter(i2, i1));
        TestCase.assertFalse(DateTimeUtils.isAfter(i2, i3));
        TestCase.assertFalse(DateTimeUtils.isAfter(i3, i2));
        // noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isAfter(null, i2));
        // noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isAfter((Instant) null, null));
        // noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isAfter(i1, null));

        final ZonedDateTime z1 = i1.atZone(TZ_AL);
        final ZonedDateTime z2 = i2.atZone(TZ_AL);
        final ZonedDateTime z3 = i3.atZone(TZ_AL);

        TestCase.assertFalse(DateTimeUtils.isAfter(z1, z2));
        TestCase.assertTrue(DateTimeUtils.isAfter(z2, z1));
        TestCase.assertFalse(DateTimeUtils.isAfter(z2, z3));
        TestCase.assertFalse(DateTimeUtils.isAfter(z3, z2));
        TestCase.assertFalse(DateTimeUtils.isAfter(null, z2));
        // noinspection ConstantConditions
        TestCase.assertFalse(DateTimeUtils.isAfter((Instant) null, null));
        TestCase.assertFalse(DateTimeUtils.isAfter(z1, null));
    }

    public void testIsAfterOrEqual() {
        final Instant i1 = Instant.ofEpochSecond(0, 123);
        final Instant i2 = Instant.ofEpochSecond(0, 456);
        final Instant i3 = Instant.ofEpochSecond(0, 456);

        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(i1, i2));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(i2, i1));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(i2, i3));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(i3, i2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(null, i2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual((Instant) null, null));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(i1, null));

        final ZonedDateTime z1 = i1.atZone(TZ_AL);
        final ZonedDateTime z2 = i2.atZone(TZ_AL);
        final ZonedDateTime z3 = i3.atZone(TZ_AL);

        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(z1, z2));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(z2, z1));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(z2, z3));
        TestCase.assertTrue(DateTimeUtils.isAfterOrEqual(z3, z2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(null, z2));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual((Instant) null, null));
        TestCase.assertFalse(DateTimeUtils.isAfterOrEqual(z1, null));
    }

    public void testClock() {
        final long nanos = 123456789123456789L;
        @NotNull
        final Clock initial = DateTimeUtils.currentClock();

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
                    return Instant.ofEpochSecond(0, nanos);
                }

                @Override
                public Instant instantMillis() {
                    return Instant.ofEpochMilli(nanos / DateTimeUtils.MILLI);
                }
            };
            DateTimeUtils.setClock(clock);
            TestCase.assertEquals(clock, DateTimeUtils.currentClock());

            TestCase.assertEquals(Instant.ofEpochSecond(0, nanos), DateTimeUtils.now());
            TestCase.assertEquals(Instant.ofEpochSecond(0, (nanos / DateTimeUtils.MILLI) * DateTimeUtils.MILLI),
                    DateTimeUtils.nowMillisResolution());

            // Occasionally tests fail because of invalid clocks on the test system
            final LocalDate startDate = LocalDate.of(1990, 1, 1);
            final LocalDate currentDate = DateTimeUtils.toLocalDate(
                    DateTimeUtils.epochNanosToInstant(Clock.system().currentTimeNanos()), DateTimeUtils.timeZone());
            assertTrue("Checking for a valid date on the test system: currentDate=" + currentDate,
                    currentDate.isAfter(startDate));

            TestCase.assertTrue(Math.abs(Clock.system().currentTimeNanos()
                    - DateTimeUtils.epochNanos(DateTimeUtils.nowSystem())) < 1_000_000L);
            TestCase.assertTrue(Math.abs(Clock.system().currentTimeNanos()
                    - DateTimeUtils.epochNanos(DateTimeUtils.nowSystemMillisResolution())) < 1_000_000L);

            TestCase.assertEquals(DateTimeUtils.formatDate(Instant.ofEpochSecond(0, nanos), TZ_AL),
                    DateTimeUtils.today(TZ_AL));
            TestCase.assertEquals(DateTimeUtils.today(DateTimeUtils.timeZone()), DateTimeUtils.today());

            TestCase.assertEquals(DateTimeUtils.toLocalDate(Instant.ofEpochSecond(0, nanos), TZ_AL),
                    DateTimeUtils.todayLocalDate(TZ_AL));
            TestCase.assertEquals(DateTimeUtils.todayLocalDate(DateTimeUtils.timeZone()),
                    DateTimeUtils.todayLocalDate());
        } catch (Exception ex) {
            DateTimeUtils.setClock(initial);
            throw ex;
        }

        assertNull(DateTimeUtils.today(null));
        assertNull(DateTimeUtils.todayLocalDate(null));

        DateTimeUtils.setClock(initial);
    }

    public void testTimeZone() {
        final String[][] values = {
                {"NY", "America/New_York"},
                {"MN", "America/Chicago"},
                {"JP", "Asia/Tokyo"},
                {"SG", "Asia/Singapore"},
                {"UTC", "UTC"},
                {"America/Argentina/Buenos_Aires", "America/Argentina/Buenos_Aires"},
                {"GMT+2", "GMT+2"},
                {"UTC+01:00", "UTC+01:00"}
        };

        for (final String[] v : values) {
            TestCase.assertEquals(TimeZoneAliases.zoneId(v[0]), DateTimeUtils.timeZone(v[0]));
            TestCase.assertEquals(TimeZoneAliases.zoneId(v[1]), DateTimeUtils.timeZone(v[1]));
        }

        TestCase.assertEquals(ZoneId.systemDefault(), DateTimeUtils.timeZone());
        TestCase.assertNull(DateTimeUtils.timeZone(null));
    }

    public void testTimeZoneAliasAddRm() {
        final String alias = "BA";
        final String tz = "America/Argentina/Buenos_Aires";
        TestCase.assertFalse(DateTimeUtils.timeZoneAliasRm(alias));
        TestCase.assertFalse(TimeZoneAliases.getAllZones().containsKey(alias));
        DateTimeUtils.timeZoneAliasAdd(alias, tz);
        TestCase.assertTrue(TimeZoneAliases.getAllZones().containsKey(alias));
        TestCase.assertEquals(ZoneId.of(tz), TimeZoneAliases.zoneId(alias));
        TestCase.assertEquals(alias, TimeZoneAliases.zoneName(ZoneId.of(tz)));
        TestCase.assertTrue(DateTimeUtils.timeZoneAliasRm(alias));
        TestCase.assertFalse(TimeZoneAliases.getAllZones().containsKey(alias));
    }

    public void testLowerBin() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;

        final Instant instant = DateTimeUtils.parseInstant("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtils.lowerBin(instant, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(instant, second), second));

        TestCase.assertEquals(DateTimeUtils.parseInstant("2010-06-15T06:14:01 NY"),
                DateTimeUtils.lowerBin(instant, second));
        TestCase.assertEquals(DateTimeUtils.parseInstant("2010-06-15T06:10:00 NY"),
                DateTimeUtils.lowerBin(instant, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.parseInstant("2010-06-15T06:00:00 NY"),
                DateTimeUtils.lowerBin(instant, hour));
        TestCase.assertNull(DateTimeUtils.lowerBin((Instant) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(instant, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.lowerBin(instant, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(instant, second), second));

        TestCase.assertEquals(DateTimeUtils.lowerBin(instant, Duration.ofMinutes(1)),
                DateTimeUtils.lowerBin(instant, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin((Instant) null, Duration.ofMinutes(1)));
        TestCase.assertNull(DateTimeUtils.lowerBin(instant, (Duration) null));

        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(instant, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.lowerBin(instant, second), TZ_AL),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(zdt, second), second));

        TestCase.assertEquals(DateTimeUtils.parseZonedDateTime("2010-06-15T06:14:01 NY").withZoneSameInstant(TZ_AL),
                DateTimeUtils.lowerBin(zdt, second));
        TestCase.assertEquals(DateTimeUtils.parseZonedDateTime("2010-06-15T06:10:00 NY").withZoneSameInstant(TZ_AL),
                DateTimeUtils.lowerBin(zdt, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.parseZonedDateTime("2010-06-15T06:00:00 NY").withZoneSameInstant(TZ_AL),
                DateTimeUtils.lowerBin(zdt, hour));
        TestCase.assertNull(DateTimeUtils.lowerBin((ZonedDateTime) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(zdt, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.lowerBin(zdt, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(zdt, second), second));

        TestCase.assertEquals(DateTimeUtils.lowerBin(zdt, Duration.ofMinutes(1)),
                DateTimeUtils.lowerBin(zdt, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin((ZonedDateTime) null, Duration.ofMinutes(1)));
        TestCase.assertNull(DateTimeUtils.lowerBin(zdt, (Duration) null));
    }

    public void testLowerBinWithOffset() {
        final long second = 1000000000L;
        final long minute = 60 * second;

        final Instant instant = DateTimeUtils.parseInstant("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtils.parseInstant("2010-06-15T06:11:00 NY"),
                DateTimeUtils.lowerBin(instant, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin((Instant) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(instant, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(instant, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.lowerBin(instant, second, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(instant, second, second), second, second));

        TestCase.assertEquals(DateTimeUtils.lowerBin(instant, Duration.ofMinutes(1), Duration.ofSeconds(2)),
                DateTimeUtils.lowerBin(instant, minute, 2 * second));
        TestCase.assertNull(DateTimeUtils.lowerBin((Instant) null, Duration.ofMinutes(1), Duration.ofSeconds(2)));
        TestCase.assertNull(DateTimeUtils.lowerBin(instant, (Duration) null, Duration.ofSeconds(2)));
        TestCase.assertNull(DateTimeUtils.lowerBin(instant, Duration.ofMinutes(1), (Duration) null));

        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(instant, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.parseZonedDateTime("2010-06-15T06:11:00 NY").withZoneSameInstant(TZ_AL),
                DateTimeUtils.lowerBin(zdt, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin((ZonedDateTime) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(zdt, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(zdt, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.lowerBin(zdt, second, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(zdt, second, second), second, second));

        TestCase.assertEquals(DateTimeUtils.lowerBin(zdt, Duration.ofMinutes(1), Duration.ofSeconds(2)),
                DateTimeUtils.lowerBin(zdt, minute, 2 * second));
        TestCase.assertNull(DateTimeUtils.lowerBin((ZonedDateTime) null, Duration.ofMinutes(1), Duration.ofSeconds(2)));
        TestCase.assertNull(DateTimeUtils.lowerBin(zdt, (Duration) null, Duration.ofSeconds(2)));
        TestCase.assertNull(DateTimeUtils.lowerBin(zdt, Duration.ofMinutes(1), (Duration) null));
    }

    public void testUpperBin() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;

        final Instant instant = DateTimeUtils.parseInstant("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtils.parseInstant("2010-06-15T06:14:02 NY"),
                DateTimeUtils.upperBin(instant, second));
        TestCase.assertEquals(DateTimeUtils.parseInstant("2010-06-15T06:15:00 NY"),
                DateTimeUtils.upperBin(instant, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.parseInstant("2010-06-15T07:00:00 NY"),
                DateTimeUtils.upperBin(instant, hour));
        TestCase.assertNull(DateTimeUtils.upperBin((Instant) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.upperBin(instant, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(instant, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(instant, second), second));

        TestCase.assertEquals(DateTimeUtils.upperBin(instant, Duration.ofMinutes(1)),
                DateTimeUtils.upperBin(instant, minute));
        TestCase.assertNull(DateTimeUtils.upperBin((Instant) null, Duration.ofMinutes(1)));
        TestCase.assertNull(DateTimeUtils.upperBin(instant, (Duration) null));

        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(instant, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.parseZonedDateTime("2010-06-15T06:14:02 NY").withZoneSameInstant(TZ_AL),
                DateTimeUtils.upperBin(zdt, second));
        TestCase.assertEquals(DateTimeUtils.parseZonedDateTime("2010-06-15T06:15:00 NY").withZoneSameInstant(TZ_AL),
                DateTimeUtils.upperBin(zdt, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.parseZonedDateTime("2010-06-15T07:00:00 NY").withZoneSameInstant(TZ_AL),
                DateTimeUtils.upperBin(zdt, hour));
        TestCase.assertNull(DateTimeUtils.upperBin((ZonedDateTime) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.upperBin(zdt, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(zdt, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(zdt, second), second));

        TestCase.assertEquals(DateTimeUtils.upperBin(zdt, Duration.ofMinutes(1)),
                DateTimeUtils.upperBin(zdt, minute));
        TestCase.assertNull(DateTimeUtils.upperBin((ZonedDateTime) null, Duration.ofMinutes(1)));
        TestCase.assertNull(DateTimeUtils.upperBin(zdt, (Duration) null));
    }

    public void testUpperBinWithOffset() {
        final long second = 1000000000L;
        final long minute = 60 * second;

        final Instant instant = DateTimeUtils.parseInstant("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtils.parseInstant("2010-06-15T06:16:00 NY"),
                DateTimeUtils.upperBin(instant, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin((Instant) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(instant, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(instant, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(instant, second, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(instant, second, second), second, second));

        TestCase.assertEquals(DateTimeUtils.upperBin(instant, Duration.ofMinutes(1), Duration.ofSeconds(2)),
                DateTimeUtils.upperBin(instant, minute, 2 * second));
        TestCase.assertNull(DateTimeUtils.upperBin((Instant) null, Duration.ofMinutes(1), Duration.ofSeconds(2)));
        TestCase.assertNull(DateTimeUtils.upperBin(instant, null, Duration.ofSeconds(2)));
        TestCase.assertNull(DateTimeUtils.upperBin(instant, Duration.ofMinutes(1), null));

        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(instant, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.parseZonedDateTime("2010-06-15T06:16:00 NY").withZoneSameInstant(TZ_AL),
                DateTimeUtils.upperBin(zdt, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin((ZonedDateTime) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(zdt, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(zdt, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(zdt, second, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(zdt, second, second), second, second));

        TestCase.assertEquals(DateTimeUtils.upperBin(zdt, Duration.ofMinutes(1), Duration.ofSeconds(2)),
                DateTimeUtils.upperBin(zdt, minute, 2 * second));
        TestCase.assertNull(DateTimeUtils.upperBin((ZonedDateTime) null, Duration.ofMinutes(1), Duration.ofSeconds(2)));
        TestCase.assertNull(DateTimeUtils.upperBin(zdt, null, Duration.ofSeconds(2)));
        TestCase.assertNull(DateTimeUtils.upperBin(zdt, Duration.ofMinutes(1), null));
    }

    public void testPlusLocalDateTime() {
        final LocalDateTime ldt = LocalDateTime.of(2010, 1, 2, 3, 4, 5, 6);
        TestCase.assertEquals(LocalDateTime.of(2010, 1, 5, 3, 4, 5, 6), DateTimeUtils.plusDays(ldt, 3));
        TestCase.assertEquals(LocalDateTime.of(2009, 12, 30, 3, 4, 5, 6), DateTimeUtils.plusDays(ldt, -3));
        TestCase.assertEquals(LocalDateTime.of(2010, 1, 5, 3, 4, 5, 6), DateTimeUtils.plus(ldt, Period.ofDays(3)));
        TestCase.assertEquals(LocalDateTime.of(2009, 12, 30, 3, 4, 5, 6), DateTimeUtils.plus(ldt, Period.ofDays(-3)));

        TestCase.assertNull(DateTimeUtils.plusDays((LocalDateTime) null, 3));
        TestCase.assertNull(DateTimeUtils.plus((LocalDateTime) null, Period.ofDays(3)));
        TestCase.assertNull(DateTimeUtils.plus(ldt, null));

        try {
            DateTimeUtils.plusDays(ldt, Long.MAX_VALUE);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        try {
            DateTimeUtils.plus(LocalDateTime.MAX, Period.ofDays(Integer.MAX_VALUE));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testPlusLocalDate() {
        final LocalDate d = LocalDate.of(2010, 1, 2);
        TestCase.assertEquals(LocalDate.of(2010, 1, 5), DateTimeUtils.plusDays(d, 3));
        TestCase.assertEquals(LocalDate.of(2009, 12, 30), DateTimeUtils.plusDays(d, -3));
        TestCase.assertEquals(LocalDate.of(2010, 1, 5), DateTimeUtils.plus(d, Period.ofDays(3)));
        TestCase.assertEquals(LocalDate.of(2009, 12, 30), DateTimeUtils.plus(d, Period.ofDays(-3)));

        TestCase.assertNull(DateTimeUtils.plusDays((LocalDate) null, 3));
        TestCase.assertNull(DateTimeUtils.plus((LocalDate) null, Period.ofDays(3)));
        TestCase.assertNull(DateTimeUtils.plus(d, null));

        try {
            DateTimeUtils.plusDays(d, Long.MAX_VALUE);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        try {
            DateTimeUtils.plus(LocalDate.MAX, Period.ofDays(Integer.MAX_VALUE));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testPlusDuration() {
        final Duration d1 = Duration.ofSeconds(1);
        final Duration d2 = Duration.ofSeconds(2);
        final Duration d3 = Duration.ofSeconds(3);
        TestCase.assertEquals(d3, DateTimeUtils.plus(d1, d2));
        TestCase.assertEquals(d3, DateTimeUtils.plus(d2, d1));
        TestCase.assertNull(DateTimeUtils.plus(d1, null));
        TestCase.assertNull(DateTimeUtils.plus((Duration) null, d2));

        try {
            DateTimeUtils.plus(d1, Duration.ofSeconds(Long.MAX_VALUE));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testPlusPeriod() {
        final Period d1 = Period.ofDays(1);
        final Period d2 = Period.ofDays(2);
        final Period d3 = Period.ofDays(3);
        TestCase.assertEquals(d3, DateTimeUtils.plus(d1, d2));
        TestCase.assertEquals(d3, DateTimeUtils.plus(d2, d1));
        TestCase.assertNull(DateTimeUtils.plus(d1, null));
        TestCase.assertNull(DateTimeUtils.plus((Period) null, d2));

        try {
            DateTimeUtils.plus(d1, Period.ofDays(Integer.MAX_VALUE));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testPlus() {
        final Instant instant = DateTimeUtils.parseInstant("2010-01-01T12:13:14.999123456 JP");
        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(instant, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) + 54321L,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, 54321L)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) - 54321L,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, -54321L)));

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) + 54321L,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, 54321L)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) - 54321L,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, -54321L)));

        Period period = Period.parse("P1D");
        Duration duration = Duration.parse("PT1h");

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) + DateTimeUtils.DAY,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, period)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) + 3600000000000L,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, duration)));

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) + DateTimeUtils.DAY,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, period)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) + 3600000000000L,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, duration)));

        period = Period.parse("-P1D");
        duration = Duration.parse("PT-1h");

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) - DateTimeUtils.DAY,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, period)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) - 3600000000000L,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, duration)));

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) - DateTimeUtils.DAY,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, period)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant) - 3600000000000L,
                DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, duration)));

        TestCase.assertNull(DateTimeUtils.plus(instant, NULL_LONG));
        TestCase.assertNull(DateTimeUtils.plus(instant, (Period) null));
        TestCase.assertNull(DateTimeUtils.plus(instant, (Duration) null));
        TestCase.assertNull(DateTimeUtils.plus((Instant) null, period));
        TestCase.assertNull(DateTimeUtils.plus((Instant) null, duration));

        TestCase.assertNull(DateTimeUtils.plus(zdt, NULL_LONG));
        TestCase.assertNull(DateTimeUtils.plus(zdt, (Period) null));
        TestCase.assertNull(DateTimeUtils.plus(zdt, (Duration) null));
        TestCase.assertNull(DateTimeUtils.plus((ZonedDateTime) null, period));
        TestCase.assertNull(DateTimeUtils.plus((ZonedDateTime) null, duration));

        // overflow plus

        DateTimeUtils.plus(Instant.ofEpochSecond(31556889864403199L, 999_999_999L - 10), 10); // edge at max
        try {
            DateTimeUtils.plus(Instant.ofEpochSecond(31556889864403199L, 999_999_999L), 1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(Instant.ofEpochSecond(31556889864403199L, 999_999_999L), Period.ofDays(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(Instant.ofEpochSecond(31556889864403199L, 999_999_999L), Duration.ofNanos(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31),
                LocalTime.of(23, 59, 59, 999_999_999 - 10), ZoneId.of("UTC")), 10); // edge at max
        try {
            DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31),
                    LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), 1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31),
                    LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), Period.ofDays(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31),
                    LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), Duration.ofNanos(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        DateTimeUtils.plus(Instant.ofEpochSecond(-31557014167219200L, 10), -10); // edge at max
        try {
            DateTimeUtils.plus(Instant.ofEpochSecond(-31557014167219200L, 0), -1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(Instant.ofEpochSecond(-31557014167219200L, 0), Period.ofDays(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(Instant.ofEpochSecond(-31557014167219200L, 0), Duration.ofNanos(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        // edge at max
        DateTimeUtils.plus(
                ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 10), ZoneId.of("UTC")), -10);
        try {
            DateTimeUtils.plus(
                    ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")),
                    -1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(
                    ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")),
                    Period.ofDays(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(
                    ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")),
                    Duration.ofNanos(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

    }

    public void testMinusLocalDateTime() {
        final LocalDateTime ldt = LocalDateTime.of(2010, 1, 2, 3, 4, 5, 6);
        TestCase.assertEquals(LocalDateTime.of(2009, 12, 30, 3, 4, 5, 6), DateTimeUtils.minusDays(ldt, 3));
        TestCase.assertEquals(LocalDateTime.of(2010, 1, 5, 3, 4, 5, 6), DateTimeUtils.minusDays(ldt, -3));
        TestCase.assertEquals(LocalDateTime.of(2009, 12, 30, 3, 4, 5, 6), DateTimeUtils.minus(ldt, Period.ofDays(3)));
        TestCase.assertEquals(LocalDateTime.of(2010, 1, 5, 3, 4, 5, 6), DateTimeUtils.minus(ldt, Period.ofDays(-3)));


        TestCase.assertNull(DateTimeUtils.minusDays((LocalDateTime) null, 3));
        TestCase.assertNull(DateTimeUtils.minus((LocalDateTime) null, Period.ofDays(3)));
        TestCase.assertNull(DateTimeUtils.minus(ldt, null));

        try {
            DateTimeUtils.minusDays(ldt, Long.MAX_VALUE);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        try {
            DateTimeUtils.minus(LocalDateTime.MIN, Period.ofDays(Integer.MAX_VALUE));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testMinusLocalDate() {
        final LocalDate d = LocalDate.of(2010, 1, 2);
        TestCase.assertEquals(LocalDate.of(2009, 12, 30), DateTimeUtils.minusDays(d, 3));
        TestCase.assertEquals(LocalDate.of(2010, 1, 5), DateTimeUtils.minusDays(d, -3));
        TestCase.assertEquals(LocalDate.of(2009, 12, 30), DateTimeUtils.minus(d, Period.ofDays(3)));
        TestCase.assertEquals(LocalDate.of(2010, 1, 5), DateTimeUtils.minus(d, Period.ofDays(-3)));


        TestCase.assertNull(DateTimeUtils.minusDays((LocalDate) null, 3));
        TestCase.assertNull(DateTimeUtils.minus((LocalDate) null, Period.ofDays(3)));
        TestCase.assertNull(DateTimeUtils.minus(d, null));

        try {
            DateTimeUtils.minusDays(d, Long.MAX_VALUE);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        try {
            DateTimeUtils.minus(LocalDate.MIN, Period.ofDays(Integer.MAX_VALUE));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testMinusDuration() {
        final Duration d1 = Duration.ofSeconds(3);
        final Duration d2 = Duration.ofSeconds(1);
        TestCase.assertEquals(Duration.ofSeconds(2), DateTimeUtils.minus(d1, d2));
        TestCase.assertEquals(Duration.ofSeconds(-2), DateTimeUtils.minus(d2, d1));
        TestCase.assertNull(DateTimeUtils.minus(d1, null));
        TestCase.assertNull(DateTimeUtils.minus((Duration) null, d2));

        try {
            DateTimeUtils.minus(d1, Duration.ofSeconds(Long.MIN_VALUE));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testMinusPeriod() {
        final Period d1 = Period.ofDays(3);
        final Period d2 = Period.ofDays(1);
        TestCase.assertEquals(Period.ofDays(2), DateTimeUtils.minus(d1, d2));
        TestCase.assertEquals(Period.ofDays(-2), DateTimeUtils.minus(d2, d1));
        TestCase.assertNull(DateTimeUtils.minus(d1, null));
        TestCase.assertNull(DateTimeUtils.minus((Period) null, d2));

        try {
            DateTimeUtils.minus(d1, Period.ofDays(Integer.MIN_VALUE));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testMinus() {
        final Instant instant1 = DateTimeUtils.parseInstant("2010-01-01T12:13:14.999123456 JP");
        final Instant instant2 = DateTimeUtils.parseInstant("2010-01-01T13:13:14.999123456 JP");
        final ZonedDateTime zdt1 = DateTimeUtils.toZonedDateTime(instant1, TZ_AL);
        final ZonedDateTime zdt2 = DateTimeUtils.toZonedDateTime(instant2, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) - 54321L,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, 54321L)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant2) + 54321L,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(instant2, -54321L)));

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) - 54321L,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, 54321L)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant2) + 54321L,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt2, -54321L)));

        TestCase.assertEquals(-3600000000000L, DateTimeUtils.minus(instant1, instant2));
        TestCase.assertEquals(3600000000000L, DateTimeUtils.minus(instant2, instant1));

        TestCase.assertEquals(-3600000000000L, DateTimeUtils.minus(zdt1, zdt2));
        TestCase.assertEquals(3600000000000L, DateTimeUtils.minus(zdt2, zdt1));

        Period period = Period.parse("P1D");
        Duration duration = Duration.parse("PT1h");

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) - DateTimeUtils.DAY,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, period)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) - 3600000000000L,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, duration)));

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) - DateTimeUtils.DAY,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, period)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) - 3600000000000L,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, duration)));

        period = Period.parse("-P1D");
        duration = Duration.parse("PT-1h");

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) + DateTimeUtils.DAY,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, period)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) + 3600000000000L,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, duration)));

        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) + DateTimeUtils.DAY,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, period)));
        TestCase.assertEquals(DateTimeUtils.epochNanos(instant1) + 3600000000000L,
                DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, duration)));

        TestCase.assertNull(DateTimeUtils.minus(instant1, NULL_LONG));
        TestCase.assertNull(DateTimeUtils.minus(instant1, (Period) null));
        TestCase.assertNull(DateTimeUtils.minus(instant1, (Duration) null));
        TestCase.assertNull(DateTimeUtils.minus((Instant) null, period));
        TestCase.assertNull(DateTimeUtils.minus((Instant) null, duration));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.minus(instant1, (Instant) null));

        TestCase.assertNull(DateTimeUtils.minus(zdt1, NULL_LONG));
        TestCase.assertNull(DateTimeUtils.minus(zdt1, (Period) null));
        TestCase.assertNull(DateTimeUtils.minus(zdt1, (Duration) null));
        TestCase.assertNull(DateTimeUtils.minus((ZonedDateTime) null, period));
        TestCase.assertNull(DateTimeUtils.minus((ZonedDateTime) null, duration));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.minus(zdt1, (ZonedDateTime) null));

        // overflow minus

        DateTimeUtils.minus(Instant.ofEpochSecond(31556889864403199L, 999_999_999L - 10), -10); // edge at max
        try {
            DateTimeUtils.minus(Instant.ofEpochSecond(31556889864403199L, 999_999_999L), -1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(Instant.ofEpochSecond(31556889864403199L, 999_999_999L), Period.ofDays(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(Instant.ofEpochSecond(31556889864403199L, 999_999_999L), Duration.ofNanos(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31),
                LocalTime.of(23, 59, 59, 999_999_999 - 10), ZoneId.of("UTC")), 10); // edge at max
        try {
            DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31),
                    LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), -1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31),
                    LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), Period.ofDays(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31),
                    LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), Duration.ofNanos(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        DateTimeUtils.minus(Instant.ofEpochSecond(-31557014167219200L, 10), 10); // edge at min
        try {
            DateTimeUtils.minus(Instant.ofEpochSecond(-31557014167219200L, 0), 1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(Instant.ofEpochSecond(-31557014167219200L, 0), Period.ofDays(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(Instant.ofEpochSecond(-31557014167219200L, 0), Duration.ofNanos(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        // edge at max
        DateTimeUtils.minus(
                ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 10), ZoneId.of("UTC")), -10);
        try {
            DateTimeUtils.minus(
                    ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")),
                    1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(
                    ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")),
                    Period.ofDays(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(
                    ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")),
                    Duration.ofNanos(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

    }

    public void testMultiply() {
        final Period p = Period.ofDays(3);
        final Duration d = Duration.ofNanos(123456789L);

        TestCase.assertEquals(Period.ofDays(6), DateTimeUtils.multiply(p, 2));
        TestCase.assertEquals(Period.ofDays(6), DateTimeUtils.multiply(2, p));
        TestCase.assertEquals(Period.ofDays(9), DateTimeUtils.multiply(p, 3));
        TestCase.assertEquals(Period.ofDays(3), DateTimeUtils.multiply(p, 1));
        TestCase.assertEquals(Period.ofDays(0), DateTimeUtils.multiply(p, 0));
        TestCase.assertEquals(Period.ofDays(-3), DateTimeUtils.multiply(p, -1));
        TestCase.assertEquals(Period.ofDays(-6), DateTimeUtils.multiply(p, -2));
        TestCase.assertEquals(Period.ofDays(-9), DateTimeUtils.multiply(p, -3));
        TestCase.assertNull(DateTimeUtils.multiply((Period) null, 3));
        TestCase.assertNull(DateTimeUtils.multiply(p, NULL_INT));


        TestCase.assertEquals(Duration.ofNanos(246913578L), DateTimeUtils.multiply(d, 2));
        TestCase.assertEquals(Duration.ofNanos(246913578L), DateTimeUtils.multiply(2, d));
        TestCase.assertEquals(Duration.ofNanos(370370367L), DateTimeUtils.multiply(d, 3));
        TestCase.assertEquals(Duration.ofNanos(123456789L), DateTimeUtils.multiply(d, 1));
        TestCase.assertEquals(Duration.ofNanos(0), DateTimeUtils.multiply(d, 0));
        TestCase.assertEquals(Duration.ofNanos(-123456789L), DateTimeUtils.multiply(d, -1));
        TestCase.assertEquals(Duration.ofNanos(-246913578L), DateTimeUtils.multiply(d, -2));
        TestCase.assertEquals(Duration.ofNanos(-370370367L), DateTimeUtils.multiply(d, -3));
        TestCase.assertNull(DateTimeUtils.multiply((Duration) null, 3));
        TestCase.assertNull(DateTimeUtils.multiply(d, NULL_LONG));
    }

    public void testDivideDuration() {
        final Duration d = Duration.ofNanos(123456789L);

        TestCase.assertEquals(Duration.ofNanos(61728394L), DateTimeUtils.divide(d, 2));
        TestCase.assertEquals(Duration.ofNanos(123456789L), DateTimeUtils.divide(d, 1));
        TestCase.assertEquals(Duration.ofNanos(-123456789L), DateTimeUtils.divide(d, -1));
        TestCase.assertEquals(Duration.ofNanos(-61728394L), DateTimeUtils.divide(d, -2));
        TestCase.assertNull(DateTimeUtils.divide((Duration) null, 3));
        TestCase.assertNull(DateTimeUtils.divide(d, NULL_LONG));

        try {
            DateTimeUtils.divide(d, 0);
            TestCase.fail("This should have excepted");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
    }

    public void testDiffNanos() {
        final Instant i1 = DateTimeUtils.epochNanosToInstant(12345678987654321L);
        final Instant i2 = DateTimeUtils.epochNanosToInstant(98765432123456789L);
        final long delta = DateTimeUtils.epochNanos(i2) - DateTimeUtils.epochNanos(i1);

        TestCase.assertEquals(delta, DateTimeUtils.diffNanos(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffNanos(i2, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(null, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(i2, null));

        final ZonedDateTime zdt1 = i1.atZone(TZ_AL);
        final ZonedDateTime zdt2 = i2.atZone(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffNanos(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffNanos(zdt2, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(null, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(zdt2, null));
    }

    public void testDiffMicros() {
        final Instant i1 = DateTimeUtils.epochNanosToInstant(12345678987654321L);
        final Instant i2 = DateTimeUtils.epochNanosToInstant(98765432123456789L);
        final long delta = (DateTimeUtils.epochNanos(i2) - DateTimeUtils.epochNanos(i1)) / DateTimeUtils.MICRO;

        TestCase.assertEquals(delta, DateTimeUtils.diffMicros(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMicros(i2, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(null, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(i2, null));

        final ZonedDateTime zdt1 = i1.atZone(TZ_AL);
        final ZonedDateTime zdt2 = i2.atZone(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffMicros(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMicros(zdt2, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(null, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(zdt2, null));
    }

    public void testDiffMillis() {
        final Instant i1 = DateTimeUtils.epochNanosToInstant(12345678987654321L);
        final Instant i2 = DateTimeUtils.epochNanosToInstant(98765432123456789L);
        final long delta = (DateTimeUtils.epochNanos(i2) - DateTimeUtils.epochNanos(i1)) / DateTimeUtils.MILLI;

        TestCase.assertEquals(delta, DateTimeUtils.diffMillis(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMillis(i2, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(null, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(i2, null));

        final ZonedDateTime zdt1 = i1.atZone(TZ_AL);
        final ZonedDateTime zdt2 = i2.atZone(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffMillis(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMillis(zdt2, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(null, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(zdt2, null));
    }

    public void testDiffSeconds() {
        final Instant i1 = DateTimeUtils.epochNanosToInstant(12345678987654321L);
        final Instant i2 = DateTimeUtils.epochNanosToInstant(98765432123456789L);
        final double delta =
                (DateTimeUtils.epochNanos(i2) - DateTimeUtils.epochNanos(i1)) / (double) DateTimeUtils.SECOND;

        TestCase.assertEquals(delta, DateTimeUtils.diffSeconds(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffSeconds(i2, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(null, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(i2, null));

        final ZonedDateTime zdt1 = i1.atZone(TZ_AL);
        final ZonedDateTime zdt2 = i2.atZone(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffSeconds(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffSeconds(zdt2, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(null, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(zdt2, null));
    }

    public void testDiffMinutes() {
        final Instant i1 = DateTimeUtils.epochNanosToInstant(12345678987654321L);
        final Instant i2 = DateTimeUtils.epochNanosToInstant(98765432123456789L);
        final double delta =
                (DateTimeUtils.epochNanos(i2) - DateTimeUtils.epochNanos(i1)) / (double) DateTimeUtils.MINUTE;

        TestCase.assertEquals(delta, DateTimeUtils.diffMinutes(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMinutes(i2, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(null, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(i2, null));

        final ZonedDateTime zdt1 = i1.atZone(TZ_AL);
        final ZonedDateTime zdt2 = i2.atZone(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffMinutes(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMinutes(zdt2, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(null, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(zdt2, null));
    }

    public void testDiffDays() {
        final Instant i1 = DateTimeUtils.epochNanosToInstant(12345678987654321L);
        final Instant i2 = DateTimeUtils.epochNanosToInstant(98765432123456789L);
        final double delta = (DateTimeUtils.epochNanos(i2) - DateTimeUtils.epochNanos(i1)) / (double) DateTimeUtils.DAY;

        TestCase.assertEquals(delta, DateTimeUtils.diffDays(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffDays(i2, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(null, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(i2, null));

        final ZonedDateTime zdt1 = i1.atZone(TZ_AL);
        final ZonedDateTime zdt2 = i2.atZone(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffDays(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffDays(zdt2, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(null, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(zdt2, null));
    }

    public void testDiffYears365() {
        final Instant i1 = DateTimeUtils.epochNanosToInstant(12345678987654321L);
        final Instant i2 = DateTimeUtils.epochNanosToInstant(98765432123456789L);
        final double delta =
                (DateTimeUtils.epochNanos(i2) - DateTimeUtils.epochNanos(i1)) / (double) DateTimeUtils.YEAR_365;

        assertEquals(delta, DateTimeUtils.diffYears365(i1, i2), 1e-10);
        assertEquals(-delta, DateTimeUtils.diffYears365(i2, i1), 1e-10);
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears365(null, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears365(i2, null));

        final ZonedDateTime zdt1 = i1.atZone(TZ_AL);
        final ZonedDateTime zdt2 = i2.atZone(TZ_AL);

        assertEquals(delta, DateTimeUtils.diffYears365(zdt1, zdt2), 1e-10);
        assertEquals(-delta, DateTimeUtils.diffYears365(zdt2, zdt1), 1e-10);
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears365(null, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears365(zdt2, null));
    }

    public void testDiffYears() {
        final Instant i1 = DateTimeUtils.epochNanosToInstant(12345678987654321L);
        final Instant i2 = DateTimeUtils.epochNanosToInstant(98765432123456789L);
        final double delta =
                (DateTimeUtils.epochNanos(i2) - DateTimeUtils.epochNanos(i1)) / (double) DateTimeUtils.YEAR_AVG;

        assertEquals(delta, DateTimeUtils.diffYearsAvg(i1, i2), 1e-10);
        assertEquals(-delta, DateTimeUtils.diffYearsAvg(i2, i1), 1e-10);
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYearsAvg(null, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYearsAvg(i2, null));

        final ZonedDateTime zdt1 = i1.atZone(TZ_AL);
        final ZonedDateTime zdt2 = i2.atZone(TZ_AL);

        assertEquals(delta, DateTimeUtils.diffYearsAvg(zdt1, zdt2), 1e-10);
        assertEquals(-delta, DateTimeUtils.diffYearsAvg(zdt2, zdt1), 1e-10);
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYearsAvg(null, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYearsAvg(zdt2, null));
    }

    public void testYear() {
        final LocalDateTime ldt = LocalDateTime.of(2023, 1, 2, 3, 4, 5);
        final LocalDate dt1 = LocalDate.of(2023, 1, 2);
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(2023, DateTimeUtils.year(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year((LocalDateTime) null));

        TestCase.assertEquals(2023, DateTimeUtils.year(dt1));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year((LocalDate) null));

        TestCase.assertEquals(2023, DateTimeUtils.year(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year(null, TZ_JP));

        TestCase.assertEquals(2023, DateTimeUtils.year(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year((ZonedDateTime) null));
    }

    public void testYearOfCentury() {
        final LocalDateTime ldt = LocalDateTime.of(2023, 1, 2, 3, 4, 5);
        final LocalDate dt1 = LocalDate.of(2023, 1, 2);
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(23, DateTimeUtils.yearOfCentury(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury((LocalDateTime) null));

        TestCase.assertEquals(23, DateTimeUtils.yearOfCentury(dt1));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury((LocalDate) null));

        TestCase.assertEquals(23, DateTimeUtils.yearOfCentury(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury(null, TZ_JP));

        TestCase.assertEquals(23, DateTimeUtils.yearOfCentury(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury((ZonedDateTime) null));
    }

    public void testMonthOfYear() {
        final LocalDateTime ldt = LocalDateTime.of(2023, 2, 3, 4, 5, 6);
        final LocalDate dt1 = LocalDate.of(2023, 2, 3);
        final Instant dt2 = DateTimeUtils.parseInstant("2023-02-03T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(2, DateTimeUtils.monthOfYear(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear((LocalDateTime) null));

        TestCase.assertEquals(2, DateTimeUtils.monthOfYear(dt1));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear((LocalDate) null));

        TestCase.assertEquals(2, DateTimeUtils.monthOfYear(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear(null, TZ_JP));

        TestCase.assertEquals(2, DateTimeUtils.monthOfYear(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear((ZonedDateTime) null));
    }

    public void testDayOfMonth() {
        final LocalDateTime ldt = LocalDateTime.of(2023, 2, 3, 4, 5, 6);
        final LocalDate dt1 = LocalDate.of(2023, 2, 3);
        final Instant dt2 = DateTimeUtils.parseInstant("2023-02-03T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(3, DateTimeUtils.dayOfMonth(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth((LocalDateTime) null));

        TestCase.assertEquals(3, DateTimeUtils.dayOfMonth(dt1));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth((LocalDate) null));

        TestCase.assertEquals(3, DateTimeUtils.dayOfMonth(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth(null, TZ_JP));

        TestCase.assertEquals(3, DateTimeUtils.dayOfMonth(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth((ZonedDateTime) null));
    }

    public void testDayOfWeek() {
        final LocalDateTime ldt = LocalDateTime.of(2023, 2, 3, 4, 5, 6);
        final LocalDate dt1 = LocalDate.of(2023, 2, 3);
        final Instant dt2 = DateTimeUtils.parseInstant("2023-02-03T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(DayOfWeek.FRIDAY, DateTimeUtils.dayOfWeek(ldt));
        TestCase.assertNull(DateTimeUtils.dayOfWeek((LocalDateTime) null));
        TestCase.assertEquals(DayOfWeek.FRIDAY.getValue(), DateTimeUtils.dayOfWeekValue(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeekValue((LocalDateTime) null));

        TestCase.assertEquals(DayOfWeek.FRIDAY, DateTimeUtils.dayOfWeek(dt1));
        TestCase.assertNull(DateTimeUtils.dayOfWeek((LocalDate) null));
        TestCase.assertEquals(DayOfWeek.FRIDAY.getValue(), DateTimeUtils.dayOfWeekValue(dt1));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeekValue((LocalDate) null));

        TestCase.assertEquals(DayOfWeek.FRIDAY, DateTimeUtils.dayOfWeek(dt2, TZ_JP));
        TestCase.assertNull(DateTimeUtils.dayOfWeek(dt2, null));
        TestCase.assertNull(DateTimeUtils.dayOfWeek(null, TZ_JP));
        TestCase.assertEquals(DayOfWeek.FRIDAY.getValue(), DateTimeUtils.dayOfWeekValue(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeekValue(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeekValue(null, TZ_JP));

        TestCase.assertEquals(DayOfWeek.FRIDAY, DateTimeUtils.dayOfWeek(dt3));
        TestCase.assertNull(DateTimeUtils.dayOfWeek((ZonedDateTime) null));
        TestCase.assertEquals(DayOfWeek.FRIDAY.getValue(), DateTimeUtils.dayOfWeekValue(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeekValue((ZonedDateTime) null));
    }

    public void testDayOfYear() {
        final LocalDateTime ldt = LocalDateTime.of(2023, 2, 3, 4, 5, 6);
        final LocalDate dt1 = LocalDate.of(2023, 2, 3);
        final Instant dt2 = DateTimeUtils.parseInstant("2023-02-03T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(34, DateTimeUtils.dayOfYear(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear((LocalDateTime) null));

        TestCase.assertEquals(34, DateTimeUtils.dayOfYear(dt1));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear((LocalDate) null));

        TestCase.assertEquals(34, DateTimeUtils.dayOfYear(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear(null, TZ_JP));

        TestCase.assertEquals(34, DateTimeUtils.dayOfYear(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear((ZonedDateTime) null));
    }

    public void testHourOfDay() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDateTime ldt = dt3.toLocalDateTime();
        final LocalTime lt = dt3.toLocalTime();

        TestCase.assertEquals(11, DateTimeUtils.hourOfDay(dt2, TZ_JP, true));
        TestCase.assertEquals(11, DateTimeUtils.hourOfDay(dt2, TZ_JP, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay(dt2, null, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay(dt2, null, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay(null, TZ_JP, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay(null, TZ_JP, false));

        TestCase.assertEquals(11, DateTimeUtils.hourOfDay(dt3, true));
        TestCase.assertEquals(11, DateTimeUtils.hourOfDay(dt3, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay(null, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay(null, false));

        TestCase.assertEquals(11, DateTimeUtils.hourOfDay(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay((LocalDateTime) null));

        TestCase.assertEquals(11, DateTimeUtils.hourOfDay(lt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay((LocalTime) null));

        // Test daylight savings time

        final Instant dstMid1 = DateTimeUtils.parseInstant("2023-03-12T00:00:00 America/Denver");

        final Instant dstI11 = DateTimeUtils.plus(dstMid1, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt11 = DateTimeUtils.toZonedDateTime(dstI11, ZoneId.of("America/Denver"));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstI11, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstI11, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt11, true));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt11, false));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt11.toLocalTime()));

        final Instant dstI12 = DateTimeUtils.plus(dstMid1, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt12 = DateTimeUtils.toZonedDateTime(dstI12, ZoneId.of("America/Denver"));
        TestCase.assertEquals(3, DateTimeUtils.hourOfDay(dstI12, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2, DateTimeUtils.hourOfDay(dstI12, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(3, DateTimeUtils.hourOfDay(dstZdt12, true));
        TestCase.assertEquals(2, DateTimeUtils.hourOfDay(dstZdt12, false));
        TestCase.assertEquals(3, DateTimeUtils.hourOfDay(dstZdt12.toLocalDateTime()));
        TestCase.assertEquals(3, DateTimeUtils.hourOfDay(dstZdt12.toLocalTime()));

        final Instant dstI13 = DateTimeUtils.plus(dstMid1, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt13 = DateTimeUtils.toZonedDateTime(dstI13, ZoneId.of("America/Denver"));
        TestCase.assertEquals(4, DateTimeUtils.hourOfDay(dstI13, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3, DateTimeUtils.hourOfDay(dstI13, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(4, DateTimeUtils.hourOfDay(dstZdt13, true));
        TestCase.assertEquals(3, DateTimeUtils.hourOfDay(dstZdt13, false));
        TestCase.assertEquals(4, DateTimeUtils.hourOfDay(dstZdt13.toLocalDateTime()));
        TestCase.assertEquals(4, DateTimeUtils.hourOfDay(dstZdt13.toLocalTime()));


        final Instant dstMid2 = DateTimeUtils.parseInstant("2023-11-05T00:00:00 America/Denver");

        final Instant dstI21 = DateTimeUtils.plus(dstMid2, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt21 = DateTimeUtils.toZonedDateTime(dstI21, ZoneId.of("America/Denver"));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstI21, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstI21, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt21, true));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt21, false));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt21.toLocalDateTime()));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt21.toLocalTime()));

        final Instant dstI22 = DateTimeUtils.plus(dstMid2, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt22 = DateTimeUtils.toZonedDateTime(dstI22, ZoneId.of("America/Denver"));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstI22, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2, DateTimeUtils.hourOfDay(dstI22, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt22, true));
        TestCase.assertEquals(2, DateTimeUtils.hourOfDay(dstZdt22, false));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt22.toLocalDateTime()));
        TestCase.assertEquals(1, DateTimeUtils.hourOfDay(dstZdt22.toLocalTime()));

        final Instant dstI23 = DateTimeUtils.plus(dstMid2, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt23 = DateTimeUtils.toZonedDateTime(dstI23, ZoneId.of("America/Denver"));
        TestCase.assertEquals(2, DateTimeUtils.hourOfDay(dstI23, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3, DateTimeUtils.hourOfDay(dstI23, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(2, DateTimeUtils.hourOfDay(dstZdt23, true));
        TestCase.assertEquals(3, DateTimeUtils.hourOfDay(dstZdt23, false));
        TestCase.assertEquals(2, DateTimeUtils.hourOfDay(dstZdt23.toLocalDateTime()));
        TestCase.assertEquals(2, DateTimeUtils.hourOfDay(dstZdt23.toLocalTime()));
    }

    public void testMinuteOfHour() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(23, DateTimeUtils.minuteOfHour(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfHour(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfHour(null, TZ_JP));

        TestCase.assertEquals(23, DateTimeUtils.minuteOfHour(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfHour(null));
    }

    public void testMinuteOfDay() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDateTime ldt = dt3.toLocalDateTime();
        final LocalTime lt = dt3.toLocalTime();

        TestCase.assertEquals(11 * 60 + 23, DateTimeUtils.minuteOfDay(dt2, TZ_JP, true));
        TestCase.assertEquals(11 * 60 + 23, DateTimeUtils.minuteOfDay(dt2, TZ_JP, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay(dt2, null, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay(dt2, null, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay(null, TZ_JP, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay(null, TZ_JP, false));

        TestCase.assertEquals(11 * 60 + 23, DateTimeUtils.minuteOfDay(dt3, true));
        TestCase.assertEquals(11 * 60 + 23, DateTimeUtils.minuteOfDay(dt3, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay(null, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay(null, false));

        TestCase.assertEquals(11 * 60 + 23, DateTimeUtils.minuteOfDay(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay((LocalDateTime) null));

        TestCase.assertEquals(11 * 60 + 23, DateTimeUtils.minuteOfDay(lt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay((LocalTime) null));

        // Test daylight savings time

        final Instant dstMid1 = DateTimeUtils.parseInstant("2023-03-12T00:00:00 America/Denver");

        final Instant dstI11 = DateTimeUtils.plus(dstMid1, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt11 = DateTimeUtils.toZonedDateTime(dstI11, ZoneId.of("America/Denver"));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI11, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI11, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE, DateTimeUtils.minuteOfDay(dstZdt11, true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE, DateTimeUtils.minuteOfDay(dstZdt11, false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt11.toLocalDateTime()));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt11.toLocalTime()));

        final Instant dstI12 = DateTimeUtils.plus(dstMid1, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt12 = DateTimeUtils.toZonedDateTime(dstI12, ZoneId.of("America/Denver"));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI12, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI12, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE, DateTimeUtils.minuteOfDay(dstZdt12, true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt12, false));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt12.toLocalDateTime()));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt12.toLocalTime()));

        final Instant dstI13 = DateTimeUtils.plus(dstMid1, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt13 = DateTimeUtils.toZonedDateTime(dstI13, ZoneId.of("America/Denver"));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI13, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI13, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE, DateTimeUtils.minuteOfDay(dstZdt13, true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt13, false));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt13.toLocalDateTime()));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt13.toLocalTime()));


        final Instant dstMid2 = DateTimeUtils.parseInstant("2023-11-05T00:00:00 America/Denver");

        final Instant dstI21 = DateTimeUtils.plus(dstMid2, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt21 = DateTimeUtils.toZonedDateTime(dstI21, ZoneId.of("America/Denver"));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI21, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI21, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE, DateTimeUtils.minuteOfDay(dstZdt21, true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE, DateTimeUtils.minuteOfDay(dstZdt21, false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt21.toLocalDateTime()));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt21.toLocalTime()));

        final Instant dstI22 = DateTimeUtils.plus(dstMid2, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt22 = DateTimeUtils.toZonedDateTime(dstI22, ZoneId.of("America/Denver"));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI22, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI22, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE, DateTimeUtils.minuteOfDay(dstZdt22, true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt22, false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt22.toLocalDateTime()));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt22.toLocalTime()));

        final Instant dstI23 = DateTimeUtils.plus(dstMid2, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt23 = DateTimeUtils.toZonedDateTime(dstI23, ZoneId.of("America/Denver"));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI23, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstI23, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE, DateTimeUtils.minuteOfDay(dstZdt23, true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt23, false));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt23.toLocalDateTime()));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MINUTE,
                DateTimeUtils.minuteOfDay(dstZdt23.toLocalTime()));
    }

    public void testSecondOfMinute() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(45, DateTimeUtils.secondOfMinute(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfMinute(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfMinute(null, TZ_JP));

        TestCase.assertEquals(45, DateTimeUtils.secondOfMinute(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfMinute(null));
    }

    public void testSecondOfDay() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDateTime ldt = dt3.toLocalDateTime();
        final LocalTime lt = dt3.toLocalTime();

        TestCase.assertEquals(11 * 60 * 60 + 23 * 60 + 45, DateTimeUtils.secondOfDay(dt2, TZ_JP, true));
        TestCase.assertEquals(11 * 60 * 60 + 23 * 60 + 45, DateTimeUtils.secondOfDay(dt2, TZ_JP, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay(dt2, null, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay(dt2, null, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay(null, TZ_JP, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay(null, TZ_JP, false));

        TestCase.assertEquals(11 * 60 * 60 + 23 * 60 + 45, DateTimeUtils.secondOfDay(dt3, true));
        TestCase.assertEquals(11 * 60 * 60 + 23 * 60 + 45, DateTimeUtils.secondOfDay(dt3, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay(null, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay(null, false));

        TestCase.assertEquals(11 * 60 * 60 + 23 * 60 + 45, DateTimeUtils.secondOfDay(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay((LocalDateTime) null));

        TestCase.assertEquals(11 * 60 * 60 + 23 * 60 + 45, DateTimeUtils.secondOfDay(lt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay((LocalTime) null));

        // Test daylight savings time

        final Instant dstMid1 = DateTimeUtils.parseInstant("2023-03-12T00:00:00 America/Denver");

        final Instant dstI11 = DateTimeUtils.plus(dstMid1, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt11 = DateTimeUtils.toZonedDateTime(dstI11, ZoneId.of("America/Denver"));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI11, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI11, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND, DateTimeUtils.secondOfDay(dstZdt11, true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND, DateTimeUtils.secondOfDay(dstZdt11, false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt11.toLocalDateTime()));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt11.toLocalTime()));

        final Instant dstI12 = DateTimeUtils.plus(dstMid1, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt12 = DateTimeUtils.toZonedDateTime(dstI12, ZoneId.of("America/Denver"));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI12, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI12, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.SECOND, DateTimeUtils.secondOfDay(dstZdt12, true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt12, false));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt12.toLocalDateTime()));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt12.toLocalTime()));

        final Instant dstI13 = DateTimeUtils.plus(dstMid1, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt13 = DateTimeUtils.toZonedDateTime(dstI13, ZoneId.of("America/Denver"));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI13, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI13, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.SECOND, DateTimeUtils.secondOfDay(dstZdt13, true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt13, false));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt13.toLocalDateTime()));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt13.toLocalTime()));


        final Instant dstMid2 = DateTimeUtils.parseInstant("2023-11-05T00:00:00 America/Denver");

        final Instant dstI21 = DateTimeUtils.plus(dstMid2, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt21 = DateTimeUtils.toZonedDateTime(dstI21, ZoneId.of("America/Denver"));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI21, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI21, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND, DateTimeUtils.secondOfDay(dstZdt21, true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND, DateTimeUtils.secondOfDay(dstZdt21, false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt21.toLocalDateTime()));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt21.toLocalTime()));

        final Instant dstI22 = DateTimeUtils.plus(dstMid2, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt22 = DateTimeUtils.toZonedDateTime(dstI22, ZoneId.of("America/Denver"));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI22, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI22, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND, DateTimeUtils.secondOfDay(dstZdt22, true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt22, false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt22.toLocalDateTime()));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt22.toLocalTime()));

        final Instant dstI23 = DateTimeUtils.plus(dstMid2, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt23 = DateTimeUtils.toZonedDateTime(dstI23, ZoneId.of("America/Denver"));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI23, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstI23, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.SECOND, DateTimeUtils.secondOfDay(dstZdt23, true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt23, false));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt23.toLocalDateTime()));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.SECOND,
                DateTimeUtils.secondOfDay(dstZdt23.toLocalTime()));
    }

    public void testNanosOfSecond() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(123456789, DateTimeUtils.nanosOfSecond(dt2, TZ_JP));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfSecond(dt2, null));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfSecond(null, TZ_JP));

        TestCase.assertEquals(123456789, DateTimeUtils.nanosOfSecond(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfSecond(null));
    }

    public void testNanosOfMilli() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-02-03T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(123456789 % DateTimeUtils.MILLI, DateTimeUtils.nanosOfMilli(dt2));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.nanosOfMilli((Instant) null));

        TestCase.assertEquals(123456789 % DateTimeUtils.MILLI, DateTimeUtils.nanosOfMilli(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.nanosOfMilli((ZonedDateTime) null));
    }

    public void testNanosOfDay() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDateTime ldt = dt3.toLocalDateTime();
        final LocalTime lt = dt3.toLocalTime();
        final long expectedNanos = 123456789L + 1_000_000_000L * (45 + 23 * 60 + 11 * 60 * 60);

        TestCase.assertEquals(expectedNanos, DateTimeUtils.nanosOfDay(dt2, TZ_JP, true));
        TestCase.assertEquals(expectedNanos, DateTimeUtils.nanosOfDay(dt2, TZ_JP, false));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay(dt2, null, true));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay(dt2, null, false));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay(null, TZ_JP, true));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay(null, TZ_JP, false));

        TestCase.assertEquals(expectedNanos, DateTimeUtils.nanosOfDay(dt3, true));
        TestCase.assertEquals(expectedNanos, DateTimeUtils.nanosOfDay(dt3, false));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay(null, true));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay(null, false));

        TestCase.assertEquals(expectedNanos, DateTimeUtils.nanosOfDay(ldt));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay((LocalDateTime) null));

        TestCase.assertEquals(expectedNanos, DateTimeUtils.nanosOfDay(lt));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay((LocalTime) null));

        // Test daylight savings time

        final Instant dstMid1 = DateTimeUtils.parseInstant("2023-03-12T00:00:00 America/Denver");

        final Instant dstI11 = DateTimeUtils.plus(dstMid1, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt11 = DateTimeUtils.toZonedDateTime(dstI11, ZoneId.of("America/Denver"));
        final LocalDateTime dstLdt11 = dstZdt11.toLocalDateTime();
        final LocalTime dstLt11 = dstZdt11.toLocalTime();
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstI11, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstI11, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt11, true));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt11, false));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLdt11));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLt11));


        final Instant dstI12 = DateTimeUtils.plus(dstMid1, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt12 = DateTimeUtils.toZonedDateTime(dstI12, ZoneId.of("America/Denver"));
        final LocalDateTime dstLdt12 = dstZdt12.toLocalDateTime();
        final LocalTime dstLt12 = dstZdt12.toLocalTime();
        TestCase.assertEquals(3 * DateTimeUtils.HOUR,
                DateTimeUtils.nanosOfDay(dstI12, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR,
                DateTimeUtils.nanosOfDay(dstI12, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt12, true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt12, false));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLdt12)); // Adjusted
        TestCase.assertEquals(3 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLt12)); // Adjusted

        final Instant dstI13 = DateTimeUtils.plus(dstMid1, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt13 = DateTimeUtils.toZonedDateTime(dstI13, ZoneId.of("America/Denver"));
        final LocalDateTime dstLdt13 = dstZdt13.toLocalDateTime();
        final LocalTime dstLt13 = dstZdt13.toLocalTime();
        TestCase.assertEquals(4 * DateTimeUtils.HOUR,
                DateTimeUtils.nanosOfDay(dstI13, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR,
                DateTimeUtils.nanosOfDay(dstI13, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt13, true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt13, false));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLdt13)); // Adjusted
        TestCase.assertEquals(4 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLt13)); // Adjusted


        final Instant dstMid2 = DateTimeUtils.parseInstant("2023-11-05T00:00:00 America/Denver");

        final Instant dstI21 = DateTimeUtils.plus(dstMid2, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt21 = DateTimeUtils.toZonedDateTime(dstI21, ZoneId.of("America/Denver"));
        final LocalDateTime dstLdt21 = dstZdt21.toLocalDateTime();
        final LocalTime dstLt21 = dstZdt21.toLocalTime();
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstI21, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstI21, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt21, true));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt21, false));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLdt21));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLt21));

        final Instant dstI22 = DateTimeUtils.plus(dstMid2, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt22 = DateTimeUtils.toZonedDateTime(dstI22, ZoneId.of("America/Denver"));
        final LocalDateTime dstLdt22 = dstZdt22.toLocalDateTime();
        final LocalTime dstLt22 = dstZdt22.toLocalTime();
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstI22, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR,
                DateTimeUtils.nanosOfDay(dstI22, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt22, true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt22, false));
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLdt22)); // Adjusted
        TestCase.assertEquals(DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLt22)); // Adjusted

        final Instant dstI23 = DateTimeUtils.plus(dstMid2, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt23 = DateTimeUtils.toZonedDateTime(dstI23, ZoneId.of("America/Denver"));
        final LocalDateTime dstLdt23 = dstZdt23.toLocalDateTime();
        final LocalTime dstLt23 = dstZdt23.toLocalTime();
        TestCase.assertEquals(2 * DateTimeUtils.HOUR,
                DateTimeUtils.nanosOfDay(dstI23, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR,
                DateTimeUtils.nanosOfDay(dstI23, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt23, true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstZdt23, false));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLdt23)); // Adjusted
        TestCase.assertEquals(2 * DateTimeUtils.HOUR, DateTimeUtils.nanosOfDay(dstLt23)); // Adjusted
    }

    public void testMillisOfSecond() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(123, DateTimeUtils.millisOfSecond(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfSecond(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfSecond(null, TZ_JP));

        TestCase.assertEquals(123, DateTimeUtils.millisOfSecond(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfSecond(null));
    }

    public void testMillisOfDay() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDateTime ldt = dt3.toLocalDateTime();
        final LocalTime lt = dt3.toLocalTime();

        TestCase.assertEquals(123L + 1_000L * (45 + 23 * 60 + 11 * 60 * 60),
                DateTimeUtils.millisOfDay(dt2, TZ_JP, true));
        TestCase.assertEquals(123L + 1_000L * (45 + 23 * 60 + 11 * 60 * 60),
                DateTimeUtils.millisOfDay(dt2, TZ_JP, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay(dt2, null, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay(dt2, null, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay(null, TZ_JP, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay(null, TZ_JP, false));

        TestCase.assertEquals(123L + 1_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.millisOfDay(dt3, true));
        TestCase.assertEquals(123L + 1_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.millisOfDay(dt3, false));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay(null, true));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay(null, false));

        TestCase.assertEquals(123L + 1_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.millisOfDay(ldt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay((LocalDateTime) null));

        TestCase.assertEquals(123L + 1_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.millisOfDay(lt));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay((LocalTime) null));

        // Test daylight savings time

        final Instant dstMid1 = DateTimeUtils.parseInstant("2023-03-12T00:00:00 America/Denver");

        final Instant dstI11 = DateTimeUtils.plus(dstMid1, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt11 = DateTimeUtils.toZonedDateTime(dstI11, ZoneId.of("America/Denver"));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI11, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI11, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt11, true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt11, false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt11.toLocalDateTime()));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt11.toLocalTime()));

        final Instant dstI12 = DateTimeUtils.plus(dstMid1, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt12 = DateTimeUtils.toZonedDateTime(dstI12, ZoneId.of("America/Denver"));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI12, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI12, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt12, true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt12, false));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt12.toLocalDateTime()));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt12.toLocalTime()));

        final Instant dstI13 = DateTimeUtils.plus(dstMid1, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt13 = DateTimeUtils.toZonedDateTime(dstI13, ZoneId.of("America/Denver"));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI13, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI13, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt13, true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt13, false));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt13.toLocalDateTime()));
        TestCase.assertEquals(4 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt13.toLocalTime()));


        final Instant dstMid2 = DateTimeUtils.parseInstant("2023-11-05T00:00:00 America/Denver");

        final Instant dstI21 = DateTimeUtils.plus(dstMid2, DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt21 = DateTimeUtils.toZonedDateTime(dstI21, ZoneId.of("America/Denver"));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI21, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI21, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt21, true));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt21, false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt21.toLocalDateTime()));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt21.toLocalTime()));

        final Instant dstI22 = DateTimeUtils.plus(dstMid2, 2 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt22 = DateTimeUtils.toZonedDateTime(dstI22, ZoneId.of("America/Denver"));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI22, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI22, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt22, true));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt22, false));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt22.toLocalDateTime()));
        TestCase.assertEquals(DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt22.toLocalTime()));

        final Instant dstI23 = DateTimeUtils.plus(dstMid2, 3 * DateTimeUtils.HOUR);
        final ZonedDateTime dstZdt23 = DateTimeUtils.toZonedDateTime(dstI23, ZoneId.of("America/Denver"));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI23, ZoneId.of("America/Denver"), true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstI23, ZoneId.of("America/Denver"), false));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt23, true));
        TestCase.assertEquals(3 * DateTimeUtils.HOUR / DateTimeUtils.MILLI, DateTimeUtils.millisOfDay(dstZdt23, false));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt23.toLocalDateTime()));
        TestCase.assertEquals(2 * DateTimeUtils.HOUR / DateTimeUtils.MILLI,
                DateTimeUtils.millisOfDay(dstZdt23.toLocalTime()));
    }

    public void testMicrosOfSecond() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(123456, DateTimeUtils.microsOfSecond(dt2, TZ_JP));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsOfSecond(dt2, null));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsOfSecond(null, TZ_JP));

        TestCase.assertEquals(123456, DateTimeUtils.microsOfSecond(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsOfSecond(null));
    }

    public void testMicrosOfMilli() {
        final Instant dt2 = DateTimeUtils.parseInstant("2023-01-02T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(457, DateTimeUtils.microsOfMilli(dt2));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.microsOfMilli((Instant) null));

        TestCase.assertEquals(457, DateTimeUtils.microsOfMilli(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.microsOfMilli((ZonedDateTime) null));
    }

    public void testAtMidnight() {
        final LocalDateTime ldt = LocalDateTime.of(2023, 2, 3, 4, 5, 6);
        final LocalDate dt1 = LocalDate.of(2023, 2, 3);

        final Instant dt2 = DateTimeUtils.parseInstant("2023-02-03T11:23:45.123456789 JP");
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        final Instant rst2 = DateTimeUtils.parseInstant("2023-02-03T00:00:00 JP");
        final ZonedDateTime rst3 = rst2.atZone(TZ_JP);

        TestCase.assertEquals(rst3, DateTimeUtils.atMidnight(ldt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.atMidnight((LocalDateTime) null, TZ_JP));

        TestCase.assertEquals(rst3, DateTimeUtils.atMidnight(dt1, TZ_JP));
        TestCase.assertNull(DateTimeUtils.atMidnight((LocalDate) null, TZ_JP));

        TestCase.assertEquals(rst2, DateTimeUtils.atMidnight(dt2, TZ_JP));
        TestCase.assertNull(DateTimeUtils.atMidnight(dt2, null));
        TestCase.assertNull(DateTimeUtils.atMidnight((Instant) null, TZ_JP));

        TestCase.assertEquals(rst3, DateTimeUtils.atMidnight(dt3));
        TestCase.assertNull(DateTimeUtils.atMidnight(null));
    }

}
