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

import static io.deephaven.util.QueryConstants.*;

@SuppressWarnings("deprecation")
public class TestDateTimeUtils extends BaseArrayTestCase {
    private static final ZoneId TZ_NY = ZoneId.of("America/New_York");
    private static final ZoneId TZ_JP = ZoneId.of("Asia/Tokyo");
    private static final ZoneId TZ_AL = ZoneId.of("America/Anchorage");
    private static final ZoneId TZ_CT = ZoneId.of("America/Chicago");
    private static final ZoneId TZ_MN = ZoneId.of("America/Chicago");

    public void testConstants() {
        TestCase.assertEquals(0, DateTimeUtils.ZERO_LENGTH_DATETIME_ARRAY.length);
        TestCase.assertEquals(0, DateTimeUtils.ZERO_LENGTH_INSTANT_ARRAY.length);

        TestCase.assertEquals(1_000L, DateTimeUtils.MICRO);
        TestCase.assertEquals(1_000_000L, DateTimeUtils.MILLI);
        TestCase.assertEquals(1_000_000_000L, DateTimeUtils.SECOND);
        TestCase.assertEquals(60_000_000_000L, DateTimeUtils.MINUTE);
        TestCase.assertEquals(60 * 60_000_000_000L, DateTimeUtils.HOUR);
        TestCase.assertEquals(24 * 60 * 60_000_000_000L, DateTimeUtils.DAY);
        TestCase.assertEquals(7 * 24 * 60 * 60_000_000_000L, DateTimeUtils.WEEK);
        TestCase.assertEquals(365 * 24 * 60 * 60_000_000_000L, DateTimeUtils.YEAR);

        TestCase.assertEquals(1.0, DateTimeUtils.SECONDS_PER_NANO * DateTimeUtils.SECOND);
        TestCase.assertEquals(1.0, DateTimeUtils.MINUTES_PER_NANO * DateTimeUtils.MINUTE);
        TestCase.assertEquals(1.0, DateTimeUtils.HOURS_PER_NANO * DateTimeUtils.HOUR);
        TestCase.assertEquals(1.0, DateTimeUtils.DAYS_PER_NANO * DateTimeUtils.DAY);
        TestCase.assertEquals(1.0, DateTimeUtils.YEARS_PER_NANO * DateTimeUtils.YEAR);
    }

    public void testParseDate() {
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("20100102", DateTimeUtils.DateStyle.YMD));

        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("2010-01-02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("01-02-2010", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("02-01-2010", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("2010/01/02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("01/02/2010", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("02/01/2010", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("10-01-02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("01-02-10", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("02-01-10", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("10/01/02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("01/02/10", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDate("02/01/10", DateTimeUtils.DateStyle.DMY));

        assertEquals(DateTimeUtils.parseDate("01/02/03"), DateTimeUtils.parseDate("01/02/03", DateTimeUtils.DateStyle.MDY));

        try {
            DateTimeUtils.parseDate("JUNK", DateTimeUtils.DateStyle.YMD);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseDate(null, DateTimeUtils.DateStyle.YMD);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            DateTimeUtils.parseDate("JUNK", null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }
    }

    public void testParseDateQuiet() {
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("20100102", DateTimeUtils.DateStyle.YMD));

        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("2010-01-02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("01-02-2010", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("02-01-2010", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("2010/01/02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("01/02/2010", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("02/01/2010", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("10-01-02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("01-02-10", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("02-01-10", DateTimeUtils.DateStyle.DMY));

        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("10/01/02", DateTimeUtils.DateStyle.YMD));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("01/02/10", DateTimeUtils.DateStyle.MDY));
        assertEquals(LocalDate.of(2010, 1, 2), DateTimeUtils.parseDateQuiet("02/01/10", DateTimeUtils.DateStyle.DMY));

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

    public void testParseTimeZoneId() {
        TestCase.assertEquals(ZoneId.of("America/Denver"), DateTimeUtils.parseTimeZone("America/Denver"));
        TestCase.assertEquals(ZoneId.of("America/New_York"), DateTimeUtils.parseTimeZone("NY"));
        TestCase.assertEquals(ZoneId.of("Asia/Yerevan"), DateTimeUtils.parseTimeZone("Asia/Yerevan"));

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

    public void testParseTimeZoneIdQuiet() {
        TestCase.assertEquals(ZoneId.of("America/Denver"), DateTimeUtils.parseTimeZoneQuiet("America/Denver"));
        TestCase.assertEquals(ZoneId.of("America/New_York"), DateTimeUtils.parseTimeZoneQuiet("NY"));
        TestCase.assertEquals(ZoneId.of("Asia/Yerevan"), DateTimeUtils.parseTimeZoneQuiet("Asia/Yerevan"));

        TestCase.assertNull(DateTimeUtils.parseTimeZoneQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseTimeZoneQuiet(null));
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

        for (String tz : tzs) {
            for (String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
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

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(DateTime.of(Instant.parse(iso8601)), DateTimeUtils.parseDateTime(iso8601));
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

        for (String tz : tzs) {
            for (String root : roots) {
                final String s = root + " " + tz;
                final ZoneId zid = DateTimeUtils.parseTimeZone(tz);
                final ZonedDateTime zdt = LocalDateTime.parse(root).atZone(zid);
                TestCase.assertEquals("DateTime string: " + s + "'", DateTime.of(zdt.toInstant()), DateTimeUtils.parseDateTimeQuiet(s));
            }
        }

        TestCase.assertNull(DateTimeUtils.parseDateTimeQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseDateTimeQuiet("2010-01-01T12:11"));
        TestCase.assertNull(DateTimeUtils.parseDateTimeQuiet("2010-01-01T12:11 JUNK"));
        TestCase.assertNull(DateTimeUtils.parseDateTimeQuiet(null));

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(DateTime.of(Instant.parse(iso8601)), DateTimeUtils.parseDateTimeQuiet(iso8601));
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

        try {
            DateTimeUtils.parseInstant("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            DateTimeUtils.parseInstant("2010-01-01T12:11");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            DateTimeUtils.parseInstant("2010-01-01T12:11 JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseInstant(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(Instant.parse(iso8601), DateTimeUtils.parseInstant(iso8601));
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
                TestCase.assertEquals("DateTime string: " + s + "'", zdt.toInstant(), DateTimeUtils.parseInstantQuiet(s));
            }
        }

        TestCase.assertNull(DateTimeUtils.parseInstantQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseInstantQuiet("2010-01-01T12:11"));
        TestCase.assertNull(DateTimeUtils.parseInstantQuiet("2010-01-01T12:11 JUNK"));
        TestCase.assertNull(DateTimeUtils.parseInstantQuiet(null));

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(Instant.parse(iso8601), DateTimeUtils.parseInstantQuiet(iso8601));
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

        try {
            DateTimeUtils.parseZonedDateTime("JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            DateTimeUtils.parseZonedDateTime("2010-01-01T12:11");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            DateTimeUtils.parseZonedDateTime("2010-01-01T12:11 JUNK");
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseZonedDateTime(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
        }

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(ZonedDateTime.parse(iso8601), DateTimeUtils.parseZonedDateTime(iso8601));
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

        TestCase.assertNull(DateTimeUtils.parseZonedDateTimeQuiet("JUNK"));
        TestCase.assertNull(DateTimeUtils.parseZonedDateTimeQuiet("2010-01-01T12:11"));
        TestCase.assertNull(DateTimeUtils.parseZonedDateTimeQuiet("2010-01-01T12:11 JUNK"));
        TestCase.assertNull(DateTimeUtils.parseZonedDateTimeQuiet(null));

        final String iso8601 = "2022-04-26T00:30:31.087360Z";
        assertEquals(ZonedDateTime.parse(iso8601), DateTimeUtils.parseZonedDateTimeQuiet(iso8601));
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
                "15:25:49.064106107",
        };

        for (boolean isNeg : new boolean[]{false, true}) {
            for (String t : times) {
                long offset = 0;
                String lts = t;

                if (lts.indexOf(":") == 1) {
                    lts = "0" + lts;
                }

                if (isNeg) {
                    t = "-" + t;
                }

                final long sign = isNeg ? -1 : 1;
                TestCase.assertEquals(sign * (LocalTime.parse(lts).toNanoOfDay() + offset), DateTimeUtils.parseNanos(t));
            }
        }

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
            TestCase.assertEquals(dd.toNanos(), DateTimeUtils.parseNanos(d));
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
                "15:25:49.064106107",
        };

        for (boolean isNeg : new boolean[]{false, true}) {
            for (String t : times) {
                long offset = 0;
                String lts = t;

                if (lts.indexOf(":") == 1) {
                    lts = "0" + lts;
                }

                if (isNeg) {
                    t = "-" + t;
                }

                final long sign = isNeg ? -1 : 1;
                TestCase.assertEquals(t, sign * (LocalTime.parse(lts).toNanoOfDay() + offset), DateTimeUtils.parseNanosQuiet(t));
            }
        }

        final String[] durations = {
                "PT1h43s",
                "-PT1h43s",
        };

        for (String d : durations) {
            final Duration dd = DateTimeUtils.parseDuration(d);
            TestCase.assertEquals(dd.toNanos(), DateTimeUtils.parseNanos(d));
        }

        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseNanosQuiet("JUNK"));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.parseNanosQuiet(null));
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
            //noinspection ConstantConditions
            DateTimeUtils.parsePeriod(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
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

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseDuration(null);
            TestCase.fail("Should throw an exception");
        } catch (Exception ex) {
            //pass
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
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("2021-02-03T11:14:32.1234"));

        TestCase.assertEquals(ChronoField.MINUTE_OF_HOUR, DateTimeUtils.parseTimePrecision("11:14"));
        TestCase.assertEquals(ChronoField.SECOND_OF_MINUTE, DateTimeUtils.parseTimePrecision("11:14:32"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.1"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.12"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.123"));
        TestCase.assertEquals(ChronoField.MILLI_OF_SECOND, DateTimeUtils.parseTimePrecision("11:14:32.1234"));

        try {
            DateTimeUtils.parseTimePrecision("JUNK");
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            //noinspection ConstantConditions
            DateTimeUtils.parseTimePrecision(null);
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
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
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_NY);
        final ZonedDateTime dt4 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt1, TZ_NY));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt1, TZ_JP));

        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt2, TZ_NY));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt2, TZ_JP));

        TestCase.assertEquals("2021-02-03", DateTimeUtils.formatDate(dt3));
        TestCase.assertEquals("2021-02-04", DateTimeUtils.formatDate(dt4));

        TestCase.assertNull(DateTimeUtils.formatDate((DateTime) null, TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDate(dt1, null));

        TestCase.assertNull(DateTimeUtils.formatDate((Instant) null, TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDate(dt2, null));

        TestCase.assertNull(DateTimeUtils.formatDate(null));
    }

    public void testFormatDateTime() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2021-02-03T11:23:32.45678912 NY");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_NY);
        final ZonedDateTime dt4 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals("2021-02-04T01:00:00.000000000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:00 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:00.000000000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:01.000000000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:01 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:01.300000000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:01.3 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456700000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.4567 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456780000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.45678 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789000 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.456789 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789100 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.4567891 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.45678912 NY"), TZ_JP));
        TestCase.assertEquals("2021-02-04T01:23:32.456789123 JP", DateTimeUtils.formatDateTime(DateTimeUtils.parseDateTime("2021-02-03T11:23:32.456789123 NY"), TZ_JP));

        TestCase.assertEquals("2021-02-03T11:23:32.456789120 NY", DateTimeUtils.formatDateTime(dt1, TZ_NY));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP", DateTimeUtils.formatDateTime(dt1, TZ_JP));

        TestCase.assertEquals("2021-02-03T11:23:32.456789120 NY", DateTimeUtils.formatDateTime(dt2, TZ_NY));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP", DateTimeUtils.formatDateTime(dt2, TZ_JP));

        TestCase.assertEquals("2021-02-03T11:23:32.456789120 NY", DateTimeUtils.formatDateTime(dt3));
        TestCase.assertEquals("2021-02-04T01:23:32.456789120 JP", DateTimeUtils.formatDateTime(dt4));


        TestCase.assertEquals("2021-02-03T20:23:32.456789120 Asia/Yerevan", DateTimeUtils.formatDateTime(dt1, ZoneId.of("Asia/Yerevan")));
        TestCase.assertEquals("2021-02-03T20:23:32.456789120 Asia/Yerevan", DateTimeUtils.formatDateTime(dt2, ZoneId.of("Asia/Yerevan")));
        TestCase.assertEquals("2021-02-03T20:23:32.456789120 Asia/Yerevan", DateTimeUtils.formatDateTime(dt3.withZoneSameInstant(ZoneId.of("Asia/Yerevan"))));

        TestCase.assertNull(DateTimeUtils.formatDateTime((DateTime) null, TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDateTime(dt1, null));

        TestCase.assertNull(DateTimeUtils.formatDateTime((Instant) null, TZ_NY));
        TestCase.assertNull(DateTimeUtils.formatDateTime(dt2, null));

        TestCase.assertNull(DateTimeUtils.formatDateTime(null));
    }

    public void testFormatNanos() {

        TestCase.assertEquals("2:00:00", DateTimeUtils.formatNanos(2 * DateTimeUtils.HOUR));
        TestCase.assertEquals("0:02:00", DateTimeUtils.formatNanos(2 * DateTimeUtils.MINUTE));
        TestCase.assertEquals("0:00:02", DateTimeUtils.formatNanos(2 * DateTimeUtils.SECOND));
        TestCase.assertEquals("0:00:00.002000000", DateTimeUtils.formatNanos(2 * DateTimeUtils.MILLI));
        TestCase.assertEquals("0:00:00.000000002", DateTimeUtils.formatNanos(2));
        TestCase.assertEquals("23:45:39.123456789", DateTimeUtils.formatNanos(23 * DateTimeUtils.HOUR + 45 * DateTimeUtils.MINUTE + 39 * DateTimeUtils.SECOND + 123456789));
        TestCase.assertEquals("123:45:39.123456789", DateTimeUtils.formatNanos(123 * DateTimeUtils.HOUR + 45 * DateTimeUtils.MINUTE + 39 * DateTimeUtils.SECOND + 123456789));

        TestCase.assertEquals("-2:00:00", DateTimeUtils.formatNanos(-2 * DateTimeUtils.HOUR));
        TestCase.assertEquals("-0:02:00", DateTimeUtils.formatNanos(-2 * DateTimeUtils.MINUTE));
        TestCase.assertEquals("-0:00:02", DateTimeUtils.formatNanos(-2 * DateTimeUtils.SECOND));
        TestCase.assertEquals("-0:00:00.002000000", DateTimeUtils.formatNanos(-2 * DateTimeUtils.MILLI));
        TestCase.assertEquals("-0:00:00.000000002", DateTimeUtils.formatNanos(-2));
        TestCase.assertEquals("-23:45:39.123456789", DateTimeUtils.formatNanos(-23 * DateTimeUtils.HOUR - 45 * DateTimeUtils.MINUTE - 39 * DateTimeUtils.SECOND - 123456789));
        TestCase.assertEquals("-123:45:39.123456789", DateTimeUtils.formatNanos(-123 * DateTimeUtils.HOUR - 45 * DateTimeUtils.MINUTE - 39 * DateTimeUtils.SECOND - 123456789));

        TestCase.assertNull(DateTimeUtils.formatNanos(NULL_LONG));
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
            //pass
        }

        try {
            DateTimeUtils.microsToNanos(-Long.MAX_VALUE / 2);
            TestCase.fail("Should throw an exception");
        } catch (DateTimeUtils.DateTimeOverflowException ex) {
            //pass
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
            //pass
        }

        try {
            DateTimeUtils.millisToNanos(-Long.MAX_VALUE / 2);
            TestCase.fail("Should throw an exception");
        } catch (DateTimeUtils.DateTimeOverflowException ex) {
            //pass
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
            //pass
        }

        try {
            DateTimeUtils.secondsToNanos(-Long.MAX_VALUE / 2);
            TestCase.fail("Should throw an exception");
        } catch (DateTimeUtils.DateTimeOverflowException ex) {
            //pass
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
        final DateTime dt1 = new DateTime(millis * DateTimeUtils.MILLI);
        final Instant dt2 = Instant.ofEpochSecond(0, millis * DateTimeUtils.MILLI);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(new Date(millis), DateTimeUtils.toDate(dt1));
        TestCase.assertNull(DateTimeUtils.toDate((DateTime) null));

        TestCase.assertEquals(new Date(millis), DateTimeUtils.toDate(dt2));
        TestCase.assertNull(DateTimeUtils.toDate((Instant) null));

        TestCase.assertEquals(new Date(millis), DateTimeUtils.toDate(dt3));
        TestCase.assertNull(DateTimeUtils.toDate((ZonedDateTime) null));
    }

    public void testToDateTime() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDate ld = LocalDate.of(1973, 11, 30);
        final LocalTime lt = LocalTime.of(6, 33, 9, 123456789);
        final Date d = new Date(DateTimeUtils.nanosToMillis(nanos));

        TestCase.assertEquals(dt1, DateTimeUtils.toDateTime(dt2));
        TestCase.assertNull(DateTimeUtils.toDateTime((Instant) null));

        TestCase.assertEquals(dt1, DateTimeUtils.toDateTime(dt3));
        TestCase.assertNull(DateTimeUtils.toDateTime((ZonedDateTime) null));

        TestCase.assertEquals(dt1, DateTimeUtils.toDateTime(ld, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toDateTime(null, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toDateTime(ld, null, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toDateTime(ld, lt, null));

        TestCase.assertEquals(new DateTime((nanos / DateTimeUtils.MILLI) * DateTimeUtils.MILLI), DateTimeUtils.toDateTime(d));
        TestCase.assertNull(DateTimeUtils.toDateTime((Date) null));

        try {
            DateTimeUtils.toDateTime(Instant.ofEpochSecond(0, Long.MAX_VALUE - 5));
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }

        try {
            DateTimeUtils.toDateTime(Instant.ofEpochSecond(0, Long.MAX_VALUE - 5).atZone(TZ_JP));
            TestCase.fail("Should have thrown an exception");
        } catch (Exception ex) {
            //pass
        }

    }

    public void testToInstant() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDate ld = LocalDate.of(1973, 11, 30);
        final LocalTime lt = LocalTime.of(6, 33, 9, 123456789);
        final Date d = new Date(DateTimeUtils.nanosToMillis(nanos));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(dt1));
        TestCase.assertNull(DateTimeUtils.toInstant((DateTime) null));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(dt3));
        TestCase.assertNull(DateTimeUtils.toInstant((ZonedDateTime) null));

        TestCase.assertEquals(dt2, DateTimeUtils.toInstant(ld, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(null, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(ld, null, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toInstant(ld, lt, null));

        TestCase.assertEquals(Instant.ofEpochSecond(0, (nanos / DateTimeUtils.MILLI) * DateTimeUtils.MILLI), DateTimeUtils.toInstant(d));
        TestCase.assertNull(DateTimeUtils.toInstant((Date) null));
    }

    public void testToLocalDate() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDate ld = LocalDate.of(1973, 11, 30);

        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt1, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalDate((DateTime) null, TZ_JP));

        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt2, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalDate((Instant) null, TZ_JP));

        TestCase.assertEquals(ld, DateTimeUtils.toLocalDate(dt3));
        //noinspection ConstantConditions
        TestCase.assertNull(DateTimeUtils.toLocalDate(null));
    }

    public void testToLocalTime() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalTime lt = LocalTime.of(6, 33, 9, 123456789);

        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt1, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalTime((DateTime) null, TZ_JP));

        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt2, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toLocalTime((Instant) null, TZ_JP));

        TestCase.assertEquals(lt, DateTimeUtils.toLocalTime(dt3));
        //noinspection ConstantConditions
        TestCase.assertNull(DateTimeUtils.toLocalTime(null));
    }

    public void testToZonedDateTime() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);
        final LocalDate ld = LocalDate.of(1973, 11, 30);
        final LocalTime lt = LocalTime.of(6, 33, 9, 123456789);

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(dt1, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((DateTime) null, TZ_JP));

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(dt2, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime((Instant) null, TZ_JP));

        TestCase.assertEquals(dt3, DateTimeUtils.toZonedDateTime(ld, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(null, lt, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ld, null, TZ_JP));
        TestCase.assertNull(DateTimeUtils.toZonedDateTime(ld, lt, null));
    }

    public void testEpochNanos() {
        final long nanos = 123456789123456789L;
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

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
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

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
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

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
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

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
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(dt1, DateTimeUtils.epochNanosToDateTime(nanos));
        TestCase.assertNull(DateTimeUtils.epochNanosToDateTime(NULL_LONG));

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
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(dt1, DateTimeUtils.epochMicrosToDateTime(micros));
        TestCase.assertNull(DateTimeUtils.epochMicrosToDateTime(NULL_LONG));

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
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(dt1, DateTimeUtils.epochMillisToDateTime(millis));
        TestCase.assertNull(DateTimeUtils.epochMillisToDateTime(NULL_LONG));

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
        final DateTime dt1 = new DateTime(nanos);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final ZonedDateTime dt3 = dt2.atZone(TZ_JP);

        TestCase.assertEquals(dt1, DateTimeUtils.epochSecondsToDateTime(seconds));
        TestCase.assertNull(DateTimeUtils.epochSecondsToDateTime(NULL_LONG));

        TestCase.assertEquals(dt2, DateTimeUtils.epochSecondsToInstant(seconds));
        TestCase.assertNull(DateTimeUtils.epochSecondsToInstant(NULL_LONG));

        TestCase.assertEquals(dt3, DateTimeUtils.epochSecondsToZonedDateTime(seconds, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochSecondsToZonedDateTime(NULL_LONG, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochSecondsToZonedDateTime(seconds, null));
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
        TestCase.assertEquals((nanos / DateTimeUtils.MICRO) * DateTimeUtils.MICRO, DateTimeUtils.epochAutoToEpochNanos(micros));
        TestCase.assertEquals((nanos / DateTimeUtils.MILLI) * DateTimeUtils.MILLI, DateTimeUtils.epochAutoToEpochNanos(millis));
        TestCase.assertEquals((nanos / DateTimeUtils.SECOND) * DateTimeUtils.SECOND, DateTimeUtils.epochAutoToEpochNanos(seconds));
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

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1, TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(nanos, TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1u, TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(micros, TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1m, TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(millis, TZ_JP));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(dt1s, TZ_JP), DateTimeUtils.epochAutoToZonedDateTime(seconds, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochAutoToZonedDateTime(NULL_LONG, TZ_JP));
        TestCase.assertNull(DateTimeUtils.epochAutoToZonedDateTime(nanos, null));
    }

    public void testToExcelTime() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2010-06-15T16:00:00 NY");
        final Instant dt2 = DateTimeUtils.toInstant(dt1);
        final ZonedDateTime dt3 = DateTimeUtils.toZonedDateTime(dt1, TZ_AL);

        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.666666666664, DateTimeUtils.toExcelTime(dt1, TZ_NY)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt1, TZ_CT)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt1, TZ_MN)));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime((DateTime) null, TZ_MN));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(dt1, null));

        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.666666666664, DateTimeUtils.toExcelTime(dt2, TZ_NY)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt2, TZ_CT)));
        TestCase.assertTrue(
                CompareUtils.doubleEquals(40344.625, DateTimeUtils.toExcelTime(dt2, TZ_MN)));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime((Instant) null, TZ_MN));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(dt2, null));

        TestCase.assertTrue(
                CompareUtils.doubleEquals(DateTimeUtils.toExcelTime(dt2, TZ_AL), DateTimeUtils.toExcelTime(dt3)));
        TestCase.assertEquals(0.0, DateTimeUtils.toExcelTime(null));
    }

    public void testExcelTimeTo() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2010-06-15T16:23:45.678 NY");
        final Instant dt2 = DateTimeUtils.toInstant(dt1);
        final ZonedDateTime dt3 = DateTimeUtils.toZonedDateTime(dt1, TZ_AL);

        TestCase.assertEquals(dt1, DateTimeUtils.excelToDateTime(DateTimeUtils.toExcelTime(dt1, TZ_AL), TZ_AL));
        TestCase.assertTrue(DateTimeUtils.epochMillis(dt1) - DateTimeUtils.epochMillis(DateTimeUtils.excelToDateTime(DateTimeUtils.toExcelTime(dt1, TZ_AL), TZ_AL)) <= 1);
        TestCase.assertNull(DateTimeUtils.excelToDateTime(123.4, null));

        TestCase.assertEquals(dt2, DateTimeUtils.excelToInstant(DateTimeUtils.toExcelTime(dt1, TZ_AL), TZ_AL));
        TestCase.assertTrue(DateTimeUtils.epochMillis(dt2) - DateTimeUtils.epochMillis(DateTimeUtils.excelToInstant(DateTimeUtils.toExcelTime(dt2, TZ_AL), TZ_AL)) <= 1);
        TestCase.assertNull(DateTimeUtils.excelToInstant(123.4, null));

        TestCase.assertEquals(dt3, DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dt1, TZ_AL), TZ_AL));
        TestCase.assertTrue(DateTimeUtils.epochMillis(dt3) - DateTimeUtils.epochMillis(DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dt1, TZ_AL), TZ_AL)) <= 1);
        TestCase.assertNull(DateTimeUtils.excelToZonedDateTime(123.4, null));

        // Test daylight savings time

        final DateTime dstDt1 = DateTimeUtils.parseDateTime("2023-03-12T01:00:00 America/Denver");
        final DateTime dstDt2 = DateTimeUtils.parseDateTime("2023-11-05T01:00:00 America/Denver");
        final Instant dstI1 = DateTimeUtils.parseInstant("2023-03-12T01:00:00 America/Denver");
        final Instant dstI2 = DateTimeUtils.parseInstant("2023-11-05T01:00:00 America/Denver");
        final ZonedDateTime dstZdt1 = DateTimeUtils.toZonedDateTime(dstI1, ZoneId.of("America/Denver"));
        final ZonedDateTime dstZdt2 = DateTimeUtils.toZonedDateTime(dstI2, ZoneId.of("America/Denver"));

        TestCase.assertEquals(dstDt1, DateTimeUtils.excelToDateTime(DateTimeUtils.toExcelTime(dstDt1, ZoneId.of("America/Denver")), ZoneId.of("America/Denver")));
        TestCase.assertEquals(dstDt2, DateTimeUtils.excelToDateTime(DateTimeUtils.toExcelTime(dstDt2, ZoneId.of("America/Denver")), ZoneId.of("America/Denver")));

        TestCase.assertEquals(dstI1, DateTimeUtils.excelToInstant(DateTimeUtils.toExcelTime(dstI1, ZoneId.of("America/Denver")), ZoneId.of("America/Denver")));
        TestCase.assertEquals(dstI2, DateTimeUtils.excelToInstant(DateTimeUtils.toExcelTime(dstI2, ZoneId.of("America/Denver")), ZoneId.of("America/Denver")));

        TestCase.assertEquals(dstZdt1, DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dstZdt1), ZoneId.of("America/Denver")));
        TestCase.assertEquals(dstZdt2, DateTimeUtils.excelToZonedDateTime(DateTimeUtils.toExcelTime(dstZdt2), ZoneId.of("America/Denver")));
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

        final ZonedDateTime z1 = DateTimeUtils.toZonedDateTime(dt1, TZ_AL);
        final ZonedDateTime z2 = DateTimeUtils.toZonedDateTime(dt2, TZ_AL);
        final ZonedDateTime z3 = DateTimeUtils.toZonedDateTime(dt3, TZ_AL);

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

        final ZonedDateTime z1 = DateTimeUtils.toZonedDateTime(dt1, TZ_AL);
        final ZonedDateTime z2 = DateTimeUtils.toZonedDateTime(dt2, TZ_AL);
        final ZonedDateTime z3 = DateTimeUtils.toZonedDateTime(dt3, TZ_AL);

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

        final ZonedDateTime z1 = DateTimeUtils.toZonedDateTime(dt1, TZ_AL);
        final ZonedDateTime z2 = DateTimeUtils.toZonedDateTime(dt2, TZ_AL);
        final ZonedDateTime z3 = DateTimeUtils.toZonedDateTime(dt3, TZ_AL);

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

        final ZonedDateTime z1 = DateTimeUtils.toZonedDateTime(dt1, TZ_AL);
        final ZonedDateTime z2 = DateTimeUtils.toZonedDateTime(dt2, TZ_AL);
        final ZonedDateTime z3 = DateTimeUtils.toZonedDateTime(dt3, TZ_AL);

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
            TestCase.assertEquals(new DateTime((nanos / DateTimeUtils.MILLI) * DateTimeUtils.MILLI), DateTimeUtils.nowMillisResolution());

            TestCase.assertTrue(Math.abs(DateTime.now().getNanos() - DateTimeUtils.nowSystem().getNanos()) < 1_000_000L);
            TestCase.assertTrue(Math.abs(DateTime.nowMillis().getNanos() - DateTimeUtils.nowSystemMillisResolution().getNanos()) < 1_000_000L);

            TestCase.assertEquals(DateTimeUtils.formatDate(new DateTime(nanos), TZ_AL), DateTimeUtils.today(TZ_AL));
            TestCase.assertEquals(DateTimeUtils.today(ZoneId.systemDefault()), DateTimeUtils.today());
        } catch (Exception ex) {
            DateTimeUtils.setClock(initial);
            throw ex;
        }

        DateTimeUtils.setClock(initial);
    }

    public void testTz() {
        final String[][] values = {
                {"NY", "America/New_York"},
                {"MN", "America/Chicago"},
                {"JP", "Asia/Tokyo"},
                {"SG", "Asia/Singapore"},
                {"UTC", "UTC"},
                {"America/Argentina/Buenos_Aires", "America/Argentina/Buenos_Aires"}
        };

        for (final String[] v : values) {
            TestCase.assertEquals(TimeZoneAliases.zoneId(v[0]), DateTimeUtils.tz(v[0]));
            TestCase.assertEquals(TimeZoneAliases.zoneId(v[1]), DateTimeUtils.tz(v[1]));
        }

        TestCase.assertEquals(ZoneId.systemDefault(), DateTimeUtils.tz());
    }

    public void testLowerBin() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DateTime time = DateTimeUtils.parseDateTime("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:14:01 NY"),
                DateTimeUtils.lowerBin(time, second));
        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:10:00 NY"),
                DateTimeUtils.lowerBin(time, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:00:00 NY"),
                DateTimeUtils.lowerBin(time, hour));
        TestCase.assertNull(DateTimeUtils.lowerBin((DateTime) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(time, NULL_LONG));

        final Instant instant = DateTimeUtils.toInstant(time);

        TestCase.assertEquals(DateTimeUtils.toInstant(DateTimeUtils.lowerBin(time, second)),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(instant, second), second));

        TestCase.assertEquals(DateTimeUtils.toInstant(DateTimeUtils.parseDateTime("2010-06-15T06:14:01 NY")),
                DateTimeUtils.lowerBin(instant, second));
        TestCase.assertEquals(DateTimeUtils.toInstant(DateTimeUtils.parseDateTime("2010-06-15T06:10:00 NY")),
                DateTimeUtils.lowerBin(instant, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.toInstant(DateTimeUtils.parseDateTime("2010-06-15T06:00:00 NY")),
                DateTimeUtils.lowerBin(instant, hour));
        TestCase.assertNull(DateTimeUtils.lowerBin((Instant) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(instant, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.lowerBin(instant, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(instant, second), second));

        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(time, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.lowerBin(time, second), TZ_AL),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(zdt, second), second));

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.parseDateTime("2010-06-15T06:14:01 NY"), TZ_AL),
                DateTimeUtils.lowerBin(zdt, second));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.parseDateTime("2010-06-15T06:10:00 NY"), TZ_AL),
                DateTimeUtils.lowerBin(zdt, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.parseDateTime("2010-06-15T06:00:00 NY"), TZ_AL),
                DateTimeUtils.lowerBin(zdt, hour));
        TestCase.assertNull(DateTimeUtils.lowerBin((ZonedDateTime) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(zdt, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.lowerBin(zdt, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(zdt, second), second));

    }

    public void testLowerBinWithOffset() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        DateTime time = DateTimeUtils.parseDateTime("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:11:00 NY"),
                DateTimeUtils.lowerBin(time, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin((DateTime) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(time, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(time, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.lowerBin(time, second, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(time, second, second), second, second));

        final Instant instant = DateTimeUtils.toInstant(time);

        TestCase.assertEquals(DateTimeUtils.toInstant(DateTimeUtils.parseDateTime("2010-06-15T06:11:00 NY")),
                DateTimeUtils.lowerBin(instant, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin((Instant) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(instant, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(instant, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.lowerBin(instant, second, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(instant, second, second), second, second));

        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(time, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.parseDateTime("2010-06-15T06:11:00 NY"), TZ_AL),
                DateTimeUtils.lowerBin(zdt, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin((ZonedDateTime) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(zdt, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.lowerBin(zdt, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.lowerBin(zdt, second, second),
                DateTimeUtils.lowerBin(DateTimeUtils.lowerBin(zdt, second, second), second, second));
    }

    public void testUpperBin() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        DateTime time = DateTimeUtils.parseDateTime("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:14:02 NY"),
                DateTimeUtils.upperBin(time, second));
        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:15:00 NY"),
                DateTimeUtils.upperBin(time, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T07:00:00 NY"),
                DateTimeUtils.upperBin(time, hour));
        TestCase.assertNull(DateTimeUtils.upperBin((DateTime) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.upperBin(time, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(time, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(time, second), second));

        final Instant instant = DateTimeUtils.toInstant(time);

        TestCase.assertEquals(DateTimeUtils.toInstant(DateTimeUtils.parseDateTime("2010-06-15T06:14:02 NY")),
                DateTimeUtils.upperBin(instant, second));
        TestCase.assertEquals(DateTimeUtils.toInstant(DateTimeUtils.parseDateTime("2010-06-15T06:15:00 NY")),
                DateTimeUtils.upperBin(instant, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.toInstant(DateTimeUtils.parseDateTime("2010-06-15T07:00:00 NY")),
                DateTimeUtils.upperBin(instant, hour));
        TestCase.assertNull(DateTimeUtils.upperBin((Instant) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.upperBin(instant, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(instant, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(instant, second), second));

        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(time, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.parseDateTime("2010-06-15T06:14:02 NY"), TZ_AL),
                DateTimeUtils.upperBin(zdt, second));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.parseDateTime("2010-06-15T06:15:00 NY"), TZ_AL),
                DateTimeUtils.upperBin(zdt, 5 * minute));
        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.parseDateTime("2010-06-15T07:00:00 NY"), TZ_AL),
                DateTimeUtils.upperBin(zdt, hour));
        TestCase.assertNull(DateTimeUtils.upperBin((ZonedDateTime) null, 5 * minute));
        TestCase.assertNull(DateTimeUtils.upperBin(zdt, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(zdt, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(zdt, second), second));
    }

    public void testUpperBinWithOffset() {
        final long second = 1000000000L;
        final long minute = 60 * second;
        DateTime time = DateTimeUtils.parseDateTime("2010-06-15T06:14:01.2345 NY");

        TestCase.assertEquals(DateTimeUtils.parseDateTime("2010-06-15T06:16:00 NY"),
                DateTimeUtils.upperBin(time, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin((DateTime) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(time, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(time, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(time, second, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(time, second, second), second, second));

        final Instant instant = DateTimeUtils.toInstant(time);

        TestCase.assertEquals(DateTimeUtils.toInstant(DateTimeUtils.parseDateTime("2010-06-15T06:16:00 NY")),
                DateTimeUtils.upperBin(instant, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin((Instant) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(instant, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(instant, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(instant, second, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(instant, second, second), second, second));

        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(time, TZ_AL);

        TestCase.assertEquals(DateTimeUtils.toZonedDateTime(DateTimeUtils.parseDateTime("2010-06-15T06:16:00 NY"), TZ_AL),
                DateTimeUtils.upperBin(zdt, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin((ZonedDateTime) null, 5 * minute, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(zdt, NULL_LONG, minute));
        TestCase.assertNull(DateTimeUtils.upperBin(zdt, 5 * minute, NULL_LONG));

        TestCase.assertEquals(DateTimeUtils.upperBin(zdt, second, second),
                DateTimeUtils.upperBin(DateTimeUtils.upperBin(zdt, second, second), second, second));
    }

    @SuppressWarnings("ConstantConditions")
    public void testPlus() {
        final DateTime dateTime = DateTimeUtils.parseDateTime("2010-01-01T12:13:14.999123456 JP");
        final Instant instant = DateTimeUtils.toInstant(dateTime);
        final ZonedDateTime zdt = DateTimeUtils.toZonedDateTime(dateTime, TZ_AL);

        TestCase.assertEquals(dateTime.getNanos() + 54321L, DateTimeUtils.plus(dateTime, 54321L).getNanos());
        TestCase.assertEquals(dateTime.getNanos() - 54321L, DateTimeUtils.plus(dateTime, -54321L).getNanos());

        TestCase.assertEquals(dateTime.getNanos() + 54321L, DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, 54321L)));
        TestCase.assertEquals(dateTime.getNanos() - 54321L, DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, -54321L)));

        TestCase.assertEquals(dateTime.getNanos() + 54321L, DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, 54321L)));
        TestCase.assertEquals(dateTime.getNanos() - 54321L, DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, -54321L)));

        Period period = Period.parse("P1D");
        Duration duration = Duration.parse("PT1h");

        TestCase.assertEquals(dateTime.getNanos() + DateTimeUtils.DAY, DateTimeUtils.plus(dateTime, period).getNanos());
        TestCase.assertEquals(dateTime.getNanos() + 3600000000000L, DateTimeUtils.plus(dateTime, duration).getNanos());

        TestCase.assertEquals(dateTime.getNanos() + DateTimeUtils.DAY, DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, period)));
        TestCase.assertEquals(dateTime.getNanos() + 3600000000000L, DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, duration)));

        TestCase.assertEquals(dateTime.getNanos() + DateTimeUtils.DAY, DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, period)));
        TestCase.assertEquals(dateTime.getNanos() + 3600000000000L, DateTimeUtils.epochNanos(DateTimeUtils.plus(dateTime, duration)));

        period = Period.parse("-P1D");
        duration = Duration.parse("PT-1h");

        TestCase.assertEquals(dateTime.getNanos() - DateTimeUtils.DAY, DateTimeUtils.plus(dateTime, period).getNanos());
        TestCase.assertEquals(dateTime.getNanos() - 3600000000000L, DateTimeUtils.plus(dateTime, duration).getNanos());

        TestCase.assertEquals(dateTime.getNanos() - DateTimeUtils.DAY, DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, period)));
        TestCase.assertEquals(dateTime.getNanos() - 3600000000000L, DateTimeUtils.epochNanos(DateTimeUtils.plus(instant, duration)));

        TestCase.assertEquals(dateTime.getNanos() - DateTimeUtils.DAY, DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, period)));
        TestCase.assertEquals(dateTime.getNanos() - 3600000000000L, DateTimeUtils.epochNanos(DateTimeUtils.plus(zdt, duration)));

        TestCase.assertNull(DateTimeUtils.plus(dateTime, NULL_LONG));
        TestCase.assertNull(DateTimeUtils.plus(dateTime, (Period) null));
        TestCase.assertNull(DateTimeUtils.plus(dateTime, (Duration) null));
        TestCase.assertNull(DateTimeUtils.plus((DateTime) null, period));
        TestCase.assertNull(DateTimeUtils.plus((DateTime) null, duration));

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
        DateTimeUtils.plus(new DateTime(Long.MAX_VALUE - 10), 10); // edge at max
        try {
            DateTimeUtils.plus(new DateTime(Long.MAX_VALUE), 1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(new DateTime(Long.MAX_VALUE), Period.ofDays(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(new DateTime(Long.MAX_VALUE), Duration.ofNanos(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

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

        DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31), LocalTime.of(23, 59, 59, 999_999_999 - 10), ZoneId.of("UTC")), 10); // edge at max
        try {
            DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31), LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), 1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31), LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), Period.ofDays(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31), LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), Duration.ofNanos(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        DateTimeUtils.plus(new DateTime(Long.MIN_VALUE + 10), -10); // edge at min
        try {
            DateTimeUtils.plus(new DateTime(Long.MIN_VALUE), -1);
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

        DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 10), ZoneId.of("UTC")), -10); // edge at max
        try {
            DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")), -1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")), Period.ofDays(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.plus(ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")), Duration.ofNanos(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

    }

    @SuppressWarnings("ConstantConditions")
    public void testMinus() {
        final DateTime dateTime1 = DateTimeUtils.parseDateTime("2010-01-01T12:13:14.999123456 JP");
        final DateTime dateTime2 = DateTimeUtils.parseDateTime("2010-01-01T13:13:14.999123456 JP");
        final Instant instant1 = DateTimeUtils.toInstant(dateTime1);
        final Instant instant2 = DateTimeUtils.toInstant(dateTime2);
        final ZonedDateTime zdt1 = DateTimeUtils.toZonedDateTime(dateTime1, TZ_AL);
        final ZonedDateTime zdt2 = DateTimeUtils.toZonedDateTime(dateTime2, TZ_AL);

        TestCase.assertEquals(dateTime1.getNanos() - 54321L, DateTimeUtils.minus(dateTime1, 54321L).getNanos());
        TestCase.assertEquals(dateTime2.getNanos() + 54321L, DateTimeUtils.minus(dateTime2, -54321L).getNanos());

        TestCase.assertEquals(dateTime1.getNanos() - 54321L, DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, 54321L)));
        TestCase.assertEquals(dateTime2.getNanos() + 54321L, DateTimeUtils.epochNanos(DateTimeUtils.minus(instant2, -54321L)));

        TestCase.assertEquals(dateTime1.getNanos() - 54321L, DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, 54321L)));
        TestCase.assertEquals(dateTime2.getNanos() + 54321L, DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt2, -54321L)));

        TestCase.assertEquals(-3600000000000L, DateTimeUtils.minus(dateTime1, dateTime2));
        TestCase.assertEquals(3600000000000L, DateTimeUtils.minus(dateTime2, dateTime1));

        TestCase.assertEquals(-3600000000000L, DateTimeUtils.minus(instant1, instant2));
        TestCase.assertEquals(3600000000000L, DateTimeUtils.minus(instant2, instant1));

        TestCase.assertEquals(-3600000000000L, DateTimeUtils.minus(zdt1, zdt2));
        TestCase.assertEquals(3600000000000L, DateTimeUtils.minus(zdt2, zdt1));

        Period period = Period.parse("P1D");
        Duration duration = Duration.parse("PT1h");

        TestCase.assertEquals(dateTime1.getNanos() - DateTimeUtils.DAY, DateTimeUtils.minus(dateTime1, period).getNanos());
        TestCase.assertEquals(dateTime1.getNanos() - 3600000000000L, DateTimeUtils.minus(dateTime1, duration).getNanos());

        TestCase.assertEquals(dateTime1.getNanos() - DateTimeUtils.DAY, DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, period)));
        TestCase.assertEquals(dateTime1.getNanos() - 3600000000000L, DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, duration)));

        TestCase.assertEquals(dateTime1.getNanos() - DateTimeUtils.DAY, DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, period)));
        TestCase.assertEquals(dateTime1.getNanos() - 3600000000000L, DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, duration)));

        period = Period.parse("-P1D");
        duration = Duration.parse("PT-1h");

        TestCase.assertEquals(dateTime1.getNanos() + DateTimeUtils.DAY, DateTimeUtils.minus(dateTime1, period).getNanos());
        TestCase.assertEquals(dateTime1.getNanos() + 3600000000000L, DateTimeUtils.minus(dateTime1, duration).getNanos());

        TestCase.assertEquals(dateTime1.getNanos() + DateTimeUtils.DAY, DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, period)));
        TestCase.assertEquals(dateTime1.getNanos() + 3600000000000L, DateTimeUtils.epochNanos(DateTimeUtils.minus(instant1, duration)));

        TestCase.assertEquals(dateTime1.getNanos() + DateTimeUtils.DAY, DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, period)));
        TestCase.assertEquals(dateTime1.getNanos() + 3600000000000L, DateTimeUtils.epochNanos(DateTimeUtils.minus(zdt1, duration)));

        TestCase.assertNull(DateTimeUtils.minus(dateTime1, NULL_LONG));
        TestCase.assertNull(DateTimeUtils.minus(dateTime1, (Period) null));
        TestCase.assertNull(DateTimeUtils.minus(dateTime1, (Duration) null));
        TestCase.assertNull(DateTimeUtils.minus((DateTime) null, period));
        TestCase.assertNull(DateTimeUtils.minus((DateTime) null, duration));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.minus(dateTime1, (DateTime) null));

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
        DateTimeUtils.minus(new DateTime(Long.MAX_VALUE - 10), -10); // edge at max
        try {
            DateTimeUtils.minus(new DateTime(Long.MAX_VALUE), -1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(new DateTime(Long.MAX_VALUE), Period.ofDays(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(new DateTime(Long.MAX_VALUE), Duration.ofNanos(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

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

        DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31), LocalTime.of(23, 59, 59, 999_999_999 - 10), ZoneId.of("UTC")), 10); // edge at max
        try {
            DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31), LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), -1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31), LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), Period.ofDays(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MAX_VALUE, 12, 31), LocalTime.of(23, 59, 59, 999_999_999), ZoneId.of("UTC")), Duration.ofNanos(-1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

        DateTimeUtils.minus(new DateTime(Long.MIN_VALUE + 10), 10); // edge at min
        try {
            DateTimeUtils.minus(new DateTime(Long.MIN_VALUE), 1);
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

        DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 10), ZoneId.of("UTC")), -10); // edge at max
        try {
            DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")), 1);
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")), Period.ofDays(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }
        try {
            DateTimeUtils.minus(ZonedDateTime.of(LocalDate.of(Year.MIN_VALUE, 1, 1), LocalTime.of(0, 0, 0, 0), ZoneId.of("UTC")), Duration.ofNanos(1));
            TestCase.fail("This should have overflowed");
        } catch (DateTimeUtils.DateTimeOverflowException e) {
            // ok
        }

    }

    public void testDiffNanos() {
        final DateTime dt1 = new DateTime(12345678987654321L);
        final DateTime dt2 = new DateTime(98765432123456789L);
        final long delta = dt2.getNanos() - dt1.getNanos();

        TestCase.assertEquals(delta, DateTimeUtils.diffNanos(dt1, dt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffNanos(dt2, dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(null, dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(dt2, null));

        final Instant i1 = dt1.toInstant();
        final Instant i2 = dt2.toInstant();

        TestCase.assertEquals(delta, DateTimeUtils.diffNanos(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffNanos(i2, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(null, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(i2, null));

        final ZonedDateTime zdt1 = dt1.toZonedDateTime(TZ_AL);
        final ZonedDateTime zdt2 = dt2.toZonedDateTime(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffNanos(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffNanos(zdt2, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(null, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffNanos(zdt2, null));
    }

    public void testDiffMicros() {
        final DateTime dt1 = new DateTime(12345678987654321L);
        final DateTime dt2 = new DateTime(98765432123456789L);
        final long delta = (dt2.getNanos() - dt1.getNanos()) / DateTimeUtils.MICRO;

        TestCase.assertEquals(delta, DateTimeUtils.diffMicros(dt1, dt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMicros(dt2, dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(null, dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(dt2, null));

        final Instant i1 = dt1.toInstant();
        final Instant i2 = dt2.toInstant();

        TestCase.assertEquals(delta, DateTimeUtils.diffMicros(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMicros(i2, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(null, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(i2, null));

        final ZonedDateTime zdt1 = dt1.toZonedDateTime(TZ_AL);
        final ZonedDateTime zdt2 = dt2.toZonedDateTime(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffMicros(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMicros(zdt2, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(null, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMicros(zdt2, null));
    }

    public void testDiffMillis() {
        final DateTime dt1 = new DateTime(12345678987654321L);
        final DateTime dt2 = new DateTime(98765432123456789L);
        final long delta = (dt2.getNanos() - dt1.getNanos()) / DateTimeUtils.MILLI;

        TestCase.assertEquals(delta, DateTimeUtils.diffMillis(dt1, dt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMillis(dt2, dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(null, dt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(dt2, null));

        final Instant i1 = dt1.toInstant();
        final Instant i2 = dt2.toInstant();

        TestCase.assertEquals(delta, DateTimeUtils.diffMillis(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMillis(i2, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(null, i1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(i2, null));

        final ZonedDateTime zdt1 = dt1.toZonedDateTime(TZ_AL);
        final ZonedDateTime zdt2 = dt2.toZonedDateTime(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffMillis(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMillis(zdt2, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(null, zdt1));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.diffMillis(zdt2, null));
    }

    public void testDiffSeconds() {
        final DateTime dt1 = new DateTime(12345678987654321L);
        final DateTime dt2 = new DateTime(98765432123456789L);
        final double delta = (dt2.getNanos() - dt1.getNanos()) / (double) DateTimeUtils.SECOND;

        TestCase.assertEquals(delta, DateTimeUtils.diffSeconds(dt1, dt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffSeconds(dt2, dt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(null, dt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(dt2, null));

        final Instant i1 = dt1.toInstant();
        final Instant i2 = dt2.toInstant();

        TestCase.assertEquals(delta, DateTimeUtils.diffSeconds(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffSeconds(i2, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(null, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(i2, null));

        final ZonedDateTime zdt1 = dt1.toZonedDateTime(TZ_AL);
        final ZonedDateTime zdt2 = dt2.toZonedDateTime(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffSeconds(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffSeconds(zdt2, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(null, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffSeconds(zdt2, null));
    }

    public void testDiffMinutes() {
        final DateTime dt1 = new DateTime(12345678987654321L);
        final DateTime dt2 = new DateTime(98765432123456789L);
        final double delta = (dt2.getNanos() - dt1.getNanos()) / (double) DateTimeUtils.MINUTE;

        TestCase.assertEquals(delta, DateTimeUtils.diffMinutes(dt1, dt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMinutes(dt2, dt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(null, dt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(dt2, null));

        final Instant i1 = dt1.toInstant();
        final Instant i2 = dt2.toInstant();

        TestCase.assertEquals(delta, DateTimeUtils.diffMinutes(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMinutes(i2, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(null, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(i2, null));

        final ZonedDateTime zdt1 = dt1.toZonedDateTime(TZ_AL);
        final ZonedDateTime zdt2 = dt2.toZonedDateTime(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffMinutes(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffMinutes(zdt2, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(null, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffMinutes(zdt2, null));
    }

    public void testDiffDays() {
        final DateTime dt1 = new DateTime(12345678987654321L);
        final DateTime dt2 = new DateTime(98765432123456789L);
        final double delta = (dt2.getNanos() - dt1.getNanos()) / (double) DateTimeUtils.DAY;

        TestCase.assertEquals(delta, DateTimeUtils.diffDays(dt1, dt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffDays(dt2, dt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(null, dt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(dt2, null));

        final Instant i1 = dt1.toInstant();
        final Instant i2 = dt2.toInstant();

        TestCase.assertEquals(delta, DateTimeUtils.diffDays(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffDays(i2, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(null, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(i2, null));

        final ZonedDateTime zdt1 = dt1.toZonedDateTime(TZ_AL);
        final ZonedDateTime zdt2 = dt2.toZonedDateTime(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffDays(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffDays(zdt2, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(null, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffDays(zdt2, null));
    }

    public void testDiffYears() {
        final DateTime dt1 = new DateTime(12345678987654321L);
        final DateTime dt2 = new DateTime(98765432123456789L);
        final double delta = (dt2.getNanos() - dt1.getNanos()) / (double) DateTimeUtils.YEAR;

        TestCase.assertEquals(delta, DateTimeUtils.diffYears(dt1, dt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffYears(dt2, dt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears(null, dt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears(dt2, null));

        final Instant i1 = dt1.toInstant();
        final Instant i2 = dt2.toInstant();

        TestCase.assertEquals(delta, DateTimeUtils.diffYears(i1, i2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffYears(i2, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears(null, i1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears(i2, null));

        final ZonedDateTime zdt1 = dt1.toZonedDateTime(TZ_AL);
        final ZonedDateTime zdt2 = dt2.toZonedDateTime(TZ_AL);

        TestCase.assertEquals(delta, DateTimeUtils.diffYears(zdt1, zdt2));
        TestCase.assertEquals(-delta, DateTimeUtils.diffYears(zdt2, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears(null, zdt1));
        TestCase.assertEquals(NULL_DOUBLE, DateTimeUtils.diffYears(zdt2, null));
    }

    public void testYear() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-01-02T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(2023, DateTimeUtils.year(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year((DateTime) null, TZ_JP));

        TestCase.assertEquals(2023, DateTimeUtils.year(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year((Instant) null, TZ_JP));

        TestCase.assertEquals(2023, DateTimeUtils.year(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.year(null));
    }

    public void testYearOfCentury() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-01-02T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(23, DateTimeUtils.yearOfCentury(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury((DateTime) null, TZ_JP));

        TestCase.assertEquals(23, DateTimeUtils.yearOfCentury(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury((Instant) null, TZ_JP));

        TestCase.assertEquals(23, DateTimeUtils.yearOfCentury(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.yearOfCentury(null));
    }

    public void testMonthOfYear() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(2, DateTimeUtils.monthOfYear(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear((DateTime) null, TZ_JP));

        TestCase.assertEquals(2, DateTimeUtils.monthOfYear(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear((Instant) null, TZ_JP));

        TestCase.assertEquals(2, DateTimeUtils.monthOfYear(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.monthOfYear(null));
    }

    public void testDayOfMonth() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(3, DateTimeUtils.dayOfMonth(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth((DateTime) null, TZ_JP));

        TestCase.assertEquals(3, DateTimeUtils.dayOfMonth(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth((Instant) null, TZ_JP));

        TestCase.assertEquals(3, DateTimeUtils.dayOfMonth(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfMonth(null));
    }

    public void testDayOfWeek() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(DayOfWeek.FRIDAY.getValue(), DateTimeUtils.dayOfWeek(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeek(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeek((DateTime) null, TZ_JP));

        TestCase.assertEquals(DayOfWeek.FRIDAY.getValue(), DateTimeUtils.dayOfWeek(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeek(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeek((Instant) null, TZ_JP));

        TestCase.assertEquals(DayOfWeek.FRIDAY.getValue(), DateTimeUtils.dayOfWeek(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfWeek(null));
    }

    public void testDayOfYear() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(34, DateTimeUtils.dayOfYear(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear((DateTime) null, TZ_JP));

        TestCase.assertEquals(34, DateTimeUtils.dayOfYear(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear((Instant) null, TZ_JP));

        TestCase.assertEquals(34, DateTimeUtils.dayOfYear(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.dayOfYear(null));
    }

    public void testHourOfDay() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(11, DateTimeUtils.hourOfDay(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay((DateTime) null, TZ_JP));

        TestCase.assertEquals(11, DateTimeUtils.hourOfDay(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay((Instant) null, TZ_JP));

        TestCase.assertEquals(11, DateTimeUtils.hourOfDay(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.hourOfDay(null));
    }

    public void testMinuteOfHour() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(23, DateTimeUtils.minuteOfHour(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfHour(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfHour((DateTime) null, TZ_JP));

        TestCase.assertEquals(23, DateTimeUtils.minuteOfHour(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfHour(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfHour((Instant) null, TZ_JP));

        TestCase.assertEquals(23, DateTimeUtils.minuteOfHour(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfHour(null));
    }

    public void testMinuteOfDay() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(11 * 60 + 23, DateTimeUtils.minuteOfDay(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay((DateTime) null, TZ_JP));

        TestCase.assertEquals(11 * 60 + 23, DateTimeUtils.minuteOfDay(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay((Instant) null, TZ_JP));

        TestCase.assertEquals(11 * 60 + 23, DateTimeUtils.minuteOfDay(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.minuteOfDay(null));
    }

    public void testSecondOfMinute() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(45, DateTimeUtils.secondOfMinute(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfMinute(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfMinute((DateTime) null, TZ_JP));

        TestCase.assertEquals(45, DateTimeUtils.secondOfMinute(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfMinute(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfMinute((Instant) null, TZ_JP));

        TestCase.assertEquals(45, DateTimeUtils.secondOfMinute(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfMinute(null));
    }

    public void testSecondOfDay() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(11 * 60 * 60 + 23 * 60 + 45, DateTimeUtils.secondOfDay(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay((DateTime) null, TZ_JP));

        TestCase.assertEquals(11 * 60 * 60 + 23 * 60 + 45, DateTimeUtils.secondOfDay(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay((Instant) null, TZ_JP));

        TestCase.assertEquals(11 * 60 * 60 + 23 * 60 + 45, DateTimeUtils.secondOfDay(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.secondOfDay(null));
    }

    public void testNanosOfSecond() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(123456789, DateTimeUtils.nanosOfSecond(dt1, TZ_JP));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfSecond(dt1, null));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfSecond((DateTime) null, TZ_JP));

        TestCase.assertEquals(123456789, DateTimeUtils.nanosOfSecond(dt2, TZ_JP));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfSecond(dt2, null));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfSecond((Instant) null, TZ_JP));

        TestCase.assertEquals(123456789, DateTimeUtils.nanosOfSecond(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfSecond(null));
    }

    public void testNanosOfMilli() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(123456789 % DateTimeUtils.MILLI, DateTimeUtils.nanosOfMilli(dt1));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.nanosOfMilli((DateTime) null));

        TestCase.assertEquals(123456789 % DateTimeUtils.MILLI, DateTimeUtils.nanosOfMilli(dt2));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.nanosOfMilli((Instant) null));

        TestCase.assertEquals(123456789 % DateTimeUtils.MILLI, DateTimeUtils.nanosOfMilli(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.nanosOfMilli((ZonedDateTime) null));
    }

    public void testNanosOfDay() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(123456789L + 1_000_000_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.nanosOfDay(dt1, TZ_JP));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay(dt1, null));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay((DateTime) null, TZ_JP));

        TestCase.assertEquals(123456789L + 1_000_000_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.nanosOfDay(dt2, TZ_JP));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay(dt2, null));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay((Instant) null, TZ_JP));

        TestCase.assertEquals(123456789L + 1_000_000_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.nanosOfDay(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.nanosOfDay(null));
    }

    public void testMillisOfSecond() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(123, DateTimeUtils.millisOfSecond(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfSecond(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfSecond((DateTime) null, TZ_JP));

        TestCase.assertEquals(123, DateTimeUtils.millisOfSecond(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfSecond(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfSecond((Instant) null, TZ_JP));

        TestCase.assertEquals(123, DateTimeUtils.millisOfSecond(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfSecond(null));
    }

    public void testMillisOfDay() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(123L + 1_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.millisOfDay(dt1, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay(dt1, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay((DateTime) null, TZ_JP));

        TestCase.assertEquals(123L + 1_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.millisOfDay(dt2, TZ_JP));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay(dt2, null));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay((Instant) null, TZ_JP));

        TestCase.assertEquals(123L + 1_000L * (45 + 23 * 60 + 11 * 60 * 60), DateTimeUtils.millisOfDay(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.millisOfDay(null));
    }

    public void testMicrosOfSecond() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(123456, DateTimeUtils.microsOfSecond(dt1, TZ_JP));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsOfSecond(dt1, null));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsOfSecond((DateTime) null, TZ_JP));

        TestCase.assertEquals(123456, DateTimeUtils.microsOfSecond(dt2, TZ_JP));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsOfSecond(dt2, null));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsOfSecond((Instant) null, TZ_JP));

        TestCase.assertEquals(123456, DateTimeUtils.microsOfSecond(dt3));
        TestCase.assertEquals(NULL_LONG, DateTimeUtils.microsOfSecond(null));
    }

    public void testMicrosOfMilli() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(457, DateTimeUtils.microsOfMilli(dt1));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.microsOfMilli((DateTime) null));

        TestCase.assertEquals(457, DateTimeUtils.microsOfMilli(dt2));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.microsOfMilli((Instant) null));

        TestCase.assertEquals(457, DateTimeUtils.microsOfMilli(dt3));
        TestCase.assertEquals(NULL_INT, DateTimeUtils.microsOfMilli((ZonedDateTime) null));
    }

    public void testDateTimeAtMidnight() {
        final DateTime dt1 = DateTimeUtils.parseDateTime("2023-02-03T11:23:45.123456789 JP");
        final Instant dt2 = dt1.toInstant();
        final ZonedDateTime dt3 = dt1.toZonedDateTime(TZ_JP);

        final DateTime rst1 = DateTimeUtils.parseDateTime("2023-02-03T00:00:00 JP");
        final Instant rst2 = rst1.toInstant();
        final ZonedDateTime rst3 = rst1.toZonedDateTime(TZ_JP);

        TestCase.assertEquals(rst1, DateTimeUtils.dateTimeAtMidnight(dt1, TZ_JP));
        TestCase.assertNull(DateTimeUtils.dateTimeAtMidnight(dt1, null));
        TestCase.assertNull(DateTimeUtils.dateTimeAtMidnight((DateTime) null, TZ_JP));

        TestCase.assertEquals(rst2, DateTimeUtils.dateTimeAtMidnight(dt2, TZ_JP));
        TestCase.assertNull(DateTimeUtils.dateTimeAtMidnight(dt2, null));
        TestCase.assertNull(DateTimeUtils.dateTimeAtMidnight((Instant) null, TZ_JP));

        TestCase.assertEquals(rst3, DateTimeUtils.dateTimeAtMidnight(dt3));
        TestCase.assertNull(DateTimeUtils.dateTimeAtMidnight(null));
    }

}
