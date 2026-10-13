//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.junit.Assert.*;

public class TestDateTimeFormatter {

    private static final ZoneId TZ_MN = ZoneId.of("America/Chicago");

    private Instant t;

    @Before
    public void setUp() throws Exception {
        t = DateTimeUtils.parseInstant("2015-06-13T13:12:11.123456789 MT");
    }

    @Test
    public void test1() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        assertEquals(dtf.toString(), "DateTimeFormatter{pattern='" + dtf.getPattern() + "'}");
        assertEquals("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS %t", dtf.getPattern());
        assertEquals("2015-06-13T14:12:11.123456789 MN", dtf.format(t, TZ_MN));
        assertEquals(dtf.format(t, DateTimeUtils.timeZone()), dtf.format(t));
    }

    @Test
    public void test2() {
        final boolean isISO = false;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        assertEquals(dtf.toString(), "DateTimeFormatter{pattern='" + dtf.getPattern() + "'}");
        assertEquals("yyyy-MM-dd HH:mm:ss.SSSSSSSSS %t", dtf.getPattern());
        assertEquals("2015-06-13 14:12:11.123456789 MN", dtf.format(t, TZ_MN));
        assertEquals(dtf.format(t, DateTimeUtils.timeZone()), dtf.format(t));
    }

    @Test
    public void test3() {
        final boolean isISO = true;
        final boolean hasDate = false;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        assertEquals(dtf.toString(), "DateTimeFormatter{pattern='" + dtf.getPattern() + "'}");
        assertEquals("HH:mm:ss.SSSSSSSSS %t", dtf.getPattern());
        assertEquals("14:12:11.123456789 MN", dtf.format(t, TZ_MN));
        assertEquals(dtf.format(t, DateTimeUtils.timeZone()), dtf.format(t));
    }

    @Test
    public void test4() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = false;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        assertEquals(dtf.toString(), "DateTimeFormatter{pattern='" + dtf.getPattern() + "'}");
        assertEquals("yyyy-MM-dd %t", dtf.getPattern());
        assertEquals("2015-06-13 MN", dtf.format(t, TZ_MN));
        assertEquals(dtf.format(t, DateTimeUtils.timeZone()), dtf.format(t));
    }

    @Test
    public void test5() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 4;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        assertEquals(dtf.toString(), "DateTimeFormatter{pattern='" + dtf.getPattern() + "'}");
        assertEquals("yyyy-MM-dd'T'HH:mm:ss.SSSS %t", dtf.getPattern());
        assertEquals("2015-06-13T14:12:11.1234 MN", dtf.format(t, TZ_MN));
        assertEquals(dtf.format(t, DateTimeUtils.timeZone()), dtf.format(t));
    }

    @Test
    public void test6() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 2;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        assertEquals(dtf.toString(), "DateTimeFormatter{pattern='" + dtf.getPattern() + "'}");
        assertEquals("yyyy-MM-dd'T'HH:mm:ss.SS %t", dtf.getPattern());
        assertEquals("2015-06-13T14:12:11.12 MN", dtf.format(t, TZ_MN));
        assertEquals(dtf.format(t, DateTimeUtils.timeZone()), dtf.format(t));
    }

    @Test
    public void test7() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = false;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        assertEquals(dtf.toString(), "DateTimeFormatter{pattern='" + dtf.getPattern() + "'}");
        assertEquals("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS", dtf.getPattern());
        assertEquals("2015-06-13T14:12:11.123456789", dtf.format(t, TZ_MN));
        assertEquals(dtf.format(t, DateTimeUtils.timeZone()), dtf.format(t));
    }
}
