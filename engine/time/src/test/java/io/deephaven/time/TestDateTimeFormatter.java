/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

public class TestDateTimeFormatter extends BaseArrayTestCase {

    private DateTime t;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        t = DateTimeUtils.convertDateTime("2015-06-13T13:12:11.123456789 MT");
    }

    public void test1() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        TestCase.assertEquals("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS %t", dtf.getPattern());
        TestCase.assertEquals("2015-06-13T14:12:11.123456789 MN", dtf.format(t, TimeZone.TZ_MN));
    }

    public void test2() {
        final boolean isISO = false;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        TestCase.assertEquals("yyyy-MM-dd HH:mm:ss.SSSSSSSSS %t", dtf.getPattern());
        TestCase.assertEquals("2015-06-13 14:12:11.123456789 MN", dtf.format(t, TimeZone.TZ_MN));
    }

    public void test3() {
        final boolean isISO = true;
        final boolean hasDate = false;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        TestCase.assertEquals("HH:mm:ss.SSSSSSSSS %t", dtf.getPattern());
        TestCase.assertEquals("14:12:11.123456789 MN", dtf.format(t, TimeZone.TZ_MN));
    }

    public void test4() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = false;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        TestCase.assertEquals("yyyy-MM-dd %t", dtf.getPattern());
        TestCase.assertEquals("2015-06-13 MN", dtf.format(t, TimeZone.TZ_MN));
    }

    public void test5() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 4;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        TestCase.assertEquals("yyyy-MM-dd'T'HH:mm:ss.SSSS %t", dtf.getPattern());
        TestCase.assertEquals("2015-06-13T14:12:11.1234 MN", dtf.format(t, TimeZone.TZ_MN));
    }

    public void test6() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 2;
        final boolean hasTZ = true;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        TestCase.assertEquals("yyyy-MM-dd'T'HH:mm:ss.SS %t", dtf.getPattern());
        TestCase.assertEquals("2015-06-13T14:12:11.12 MN", dtf.format(t, TimeZone.TZ_MN));
    }

    public void test7() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = false;
        DateTimeFormatter dtf = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);

        TestCase.assertEquals("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS", dtf.getPattern());
        TestCase.assertEquals("2015-06-13T14:12:11.123456789", dtf.format(t, TimeZone.TZ_MN));
    }
}
