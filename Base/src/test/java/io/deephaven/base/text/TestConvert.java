/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.text;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

@SuppressWarnings("UnusedAssignment")
public class TestConvert extends TestCase {

    StringBuilder sb = new StringBuilder();

    private int assertArrayEqual(byte[] b, int pos, int next, String expected) {
        sb.setLength(0);
        while (pos < next) {
            sb.append((char) b[pos++]);
        }
        assertEquals(expected, sb.toString());
        assertEquals(next, pos);
        return pos;
    }

    private int assertBufferEqual(ByteBuffer b, int pos, String expected) {
        sb.setLength(0);
        while (pos < b.position()) {
            sb.append((char) b.get(pos++));
        }
        assertEquals(expected, sb.toString());
        return pos;
    }

    // ----------------------------------------------------------------------------------------------
    // appendShort
    // ----------------------------------------------------------------------------------------------

    public void testAppendShortBuffer() {
        ByteBuffer b = ByteBuffer.allocate(4096);
        b.clear();
        int pos = b.position();

        Convert.appendShort((short) 0, b);
        pos = assertBufferEqual(b, pos, "0");

        Convert.appendShort((short) -1, b);
        pos = assertBufferEqual(b, pos, "-1");

        Convert.appendShort((short) 42, b);
        pos = assertBufferEqual(b, pos, "42");

        Convert.appendShort(Short.MAX_VALUE, b);
        pos = assertBufferEqual(b, pos, "32767");

        Convert.appendShort(Short.MIN_VALUE, b);
        pos = assertBufferEqual(b, pos, "-32768");
    }

    // ----------------------------------------------------------------------------------------------
    // appendInt
    // ----------------------------------------------------------------------------------------------

    public void testAppendIntBuffer() {
        ByteBuffer b = ByteBuffer.allocate(4096);
        b.clear();
        int pos = b.position();

        Convert.appendInt(0, b);
        pos = assertBufferEqual(b, pos, "0");

        Convert.appendInt(-1, b);
        pos = assertBufferEqual(b, pos, "-1");

        Convert.appendInt(42, b);
        pos = assertBufferEqual(b, pos, "42");

        Convert.appendInt(Integer.MAX_VALUE, b);
        pos = assertBufferEqual(b, pos, "2147483647");

        Convert.appendInt(Integer.MIN_VALUE, b);
        pos = assertBufferEqual(b, pos, "-2147483648");
    }

    // ----------------------------------------------------------------------------------------------
    // appendLong
    // ----------------------------------------------------------------------------------------------

    public void testAppendLongBuffer() {
        ByteBuffer b = ByteBuffer.allocate(4096);
        b.clear();
        int pos = b.position();

        Convert.appendLong(0, b);
        pos = assertBufferEqual(b, pos, "0");

        Convert.appendLong(-1, b);
        pos = assertBufferEqual(b, pos, "-1");

        Convert.appendLong(42, b);
        pos = assertBufferEqual(b, pos, "42");

        Convert.appendLong(Long.MAX_VALUE, b);
        pos = assertBufferEqual(b, pos, "9223372036854775807");

        Convert.appendLong(Long.MIN_VALUE, b);
        pos = assertBufferEqual(b, pos, "-9223372036854775808");
    }

    // ----------------------------------------------------------------------------------------------
    // appendDouble
    // ----------------------------------------------------------------------------------------------

    public void testAppendDoubleBuffer() {
        ByteBuffer b = ByteBuffer.allocate(4096);
        b.clear();
        int pos = b.position();

        Convert.appendDouble(0, b);
        pos = assertBufferEqual(b, pos, "0.0");

        Convert.appendDouble(-1, b);
        pos = assertBufferEqual(b, pos, "-1.0");

        Convert.appendDouble(42, b);
        pos = assertBufferEqual(b, pos, "42.0");

        Convert.appendDouble(0.001, b);
        pos = assertBufferEqual(b, pos, "0.001");

        Convert.appendDouble(0.00099, b);
        pos = assertBufferEqual(b, pos, "9.9E-4");

        Convert.appendDouble(9999999.0, b);
        pos = assertBufferEqual(b, pos, "9999999.0");

        Convert.appendDouble(10000000.0, b);
        pos = assertBufferEqual(b, pos, "1.0E7");

        Convert.appendDouble(0.000999, b);
        pos = assertBufferEqual(b, pos, "9.99E-4"); // not 9.990000000000001E-4

        Convert.appendDouble(777777.0 / 100000000.0, b);
        pos = assertBufferEqual(b, pos, "0.00777777"); // not 0.0077777699999999998
    }

    // ################################################################

    public void testAppendISO8601Millis() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        long[] testCases = {
                0L, 1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, 100000000L,
                1000000000L, 10000000000L, 100000000000L, 1000000000000L, 10000000000000L,
                100000000000000L, 253402300799999L,
                -1L, -10L, -100L, -1000L, -10000L, -100000L, -1000000L, -10000000L, -100000000L,
                -1000000000L, -10000000000L, -100000000000L, -1000000000000L, -10000000000000L,
        };
        for (long testCase : testCases) {
            checkAppendIso8601Millis(testCase, dateFormat.format(testCase));
        }

        checkAppendIso8601Millis(-62167132799999L, "0000-01-01T00:00:00.001");
        checkAppendIso8601Millis(-62167132800000L, "0000-01-01T00:00:00.000");
        checkAppendIso8601Millis(-62167132800001L, "0000-00-00T00:00:00.000");

        checkAppendIso8601Millis(253402300799999L, "9999-12-31T23:59:59.999");
        checkAppendIso8601Millis(253402300800000L, "9999-99-99T99:99:99.999");

        checkAppendIso8601Millis(dateFormat.parse("2004-02-20T11:12:13.014").getTime(),
            "2004-02-20T11:12:13.014");
        checkAppendIso8601Millis(dateFormat.parse("2000-02-20T11:12:13.014").getTime(),
            "2000-02-20T11:12:13.014");
        checkAppendIso8601Millis(dateFormat.parse("1900-02-20T11:12:13.014").getTime(),
            "1900-02-20T11:12:13.014");

        {
            ByteBuffer byteBuffer = ByteBuffer.allocate(100);
            assertSame(byteBuffer, Convert.appendISO8601Millis(0,
                new byte[] {'_', 's', 'u', 'f', 'f', 'i', 'x'}, byteBuffer));
            assertBufferEqual(byteBuffer, 0, "1970-01-01T00:00:00.000_suffix");
        }
    }

    private void checkAppendIso8601Millis(long millis, String expectedString) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        assertSame(byteBuffer, Convert.appendISO8601Millis(millis, null, byteBuffer));
        assertBufferEqual(byteBuffer, 0, expectedString);
    }

    public void testAppendISO8601Micros() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        long[] testCases = {
                0L,
                1000L,
                10000L,
                100000L,
                1000000L,
                10000000L,
                100000000L,
                1000000000L,
                10000000000L,
                100000000000L,
                1000000000000L,
                10000000000000L,
                100000000000000L,
                1000000000000000L,
                10000000000000000L,
                253402300799999000L,
                -1000L,
                -10000L,
                -100000L,
                -1000000L,
                -10000000L,
                -100000000L,
                -1000000000L,
                -10000000000L,
                -100000000000L,
                -1000000000000L,
                -10000000000000L,
                -100000000000000L,
                -1000000000000000L,
                -10000000000000000L
        };
        for (long testCase : testCases) {
            checkAppendIso8601Micros(testCase, dateFormat.format(testCase / 1000) + "000");
        }

        checkAppendIso8601Micros(-62167132799999999L, "0000-01-01T00:00:00.000001");
        checkAppendIso8601Micros(-62167132800000000L, "0000-01-01T00:00:00.000000");
        checkAppendIso8601Micros(-62167132800000001L, "0000-00-00T00:00:00.000000");

        checkAppendIso8601Micros(253402300799999000L, "9999-12-31T23:59:59.999000");
        checkAppendIso8601Micros(253402300800000000L, "9999-99-99T99:99:99.999999");

        checkAppendIso8601Micros(dateFormat.parse("2004-02-20T11:12:13.014").getTime() * 1000,
            "2004-02-20T11:12:13.014000");
        checkAppendIso8601Micros(dateFormat.parse("2000-02-20T11:12:13.014").getTime() * 1000,
            "2000-02-20T11:12:13.014000");
        checkAppendIso8601Micros(dateFormat.parse("1900-02-20T11:12:13.014").getTime() * 1000,
            "1900-02-20T11:12:13.014000");

        {
            ByteBuffer byteBuffer = ByteBuffer.allocate(100);
            assertSame(byteBuffer, Convert.appendISO8601Micros(0,
                new byte[] {'_', 's', 'u', 'f', 'f', 'i', 'x'}, byteBuffer));
            assertBufferEqual(byteBuffer, 0, "1970-01-01T00:00:00.000000_suffix");
        }
    }

    private void checkAppendIso8601Micros(long micros, String expectedString) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        assertSame(byteBuffer, Convert.appendISO8601Micros(micros, null, byteBuffer));
        assertBufferEqual(byteBuffer, 0, expectedString);
    }
}
