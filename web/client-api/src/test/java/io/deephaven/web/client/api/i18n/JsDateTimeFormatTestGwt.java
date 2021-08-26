package io.deephaven.web.client.api.i18n;

import com.google.gwt.i18n.shared.DateTimeFormat;
import com.google.gwt.junit.client.GWTTestCase;
import io.deephaven.web.client.api.LongWrapper;

import java.util.Date;

public class JsDateTimeFormatTestGwt extends GWTTestCase {

    public void testGetFormat() {
        // with subseconds at suffix, add more subseconds
        long nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss", "2018-04-26T12:34:56");
        assertEquals(0, nanos % 1_000_000_000);
        // S
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.S", "2018-04-26T12:34:56.1");
        assertEquals(100_000_000, nanos % 1_000_000_000);
        // SS
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SS", "2018-04-26T12:34:56.12");
        assertEquals(120_000_000, nanos % 1_000_000_000);
        // SSS
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSS", "2018-04-26T12:34:56.123");
        assertEquals(123_000_000, nanos % 1_000_000_000);
        // SSSSSS
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSS", "2018-04-26T12:34:56.123456");
        assertEquals(123_456_000, nanos % 1_000_000_000);
        // SSSSSSSSS
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS", "2018-04-26T12:34:56.123456789");
        assertEquals(123_456_789, nanos % 1_000_000_000);

        // same, with another suffix (like tz)
        // first get the timezone string for the browser's current settings
        String tz = DateTimeFormat.getFormat("zzzz").format(new Date(2018 - 1900, 4, 26, 12, 34));

        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss zzzz", "2018-04-26T12:34:56 " + tz);
        assertEquals(0, nanos % 1_000_000_000);
        // S
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.S zzzz", "2018-04-26T12:34:56.1 " + tz);
        assertEquals(100_000_000, nanos % 1_000_000_000);
        // SS
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SS zzzz", "2018-04-26T12:34:56.12 " + tz);
        assertEquals(120_000_000, nanos % 1_000_000_000);
        // SSS
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSS zzzz", "2018-04-26T12:34:56.123 " + tz);
        assertEquals(123_000_000, nanos % 1_000_000_000);
        // SSSSSS
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSS zzzz",
            "2018-04-26T12:34:56.123456 " + tz);
        assertEquals(123_456_000, nanos % 1_000_000_000);
        // SSSSSSSSS
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS zzzz",
            "2018-04-26T12:34:56.123456789 " + tz);
        assertEquals(123_456_789, nanos % 1_000_000_000);

        // test with leading zeros
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSS zzzz", "2018-04-26T12:34:56.000 " + tz);
        assertEquals(0, nanos % 1_000_000_000);

        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSS zzzz", "2018-04-26T12:34:56.001 " + tz);
        assertEquals(1_000_000, nanos % 1_000_000_000);

        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSS zzzz",
            "2018-04-26T12:34:56.001000 " + tz);
        assertEquals(1_000_000, nanos % 1_000_000_000);
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSS zzzz",
            "2018-04-26T12:34:56.001001 " + tz);
        assertEquals(1_001_000, nanos % 1_000_000_000);
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSS zzzz",
            "2018-04-26T12:34:56.100000 " + tz);
        assertEquals(100_000_000, nanos % 1_000_000_000);

        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS zzzz",
            "2018-04-26T12:34:56.000000000 " + tz);
        assertEquals(0, nanos % 1_000_000_000);
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS zzzz",
            "2018-04-26T12:34:56.000000001 " + tz);
        assertEquals(1, nanos % 1_000_000_000);
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS zzzz",
            "2018-04-26T12:34:56.000000010 " + tz);
        assertEquals(10, nanos % 1_000_000_000);
        nanos = assertRoundTrip("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS zzzz",
            "2018-04-26T12:34:56.000001234 " + tz);
        assertEquals(1234, nanos % 1_000_000_000);
    }

    /**
     * Helper which takes a string, parses the string, and then formats the long again. The input is
     * checked to match the output of the final format call, and the intermediate value is returned
     * for any subsequent checks.
     */
    private long assertRoundTrip(String formatString, String input) {
        JsDateTimeFormat format = JsDateTimeFormat.getFormat(formatString);
        LongWrapper parsed = format.parse(input, null);
        assertEquals(input, format.format(parsed, null));
        return parsed.getWrapped();
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DhApiDev";
    }
}
