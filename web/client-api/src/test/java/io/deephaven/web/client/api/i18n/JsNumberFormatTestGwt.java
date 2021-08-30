package io.deephaven.web.client.api.i18n;

import com.google.gwt.junit.client.GWTTestCase;
import io.deephaven.web.client.api.LongWrapper;

/**
 * Since the JsNumberFormat test class is so thin, this mostly serves to confirm that a few
 * behaviors are the same in the JS client as they are in the swing console.
 */
public class JsNumberFormatTestGwt extends GWTTestCase {

    public void testFormat() {
        // broken in gwt 2.8.2
        // assertRoundTrip("0.00%", "100.00%", 1.0);
        // assertRoundTrip("0.00%", "0.00%", 0.00);
        // assertRoundTrip("0.00%", "55.00%", 0.55);
        // assertRoundTrip("0.00%", "55.00%", 0.0123);
        JsNumberFormat percent = JsNumberFormat.getFormat("0.00%");
        assertEquals("100.00%", percent.format(1.0));
        assertEquals("55.00%", percent.format(0.55));
        assertEquals("0.00%", percent.format(0.0));
        assertEquals("1.23%", percent.format(0.0123));

        assertRoundTrip("$###,##0.00", "$1.00", 1.0);

        assertRoundTrip("###,##0.00", "1.00", 1.0);
        assertRoundTrip("###,##0.00", "0.00", 0.0);
        assertRoundTrip("###,##0.00", "0.55", 0.55);
        assertRoundTrip("###,##0.00", "1,234.56", 1234.56);

        assertRoundTrip("###,###.00", "1.00", 1.0);
        assertRoundTrip("###,###.00", ".00", 0.0);
        assertRoundTrip("###,###.00", ".55", 0.55);
        assertRoundTrip("###,###.00", "1,234.56", 1234.56);

        // leave leading zero in parsed text
        JsNumberFormat format = JsNumberFormat.getFormat("###,###.00");
        assertEquals(0.55, format.parse("0.55"), 0.000001);
    }

    private void assertRoundTrip(String pattern, String input, double value) {
        JsNumberFormat format = JsNumberFormat.getFormat(pattern);
        double parsed = format.parse(input);
        assertEquals(parsed, value, 0.000001);
        assertEquals(input, format.format(parsed));
    }

    public void testLongFormat() {
        String defaultIntFormat = "###,##0";
        JsNumberFormat format = JsNumberFormat.getFormat(defaultIntFormat);

        assertEquals("0", format.format(LongWrapper.of(0)));
        assertEquals("-1", format.format(LongWrapper.of(-1)));
        assertEquals("1", format.format(LongWrapper.of(1)));

        assertEquals("9,999,000,000,000,000",
            format.format(LongWrapper.of(9_999_000_000_000_000L)));
        assertEquals("9,999,000,000,000,001",
            format.format(LongWrapper.of(9_999_000_000_000_001L)));
        assertEquals("9,999,000,000,000,002",
            format.format(LongWrapper.of(9_999_000_000_000_002L)));
        assertEquals("9,999,000,000,000,003",
            format.format(LongWrapper.of(9_999_000_000_000_003L)));

        assertEquals("-9,999,000,000,000,000",
            format.format(LongWrapper.of(-9_999_000_000_000_000L)));
        assertEquals("-9,999,000,000,000,001",
            format.format(LongWrapper.of(-9_999_000_000_000_001L)));
        assertEquals("-9,999,000,000,000,002",
            format.format(LongWrapper.of(-9_999_000_000_000_002L)));
        assertEquals("-9,999,000,000,000,003",
            format.format(LongWrapper.of(-9_999_000_000_000_003L)));
    }


    @Override
    public String getModuleName() {
        return "io.deephaven.web.DhApiDev";
    }
}
