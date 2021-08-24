package io.deephaven.libs.primitives;

import io.deephaven.base.verify.Require;
import junit.framework.TestCase;

import static io.deephaven.libs.primitives.PrimitiveParseUtil.*;
import static io.deephaven.util.QueryConstants.*;

public class TestPrimitiveParseUtil extends TestCase {

    public void testParseByte() {
        Require.eq(parseByte(null), "parseByte(null)", NULL_BYTE);
        Require.eq(parseByte("20"), "parseByte(\"20\")", 20);

        try {
            parseByte("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(parseByte("20", NULL_INT), "parseByte(\"20\", NULL_INT)", NULL_BYTE);
        Require.eq(parseByte("20", 10), "parseByte(\"20\", 10)", (short) 20);
    }

    public void testParseShort() {
        Require.eq(parseShort(null), "parseShort(null)", NULL_SHORT);
        Require.eq(parseShort("20"), "parseShort(\"20\")", 20);

        try {
            parseShort("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(parseShort("20", NULL_INT), "parseShort(\"20\", NULL_INT)", NULL_SHORT);
        Require.eq(parseShort("20", 10), "parseShort(\"20\", 10)", (short) 20);
    }

    public void testParseInt() {
        Require.eq(parseInt(null), "parseInt(null)", NULL_INT);
        Require.eq(parseInt("1000000"), "parseInt(\"1000000\")", 1_000_000);

        try {
            parseInt("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(parseInt("1000000", NULL_INT), "parseInt(\"1_000_000\", NULL_INT)", NULL_INT);
        Require.eq(parseInt("1000000", 10), "parseInt(\"1_000_000\", 10)", 1_000_000);
    }

    public void testParseUnsignedInt() {
        Require.eq(parseUnsignedInt(null), "parseUnsignedInt(null)", NULL_INT);
        Require.eq(parseUnsignedInt("1000000"), "parseUnsignedInt(\"1000000\")", 1_000_000);

        try {
            parseUnsignedInt("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        try {
            parseUnsignedInt("-1000000");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(parseUnsignedInt("1000000", NULL_INT),
            "parseUnsignedInt(\"1_000_000\", NULL_INT)", NULL_INT);
        Require.eq(parseUnsignedInt("1000000", 10), "parseUnsignedInt(\"1_000_000\", 10)",
            1_000_000);
    }

    public void testParseLong() {
        Require.eq(parseLong(null), "parseLong(null)", NULL_LONG);
        Require.eq(parseLong("4000000000"), "parseLong(\"4000000000\")", 4_000_000_000L);

        try {
            parseLong("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(parseLong("20", NULL_INT), "parseLong(\"20\", NULL_INT)", NULL_LONG);
        Require.eq(parseLong("20", 10), "parseLong(\"20\", 10)", 20);
    }

    public void testParseUnsignedLong() {
        Require.eq(parseUnsignedLong(null), "parseUnsignedLong(null)", NULL_LONG);
        Require.eq(parseUnsignedLong("4000000000"), "parseUnsignedLong(\"4000000000\")",
            4_000_000_000L);

        try {
            parseUnsignedLong("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        try {
            parseUnsignedLong("-4000000000");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(parseUnsignedLong("4000000000", NULL_INT),
            "parseUnsignedLong(\"4_000_000_000\", NULL_INT)", NULL_LONG);
        Require.eq(parseUnsignedLong("4000000000", 10), "parseUnsignedLong(\"4_000_000_000\", 10)",
            4_000_000_000L);
    }

    public void testParseDouble() {
        Require.eq(parseDouble(null), "parseDouble(null)", NULL_DOUBLE);
        Require.eq(parseDouble("0.000000001"), "parseDouble(\"0.000000001\")", 0.000000001d);

        try {
            parseDouble("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }
    }

    public void testParseFloat() {
        Require.eq(parseFloat(null), "parseFloat(null)", NULL_FLOAT);
        Require.eq(parseFloat("0.000000001"), "parseFloat(\"0.000000001\")", 0.000000001f);

        try {
            parseFloat("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }
    }

    public void testParseBoolean() {
        Require.eq(parseBoolean(null), "parseBoolean(null)", NULL_BOOLEAN);
        Require.eq(parseBoolean("true"), "parseBoolean(\"true\")", Boolean.TRUE);
        Require.eq(parseBoolean(""), "parseBoolean(\"\")", Boolean.FALSE); // Unlike numbers, no
                                                                           // Exception for this one
    }

}
