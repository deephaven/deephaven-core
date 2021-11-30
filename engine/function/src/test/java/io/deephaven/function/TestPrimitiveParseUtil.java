package io.deephaven.function;

import io.deephaven.base.verify.Require;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.*;

public class TestPrimitiveParseUtil extends TestCase {

    public void testParseByte() {
        Require.eq(PrimitiveParseUtil.parseByte(null), "parseByte(null)", NULL_BYTE);
        Require.eq(PrimitiveParseUtil.parseByte("20"), "parseByte(\"20\")", 20);

        try {
            PrimitiveParseUtil.parseByte("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(PrimitiveParseUtil.parseByte("20", NULL_INT), "parseByte(\"20\", NULL_INT)", NULL_BYTE);
        Require.eq(PrimitiveParseUtil.parseByte("20", 10), "parseByte(\"20\", 10)", (short) 20);
    }

    public void testParseShort() {
        Require.eq(PrimitiveParseUtil.parseShort(null), "parseShort(null)", NULL_SHORT);
        Require.eq(PrimitiveParseUtil.parseShort("20"), "parseShort(\"20\")", 20);

        try {
            PrimitiveParseUtil.parseShort("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(PrimitiveParseUtil.parseShort("20", NULL_INT), "parseShort(\"20\", NULL_INT)", NULL_SHORT);
        Require.eq(PrimitiveParseUtil.parseShort("20", 10), "parseShort(\"20\", 10)", (short) 20);
    }

    public void testParseInt() {
        Require.eq(PrimitiveParseUtil.parseInt(null), "parseInt(null)", NULL_INT);
        Require.eq(PrimitiveParseUtil.parseInt("1000000"), "parseInt(\"1000000\")", 1_000_000);

        try {
            PrimitiveParseUtil.parseInt("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(PrimitiveParseUtil.parseInt("1000000", NULL_INT), "parseInt(\"1_000_000\", NULL_INT)", NULL_INT);
        Require.eq(PrimitiveParseUtil.parseInt("1000000", 10), "parseInt(\"1_000_000\", 10)", 1_000_000);
    }

    public void testParseUnsignedInt() {
        Require.eq(PrimitiveParseUtil.parseUnsignedInt(null), "parseUnsignedInt(null)", NULL_INT);
        Require.eq(PrimitiveParseUtil.parseUnsignedInt("1000000"), "parseUnsignedInt(\"1000000\")", 1_000_000);

        try {
            PrimitiveParseUtil.parseUnsignedInt("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        try {
            PrimitiveParseUtil.parseUnsignedInt("-1000000");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(PrimitiveParseUtil.parseUnsignedInt("1000000", NULL_INT),
                "parseUnsignedInt(\"1_000_000\", NULL_INT)", NULL_INT);
        Require.eq(PrimitiveParseUtil.parseUnsignedInt("1000000", 10), "parseUnsignedInt(\"1_000_000\", 10)",
                1_000_000);
    }

    public void testParseLong() {
        Require.eq(PrimitiveParseUtil.parseLong(null), "parseLong(null)", NULL_LONG);
        Require.eq(PrimitiveParseUtil.parseLong("4000000000"), "parseLong(\"4000000000\")", 4_000_000_000L);

        try {
            PrimitiveParseUtil.parseLong("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(PrimitiveParseUtil.parseLong("20", NULL_INT), "parseLong(\"20\", NULL_INT)", NULL_LONG);
        Require.eq(PrimitiveParseUtil.parseLong("20", 10), "parseLong(\"20\", 10)", 20);
    }

    public void testParseUnsignedLong() {
        Require.eq(PrimitiveParseUtil.parseUnsignedLong(null), "parseUnsignedLong(null)", NULL_LONG);
        Require.eq(PrimitiveParseUtil.parseUnsignedLong("4000000000"), "parseUnsignedLong(\"4000000000\")",
                4_000_000_000L);

        try {
            PrimitiveParseUtil.parseUnsignedLong("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        try {
            PrimitiveParseUtil.parseUnsignedLong("-4000000000");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }

        // Test with radix
        Require.eq(PrimitiveParseUtil.parseUnsignedLong("4000000000", NULL_INT),
                "parseUnsignedLong(\"4_000_000_000\", NULL_INT)",
                NULL_LONG);
        Require.eq(PrimitiveParseUtil.parseUnsignedLong("4000000000", 10), "parseUnsignedLong(\"4_000_000_000\", 10)",
                4_000_000_000L);
    }

    public void testParseDouble() {
        Require.eq(PrimitiveParseUtil.parseDouble(null), "parseDouble(null)", NULL_DOUBLE);
        Require.eq(PrimitiveParseUtil.parseDouble("0.000000001"), "parseDouble(\"0.000000001\")", 0.000000001d);

        try {
            PrimitiveParseUtil.parseDouble("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }
    }

    public void testParseFloat() {
        Require.eq(PrimitiveParseUtil.parseFloat(null), "parseFloat(null)", NULL_FLOAT);
        Require.eq(PrimitiveParseUtil.parseFloat("0.000000001"), "parseFloat(\"0.000000001\")", 0.000000001f);

        try {
            PrimitiveParseUtil.parseFloat("");
            fail("Should have thrown exception");
        } catch (NumberFormatException ignored) {
        }
    }

    public void testParseBoolean() {
        Require.eq(PrimitiveParseUtil.parseBoolean(null), "parseBoolean(null)", NULL_BOOLEAN);
        Require.eq(PrimitiveParseUtil.parseBoolean("true"), "parseBoolean(\"true\")", Boolean.TRUE);
        Require.eq(PrimitiveParseUtil.parseBoolean(""), "parseBoolean(\"\")", Boolean.FALSE); // Unlike numbers, no
                                                                                              // Exception for this one
    }

}
