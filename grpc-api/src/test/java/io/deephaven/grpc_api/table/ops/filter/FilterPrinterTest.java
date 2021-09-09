package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.Literal;
import org.junit.Test;

import static org.junit.Assert.*;

public class FilterPrinterTest {

    @Test
    public void printNoEscape() {
        // simple double -> string and back again
        assertSameValue(1.5);

        // make sure that if a "double" is passed instead of a more correct integer type, we still print something that
        // matches
        assertSameValue(2.0);

        // other cases, make sure we match plain java expectations
        for (double val : new double[] {
                0,
                0.1, 0.1 + 0.1,
                Math.E, Math.PI,
                Double.MAX_VALUE, Double.MIN_VALUE, Double.MIN_NORMAL,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN}) {
            // ensure the rendered value turns back into a double and equals _exactly_ the original value
            assertSameValue(val);
        }

        // simple integer cases
        for (long val : new long[] {
                -150, -12, -1, 0, 1, 15, 500,
                Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            assertSameValue(val);
        }

        // note that these functions take bits, not a numeric value
        // try a single 1 in each bit, to random-walk a bit
        rotateAndAssert(1);
        // same with all 1s except one
        rotateAndAssert(Long.MAX_VALUE);

        // walk some random values (logging our start position)
        double r = Math.random();
        long longBits = Double.doubleToLongBits(r);
        System.out.println("Random long bits for FilterPrinterTest: " + longBits);
        rotateAndAssert(longBits);
    }

    private static void assertSameValue(double expected) {
        Literal literal = lit(expected);
        String str = removeQuotes(FilterPrinter.printNoEscape(literal));

        // test magic values that make sense in java, but Double.parseDouble won't accept directly
        if (Double.isNaN(expected)) {
            assertEquals("Double.NaN", str);
        } else if (Double.POSITIVE_INFINITY == expected) {
            assertEquals("Double.POSITIVE_INFINITY", str);
        } else if (Double.NEGATIVE_INFINITY == expected) {
            assertEquals("Double.NEGATIVE_INFINITY", str);
        } else {
            assertEquals(expected, Double.parseDouble(str), 0);
            if (str.contains(".")) {
                assertEquals(Double.toString(expected), str);
            } else {
                assertEquals("Provided value should have no decimal component " + expected, 0,
                        expected - (long) expected, 0);
                assertEquals(Long.toString((long) expected), str);
            }
        }
    }

    private static void assertSameValue(long expected) {
        assertTrue("Must be in the range that a double value can represent", Math.abs(expected) < (1L << 53));
        Literal literal = lit(expected);
        String str = removeQuotes(FilterPrinter.printNoEscape(literal));

        assertEquals(Long.toString(expected), str);
    }

    private static String removeQuotes(String quotedStr) {
        assertTrue(quotedStr.length() >= 2);
        assertEquals(quotedStr.charAt(0), '"');
        assertEquals(quotedStr.charAt(quotedStr.length() - 1), '"');
        return quotedStr.substring(1, quotedStr.length() - 1);
    }

    private static void rotateAndAssert(long longBits) {
        for (int i = 0; i < 64; i++) {
            // test as a double
            double doubleValue = Double.longBitsToDouble(longBits);
            assertSameValue(doubleValue);

            // test as a long, with higher bits cleared out
            assertSameValue(longBits & 0x0000ffffffffffffL);
            assertSameValue(-(longBits & 0x0000ffffffffffffL));

            // rotate for the next test
            longBits = (longBits >>> 1) | (longBits << (Long.SIZE - 1));
        }
    }

    private static Literal lit(double doubleValue) {
        return Literal.newBuilder().setDoubleValue(doubleValue).build();
    }
}
