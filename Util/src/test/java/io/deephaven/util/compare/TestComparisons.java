//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestComparisons {

    @Test
    public void testBoolBoolComparisons() {
        // pairwise full
        {
            final boolean[] sorted = new boolean[] {
                    false,
                    true
            };
            for (int i = 0; i < sorted.length; ++i) {
                eq(sorted[i], sorted[i]);
                for (int j = i + 1; j < sorted.length; ++j) {
                    lt(sorted[i], sorted[j]);
                }
            }
        }
    }

    @Test
    public void testCharCharComparisons() {
        // pairwise subset
        {
            final char[] sorted = new char[] {
                    QueryConstants.NULL_CHAR,
                    QueryConstants.MIN_CHAR,
                    'A',
                    'B',
                    QueryConstants.MAX_CHAR
            };
            for (int i = 0; i < sorted.length; ++i) {
                eq(sorted[i], sorted[i]);
                for (int j = i + 1; j < sorted.length; ++j) {
                    lt(sorted[i], sorted[j]);
                }
            }
        }
        // transitive full
        {
            char prev = QueryConstants.MIN_CHAR;
            char next;
            while ((next = (char) (prev + 1)) != QueryConstants.NULL_CHAR) {
                lt(QueryConstants.NULL_CHAR, next);
                lt(prev, next);
                prev = next;
            }
        }
    }

    @Test
    public void testByteByteComparisons() {
        // pairwise subset
        {
            final byte[] sorted = new byte[] {
                    QueryConstants.NULL_BYTE,
                    QueryConstants.MIN_BYTE,
                    (byte) -1,
                    (byte) 0,
                    (byte) 1,
                    (byte) 42,
                    QueryConstants.MAX_BYTE
            };
            for (int i = 0; i < sorted.length; ++i) {
                eq(sorted[i], sorted[i]);
                for (int j = i + 1; j < sorted.length; ++j) {
                    lt(sorted[i], sorted[j]);
                }
            }
        }
        // transitive full
        {
            byte prev = QueryConstants.NULL_BYTE;
            byte next;
            while ((next = (byte) (prev + 1)) != QueryConstants.MAX_BYTE) {
                lt(QueryConstants.NULL_BYTE, next);
                lt(prev, next);
                prev = next;
            }
            lt(QueryConstants.NULL_BYTE, next);
            lt(prev, next);
        }
    }

    @Test
    public void testShortShortComparisons() {
        // pairwise subset
        {
            final short[] sorted = new short[] {
                    QueryConstants.NULL_SHORT,
                    QueryConstants.MIN_SHORT,
                    (short) -1,
                    (short) 0,
                    (short) 1,
                    (short) 42,
                    QueryConstants.MAX_SHORT
            };
            for (int i = 0; i < sorted.length; ++i) {
                eq(sorted[i], sorted[i]);
                for (int j = i + 1; j < sorted.length; ++j) {
                    lt(sorted[i], sorted[j]);
                }
            }
        }
        // transitive full
        {
            short prev = QueryConstants.NULL_SHORT;
            short next;
            while ((next = (short) (prev + 1)) != QueryConstants.MAX_SHORT) {
                lt(QueryConstants.NULL_SHORT, next);
                lt(prev, next);
                prev = next;
            }
            lt(QueryConstants.NULL_SHORT, next);
            lt(prev, next);
        }
    }

    @Test
    public void testIntIntComparisons() {
        // pairwise subset
        {
            final int[] sorted = new int[] {
                    QueryConstants.NULL_INT,
                    QueryConstants.MIN_INT,
                    -1,
                    0,
                    1,
                    42,
                    QueryConstants.MAX_INT
            };
            for (int i = 0; i < sorted.length; ++i) {
                eq(sorted[i], sorted[i]);
                for (int j = i + 1; j < sorted.length; ++j) {
                    lt(sorted[i], sorted[j]);
                }
            }
        }
        // transitive full; not too expensive, but probably not worth cost of running every time
        // {
        // int prev = QueryConstants.NULL_INT;
        // int next;
        // while ((next = prev + 1) != QueryConstants.MAX_INT) {
        // lt(QueryConstants.NULL_INT, next);
        // lt(prev, next);
        // prev = next;
        // }
        // lt(QueryConstants.NULL_INT, next);
        // lt(prev, next);
        // }
    }

    @Test
    public void testLongLongComparisons() {
        // pairwise subset
        {
            final long[] sorted = new long[] {
                    QueryConstants.NULL_LONG,
                    QueryConstants.MIN_LONG,
                    -1L,
                    0L,
                    1L,
                    42L,
                    QueryConstants.MAX_LONG
            };
            for (int i = 0; i < sorted.length; ++i) {
                eq(sorted[i], sorted[i]);
                for (int j = i + 1; j < sorted.length; ++j) {
                    lt(sorted[i], sorted[j]);
                }
            }
        }
        // transitive full too expensive
    }

    @Test
    public void testFloatFloatComparisons() {
        // pairwise subset
        {
            final float[] sorted = new float[] {
                    QueryConstants.NULL_FLOAT,
                    Float.NEGATIVE_INFINITY,
                    QueryConstants.MIN_FINITE_FLOAT,
                    -1.0f,
                    Math.nextDown(0.0f),
                    0.0f,
                    Math.nextUp(0.0f),
                    1.0f,
                    42.0f,
                    QueryConstants.MAX_FINITE_FLOAT,
                    Float.POSITIVE_INFINITY,
                    Float.NaN
            };
            for (int i = 0; i < sorted.length; ++i) {
                eq(sorted[i], sorted[i]);
                for (int j = i + 1; j < sorted.length; ++j) {
                    lt(sorted[i], sorted[j]);
                }
            }
        }
        // special cases
        {
            eq(-0.0f, 0.0f);
            {
                // These are the edges of float NaN representations, as documented by intBitsToFloat
                for (int nanEdge : new int[] {
                        0x7f800001,
                        0x7fffffff,
                        0xff800001,
                        0xffffffff
                }) {
                    final float altNaN = Float.intBitsToFloat(nanEdge);
                    eq(Float.NaN, altNaN);
                    lt(QueryConstants.NULL_FLOAT, altNaN);
                    lt(QueryConstants.MAX_FLOAT, altNaN);
                }
            }
        }
        // transitive full; expensive, but possible
        // {
        // double prev = Math.nextUp(QueryConstants.NULL_FLOAT);
        // do {
        // double next = Math.nextUp(prev);
        // lt(QueryConstants.NULL_FLOAT, next);
        // lt(prev, next);
        // prev = next;
        // } while (prev != Float.POSITIVE_INFINITY);
        // }
    }

    @Test
    public void testDoubleDoubleComparisons() {
        // pairwise subset
        {
            final double[] sorted = new double[] {
                    QueryConstants.NULL_DOUBLE,
                    Double.NEGATIVE_INFINITY,
                    QueryConstants.MIN_FINITE_DOUBLE,
                    -1.0,
                    Math.nextDown(0.0),
                    0.0,
                    Math.nextUp(0.0),
                    1.0,
                    42.0,
                    QueryConstants.MAX_FINITE_DOUBLE,
                    Double.POSITIVE_INFINITY,
                    Double.NaN
            };
            for (int i = 0; i < sorted.length; ++i) {
                eq(sorted[i], sorted[i]);
                for (int j = i + 1; j < sorted.length; ++j) {
                    lt(sorted[i], sorted[j]);
                }
            }
        }
        // special cases
        {
            eq(-0.0, 0.0);
            {
                // These are the edges of double NaN representations, as documented by longBitsToDouble
                for (long nanEdge : new long[] {
                        0x7ff0000000000001L,
                        0x7fffffffffffffffL,
                        0xfff0000000000001L,
                        0xffffffffffffffffL
                }) {
                    final double altNaN = Double.longBitsToDouble(nanEdge);
                    eq(Double.NaN, altNaN);
                    lt(QueryConstants.NULL_DOUBLE, altNaN);
                    lt(QueryConstants.MAX_DOUBLE, altNaN);
                }
            }
        }
        // transitive full too expensive
    }

    private static void eq(boolean x, boolean y) {
        // x == y
        assertEquals(0, BooleanComparisons.compare(x, y));
        assertTrue(BooleanComparisons.eq(x, y));
        assertFalse(BooleanComparisons.lt(x, y));
        assertTrue(BooleanComparisons.leq(x, y));
        assertFalse(BooleanComparisons.gt(x, y));
        assertTrue(BooleanComparisons.geq(x, y));

        // y == x
        assertEquals(0, BooleanComparisons.compare(y, x));
        assertTrue(BooleanComparisons.eq(y, x));
        assertFalse(BooleanComparisons.lt(y, x));
        assertTrue(BooleanComparisons.leq(y, x));
        assertFalse(BooleanComparisons.gt(y, x));
        assertTrue(BooleanComparisons.geq(y, x));

        assertEquals(BooleanComparisons.hashCode(x), BooleanComparisons.hashCode(y));
    }

    private static void lt(boolean x, boolean y) {
        // x < y
        assertTrue(BooleanComparisons.compare(x, y) < 0);
        assertFalse(BooleanComparisons.eq(x, y));
        assertTrue(BooleanComparisons.lt(x, y));
        assertTrue(BooleanComparisons.leq(x, y));
        assertFalse(BooleanComparisons.gt(x, y));
        assertFalse(BooleanComparisons.geq(x, y));

        // y > x
        assertTrue(BooleanComparisons.compare(y, x) > 0);
        assertFalse(BooleanComparisons.eq(y, x));
        assertFalse(BooleanComparisons.lt(y, x));
        assertFalse(BooleanComparisons.leq(y, x));
        assertTrue(BooleanComparisons.gt(y, x));
        assertTrue(BooleanComparisons.geq(y, x));
    }


    private static void eq(char x, char y) {
        // x == y
        assertEquals(0, CharComparisons.compare(x, y));
        assertTrue(CharComparisons.eq(x, y));
        assertFalse(CharComparisons.lt(x, y));
        assertTrue(CharComparisons.leq(x, y));
        assertFalse(CharComparisons.gt(x, y));
        assertTrue(CharComparisons.geq(x, y));

        // y == x
        assertEquals(0, CharComparisons.compare(y, x));
        assertTrue(CharComparisons.eq(y, x));
        assertFalse(CharComparisons.lt(y, x));
        assertTrue(CharComparisons.leq(y, x));
        assertFalse(CharComparisons.gt(y, x));
        assertTrue(CharComparisons.geq(y, x));

        assertEquals(CharComparisons.hashCode(x), CharComparisons.hashCode(y));
    }

    private static void lt(char x, char y) {
        // x < y
        assertTrue(CharComparisons.compare(x, y) < 0);
        assertFalse(CharComparisons.eq(x, y));
        assertTrue(CharComparisons.lt(x, y));
        assertTrue(CharComparisons.leq(x, y));
        assertFalse(CharComparisons.gt(x, y));
        assertFalse(CharComparisons.geq(x, y));

        // y > x
        assertTrue(CharComparisons.compare(y, x) > 0);
        assertFalse(CharComparisons.eq(y, x));
        assertFalse(CharComparisons.lt(y, x));
        assertFalse(CharComparisons.leq(y, x));
        assertTrue(CharComparisons.gt(y, x));
        assertTrue(CharComparisons.geq(y, x));
    }

    private static void eq(byte x, byte y) {
        // x == y
        assertEquals(0, ByteComparisons.compare(x, y));
        assertTrue(ByteComparisons.eq(x, y));
        assertFalse(ByteComparisons.lt(x, y));
        assertTrue(ByteComparisons.leq(x, y));
        assertFalse(ByteComparisons.gt(x, y));
        assertTrue(ByteComparisons.geq(x, y));

        // y == x
        assertEquals(0, ByteComparisons.compare(y, x));
        assertTrue(ByteComparisons.eq(y, x));
        assertFalse(ByteComparisons.lt(y, x));
        assertTrue(ByteComparisons.leq(y, x));
        assertFalse(ByteComparisons.gt(y, x));
        assertTrue(ByteComparisons.geq(y, x));

        assertEquals(ByteComparisons.hashCode(x), ByteComparisons.hashCode(y));
    }

    private static void lt(byte x, byte y) {
        // x < y
        assertTrue(ByteComparisons.compare(x, y) < 0);
        assertFalse(ByteComparisons.eq(x, y));
        assertTrue(ByteComparisons.lt(x, y));
        assertTrue(ByteComparisons.leq(x, y));
        assertFalse(ByteComparisons.gt(x, y));
        assertFalse(ByteComparisons.geq(x, y));

        // y > x
        assertTrue(ByteComparisons.compare(y, x) > 0);
        assertFalse(ByteComparisons.eq(y, x));
        assertFalse(ByteComparisons.lt(y, x));
        assertFalse(ByteComparisons.leq(y, x));
        assertTrue(ByteComparisons.gt(y, x));
        assertTrue(ByteComparisons.geq(y, x));
    }

    private static void eq(short x, short y) {
        // x == y
        assertEquals(0, ShortComparisons.compare(x, y));
        assertTrue(ShortComparisons.eq(x, y));
        assertFalse(ShortComparisons.lt(x, y));
        assertTrue(ShortComparisons.leq(x, y));
        assertFalse(ShortComparisons.gt(x, y));
        assertTrue(ShortComparisons.geq(x, y));

        // y == x
        assertEquals(0, ShortComparisons.compare(y, x));
        assertTrue(ShortComparisons.eq(y, x));
        assertFalse(ShortComparisons.lt(y, x));
        assertTrue(ShortComparisons.leq(y, x));
        assertFalse(ShortComparisons.gt(y, x));
        assertTrue(ShortComparisons.geq(y, x));

        assertEquals(ShortComparisons.hashCode(x), ShortComparisons.hashCode(y));
    }

    private static void lt(short x, short y) {
        // x < y
        assertTrue(ShortComparisons.compare(x, y) < 0);
        assertFalse(ShortComparisons.eq(x, y));
        assertTrue(ShortComparisons.lt(x, y));
        assertTrue(ShortComparisons.leq(x, y));
        assertFalse(ShortComparisons.gt(x, y));
        assertFalse(ShortComparisons.geq(x, y));

        // y > x
        assertTrue(ShortComparisons.compare(y, x) > 0);
        assertFalse(ShortComparisons.eq(y, x));
        assertFalse(ShortComparisons.lt(y, x));
        assertFalse(ShortComparisons.leq(y, x));
        assertTrue(ShortComparisons.gt(y, x));
        assertTrue(ShortComparisons.geq(y, x));
    }

    private static void eq(int x, int y) {
        // x == y
        assertEquals(0, IntComparisons.compare(x, y));
        assertTrue(IntComparisons.eq(x, y));
        assertFalse(IntComparisons.lt(x, y));
        assertTrue(IntComparisons.leq(x, y));
        assertFalse(IntComparisons.gt(x, y));
        assertTrue(IntComparisons.geq(x, y));

        // y == x
        assertEquals(0, IntComparisons.compare(y, x));
        assertTrue(IntComparisons.eq(y, x));
        assertFalse(IntComparisons.lt(y, x));
        assertTrue(IntComparisons.leq(y, x));
        assertFalse(IntComparisons.gt(y, x));
        assertTrue(IntComparisons.geq(y, x));

        assertEquals(IntComparisons.hashCode(x), IntComparisons.hashCode(y));
    }

    private static void lt(int x, int y) {
        // x < y
        assertTrue(IntComparisons.compare(x, y) < 0);
        assertFalse(IntComparisons.eq(x, y));
        assertTrue(IntComparisons.lt(x, y));
        assertTrue(IntComparisons.leq(x, y));
        assertFalse(IntComparisons.gt(x, y));
        assertFalse(IntComparisons.geq(x, y));

        // y > x
        assertTrue(IntComparisons.compare(y, x) > 0);
        assertFalse(IntComparisons.eq(y, x));
        assertFalse(IntComparisons.lt(y, x));
        assertFalse(IntComparisons.leq(y, x));
        assertTrue(IntComparisons.gt(y, x));
        assertTrue(IntComparisons.geq(y, x));
    }

    private static void eq(long x, long y) {
        // x == y
        assertEquals(0, LongComparisons.compare(x, y));
        assertTrue(LongComparisons.eq(x, y));
        assertFalse(LongComparisons.lt(x, y));
        assertTrue(LongComparisons.leq(x, y));
        assertFalse(LongComparisons.gt(x, y));
        assertTrue(LongComparisons.geq(x, y));

        // y == x
        assertEquals(0, LongComparisons.compare(y, x));
        assertTrue(LongComparisons.eq(y, x));
        assertFalse(LongComparisons.lt(y, x));
        assertTrue(LongComparisons.leq(y, x));
        assertFalse(LongComparisons.gt(y, x));
        assertTrue(LongComparisons.geq(y, x));

        assertEquals(LongComparisons.hashCode(x), LongComparisons.hashCode(y));
    }

    private static void lt(long x, long y) {
        // x < y
        assertTrue(LongComparisons.compare(x, y) < 0);
        assertFalse(LongComparisons.eq(x, y));
        assertTrue(LongComparisons.lt(x, y));
        assertTrue(LongComparisons.leq(x, y));
        assertFalse(LongComparisons.gt(x, y));
        assertFalse(LongComparisons.geq(x, y));

        // y > x
        assertTrue(LongComparisons.compare(y, x) > 0);
        assertFalse(LongComparisons.eq(y, x));
        assertFalse(LongComparisons.lt(y, x));
        assertFalse(LongComparisons.leq(y, x));
        assertTrue(LongComparisons.gt(y, x));
        assertTrue(LongComparisons.geq(y, x));
    }

    private static void eq(float x, float y) {
        // x == y
        assertEquals(0, FloatComparisons.compare(x, y));
        assertTrue(FloatComparisons.eq(x, y));
        assertFalse(FloatComparisons.lt(x, y));
        assertTrue(FloatComparisons.leq(x, y));
        assertFalse(FloatComparisons.gt(x, y));
        assertTrue(FloatComparisons.geq(x, y));

        // y == x
        assertEquals(0, FloatComparisons.compare(y, x));
        assertTrue(FloatComparisons.eq(y, x));
        assertFalse(FloatComparisons.lt(y, x));
        assertTrue(FloatComparisons.leq(y, x));
        assertFalse(FloatComparisons.gt(y, x));
        assertTrue(FloatComparisons.geq(y, x));

        assertEquals(FloatComparisons.hashCode(x), FloatComparisons.hashCode(y));
    }

    private static void lt(float x, float y) {
        // x < y
        assertTrue(FloatComparisons.compare(x, y) < 0);
        assertFalse(FloatComparisons.eq(x, y));
        assertTrue(FloatComparisons.lt(x, y));
        assertTrue(FloatComparisons.leq(x, y));
        assertFalse(FloatComparisons.gt(x, y));
        assertFalse(FloatComparisons.geq(x, y));

        // y > x
        assertTrue(FloatComparisons.compare(y, x) > 0);
        assertFalse(FloatComparisons.eq(y, x));
        assertFalse(FloatComparisons.lt(y, x));
        assertFalse(FloatComparisons.leq(y, x));
        assertTrue(FloatComparisons.gt(y, x));
        assertTrue(FloatComparisons.geq(y, x));
    }

    private static void eq(double x, double y) {
        // x == y
        assertEquals(0, DoubleComparisons.compare(x, y));
        assertTrue(DoubleComparisons.eq(x, y));
        assertFalse(DoubleComparisons.lt(x, y));
        assertTrue(DoubleComparisons.leq(x, y));
        assertFalse(DoubleComparisons.gt(x, y));
        assertTrue(DoubleComparisons.geq(x, y));

        // y == x
        assertEquals(0, DoubleComparisons.compare(y, x));
        assertTrue(DoubleComparisons.eq(y, x));
        assertFalse(DoubleComparisons.lt(y, x));
        assertTrue(DoubleComparisons.leq(y, x));
        assertFalse(DoubleComparisons.gt(y, x));
        assertTrue(DoubleComparisons.geq(y, x));

        assertEquals(DoubleComparisons.hashCode(x), DoubleComparisons.hashCode(y));
    }

    private static void lt(double x, double y) {
        // x < y
        assertTrue(DoubleComparisons.compare(x, y) < 0);
        assertFalse(DoubleComparisons.eq(x, y));
        assertTrue(DoubleComparisons.lt(x, y));
        assertTrue(DoubleComparisons.leq(x, y));
        assertFalse(DoubleComparisons.gt(x, y));
        assertFalse(DoubleComparisons.geq(x, y));

        // y > x
        assertTrue(DoubleComparisons.compare(y, x) > 0);
        assertFalse(DoubleComparisons.eq(y, x));
        assertFalse(DoubleComparisons.lt(y, x));
        assertFalse(DoubleComparisons.leq(y, x));
        assertTrue(DoubleComparisons.gt(y, x));
        assertTrue(DoubleComparisons.geq(y, x));
    }
}
