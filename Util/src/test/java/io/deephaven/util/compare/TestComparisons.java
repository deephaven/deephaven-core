//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.Test;

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
        TestCase.assertEquals(0, BooleanComparisons.compare(x, y));
        TestCase.assertTrue(BooleanComparisons.eq(x, y));
        TestCase.assertFalse(BooleanComparisons.lt(x, y));
        TestCase.assertTrue(BooleanComparisons.leq(x, y));
        TestCase.assertFalse(BooleanComparisons.gt(x, y));
        TestCase.assertTrue(BooleanComparisons.geq(x, y));

        // y == x
        TestCase.assertEquals(0, BooleanComparisons.compare(y, x));
        TestCase.assertTrue(BooleanComparisons.eq(y, x));
        TestCase.assertFalse(BooleanComparisons.lt(y, x));
        TestCase.assertTrue(BooleanComparisons.leq(y, x));
        TestCase.assertFalse(BooleanComparisons.gt(y, x));
        TestCase.assertTrue(BooleanComparisons.geq(y, x));

        TestCase.assertEquals(BooleanComparisons.hashCode(x), BooleanComparisons.hashCode(y));
    }

    private static void lt(boolean x, boolean y) {
        // x < y
        TestCase.assertTrue(BooleanComparisons.compare(x, y) < 0);
        TestCase.assertFalse(BooleanComparisons.eq(x, y));
        TestCase.assertTrue(BooleanComparisons.lt(x, y));
        TestCase.assertTrue(BooleanComparisons.leq(x, y));
        TestCase.assertFalse(BooleanComparisons.gt(x, y));
        TestCase.assertFalse(BooleanComparisons.geq(x, y));

        // y > x
        TestCase.assertTrue(BooleanComparisons.compare(y, x) > 0);
        TestCase.assertFalse(BooleanComparisons.eq(y, x));
        TestCase.assertFalse(BooleanComparisons.lt(y, x));
        TestCase.assertFalse(BooleanComparisons.leq(y, x));
        TestCase.assertTrue(BooleanComparisons.gt(y, x));
        TestCase.assertTrue(BooleanComparisons.geq(y, x));
    }


    private static void eq(char x, char y) {
        // x == y
        TestCase.assertEquals(0, CharComparisons.compare(x, y));
        TestCase.assertTrue(CharComparisons.eq(x, y));
        TestCase.assertFalse(CharComparisons.lt(x, y));
        TestCase.assertTrue(CharComparisons.leq(x, y));
        TestCase.assertFalse(CharComparisons.gt(x, y));
        TestCase.assertTrue(CharComparisons.geq(x, y));

        // y == x
        TestCase.assertEquals(0, CharComparisons.compare(y, x));
        TestCase.assertTrue(CharComparisons.eq(y, x));
        TestCase.assertFalse(CharComparisons.lt(y, x));
        TestCase.assertTrue(CharComparisons.leq(y, x));
        TestCase.assertFalse(CharComparisons.gt(y, x));
        TestCase.assertTrue(CharComparisons.geq(y, x));

        TestCase.assertEquals(CharComparisons.hashCode(x), CharComparisons.hashCode(y));
    }

    private static void lt(char x, char y) {
        // x < y
        TestCase.assertTrue(CharComparisons.compare(x, y) < 0);
        TestCase.assertFalse(CharComparisons.eq(x, y));
        TestCase.assertTrue(CharComparisons.lt(x, y));
        TestCase.assertTrue(CharComparisons.leq(x, y));
        TestCase.assertFalse(CharComparisons.gt(x, y));
        TestCase.assertFalse(CharComparisons.geq(x, y));

        // y > x
        TestCase.assertTrue(CharComparisons.compare(y, x) > 0);
        TestCase.assertFalse(CharComparisons.eq(y, x));
        TestCase.assertFalse(CharComparisons.lt(y, x));
        TestCase.assertFalse(CharComparisons.leq(y, x));
        TestCase.assertTrue(CharComparisons.gt(y, x));
        TestCase.assertTrue(CharComparisons.geq(y, x));
    }

    private static void eq(byte x, byte y) {
        // x == y
        TestCase.assertEquals(0, ByteComparisons.compare(x, y));
        TestCase.assertTrue(ByteComparisons.eq(x, y));
        TestCase.assertFalse(ByteComparisons.lt(x, y));
        TestCase.assertTrue(ByteComparisons.leq(x, y));
        TestCase.assertFalse(ByteComparisons.gt(x, y));
        TestCase.assertTrue(ByteComparisons.geq(x, y));

        // y == x
        TestCase.assertEquals(0, ByteComparisons.compare(y, x));
        TestCase.assertTrue(ByteComparisons.eq(y, x));
        TestCase.assertFalse(ByteComparisons.lt(y, x));
        TestCase.assertTrue(ByteComparisons.leq(y, x));
        TestCase.assertFalse(ByteComparisons.gt(y, x));
        TestCase.assertTrue(ByteComparisons.geq(y, x));

        TestCase.assertEquals(ByteComparisons.hashCode(x), ByteComparisons.hashCode(y));
    }

    private static void lt(byte x, byte y) {
        // x < y
        TestCase.assertTrue(ByteComparisons.compare(x, y) < 0);
        TestCase.assertFalse(ByteComparisons.eq(x, y));
        TestCase.assertTrue(ByteComparisons.lt(x, y));
        TestCase.assertTrue(ByteComparisons.leq(x, y));
        TestCase.assertFalse(ByteComparisons.gt(x, y));
        TestCase.assertFalse(ByteComparisons.geq(x, y));

        // y > x
        TestCase.assertTrue(ByteComparisons.compare(y, x) > 0);
        TestCase.assertFalse(ByteComparisons.eq(y, x));
        TestCase.assertFalse(ByteComparisons.lt(y, x));
        TestCase.assertFalse(ByteComparisons.leq(y, x));
        TestCase.assertTrue(ByteComparisons.gt(y, x));
        TestCase.assertTrue(ByteComparisons.geq(y, x));
    }

    private static void eq(short x, short y) {
        // x == y
        TestCase.assertEquals(0, ShortComparisons.compare(x, y));
        TestCase.assertTrue(ShortComparisons.eq(x, y));
        TestCase.assertFalse(ShortComparisons.lt(x, y));
        TestCase.assertTrue(ShortComparisons.leq(x, y));
        TestCase.assertFalse(ShortComparisons.gt(x, y));
        TestCase.assertTrue(ShortComparisons.geq(x, y));

        // y == x
        TestCase.assertEquals(0, ShortComparisons.compare(y, x));
        TestCase.assertTrue(ShortComparisons.eq(y, x));
        TestCase.assertFalse(ShortComparisons.lt(y, x));
        TestCase.assertTrue(ShortComparisons.leq(y, x));
        TestCase.assertFalse(ShortComparisons.gt(y, x));
        TestCase.assertTrue(ShortComparisons.geq(y, x));

        TestCase.assertEquals(ShortComparisons.hashCode(x), ShortComparisons.hashCode(y));
    }

    private static void lt(short x, short y) {
        // x < y
        TestCase.assertTrue(ShortComparisons.compare(x, y) < 0);
        TestCase.assertFalse(ShortComparisons.eq(x, y));
        TestCase.assertTrue(ShortComparisons.lt(x, y));
        TestCase.assertTrue(ShortComparisons.leq(x, y));
        TestCase.assertFalse(ShortComparisons.gt(x, y));
        TestCase.assertFalse(ShortComparisons.geq(x, y));

        // y > x
        TestCase.assertTrue(ShortComparisons.compare(y, x) > 0);
        TestCase.assertFalse(ShortComparisons.eq(y, x));
        TestCase.assertFalse(ShortComparisons.lt(y, x));
        TestCase.assertFalse(ShortComparisons.leq(y, x));
        TestCase.assertTrue(ShortComparisons.gt(y, x));
        TestCase.assertTrue(ShortComparisons.geq(y, x));
    }

    private static void eq(int x, int y) {
        // x == y
        TestCase.assertEquals(0, IntComparisons.compare(x, y));
        TestCase.assertTrue(IntComparisons.eq(x, y));
        TestCase.assertFalse(IntComparisons.lt(x, y));
        TestCase.assertTrue(IntComparisons.leq(x, y));
        TestCase.assertFalse(IntComparisons.gt(x, y));
        TestCase.assertTrue(IntComparisons.geq(x, y));

        // y == x
        TestCase.assertEquals(0, IntComparisons.compare(y, x));
        TestCase.assertTrue(IntComparisons.eq(y, x));
        TestCase.assertFalse(IntComparisons.lt(y, x));
        TestCase.assertTrue(IntComparisons.leq(y, x));
        TestCase.assertFalse(IntComparisons.gt(y, x));
        TestCase.assertTrue(IntComparisons.geq(y, x));

        TestCase.assertEquals(IntComparisons.hashCode(x), IntComparisons.hashCode(y));
    }

    private static void lt(int x, int y) {
        // x < y
        TestCase.assertTrue(IntComparisons.compare(x, y) < 0);
        TestCase.assertFalse(IntComparisons.eq(x, y));
        TestCase.assertTrue(IntComparisons.lt(x, y));
        TestCase.assertTrue(IntComparisons.leq(x, y));
        TestCase.assertFalse(IntComparisons.gt(x, y));
        TestCase.assertFalse(IntComparisons.geq(x, y));

        // y > x
        TestCase.assertTrue(IntComparisons.compare(y, x) > 0);
        TestCase.assertFalse(IntComparisons.eq(y, x));
        TestCase.assertFalse(IntComparisons.lt(y, x));
        TestCase.assertFalse(IntComparisons.leq(y, x));
        TestCase.assertTrue(IntComparisons.gt(y, x));
        TestCase.assertTrue(IntComparisons.geq(y, x));
    }

    private static void eq(long x, long y) {
        // x == y
        TestCase.assertEquals(0, LongComparisons.compare(x, y));
        TestCase.assertTrue(LongComparisons.eq(x, y));
        TestCase.assertFalse(LongComparisons.lt(x, y));
        TestCase.assertTrue(LongComparisons.leq(x, y));
        TestCase.assertFalse(LongComparisons.gt(x, y));
        TestCase.assertTrue(LongComparisons.geq(x, y));

        // y == x
        TestCase.assertEquals(0, LongComparisons.compare(y, x));
        TestCase.assertTrue(LongComparisons.eq(y, x));
        TestCase.assertFalse(LongComparisons.lt(y, x));
        TestCase.assertTrue(LongComparisons.leq(y, x));
        TestCase.assertFalse(LongComparisons.gt(y, x));
        TestCase.assertTrue(LongComparisons.geq(y, x));

        TestCase.assertEquals(LongComparisons.hashCode(x), LongComparisons.hashCode(y));
    }

    private static void lt(long x, long y) {
        // x < y
        TestCase.assertTrue(LongComparisons.compare(x, y) < 0);
        TestCase.assertFalse(LongComparisons.eq(x, y));
        TestCase.assertTrue(LongComparisons.lt(x, y));
        TestCase.assertTrue(LongComparisons.leq(x, y));
        TestCase.assertFalse(LongComparisons.gt(x, y));
        TestCase.assertFalse(LongComparisons.geq(x, y));

        // y > x
        TestCase.assertTrue(LongComparisons.compare(y, x) > 0);
        TestCase.assertFalse(LongComparisons.eq(y, x));
        TestCase.assertFalse(LongComparisons.lt(y, x));
        TestCase.assertFalse(LongComparisons.leq(y, x));
        TestCase.assertTrue(LongComparisons.gt(y, x));
        TestCase.assertTrue(LongComparisons.geq(y, x));
    }

    private static void eq(float x, float y) {
        // x == y
        TestCase.assertEquals(0, FloatComparisons.compare(x, y));
        TestCase.assertTrue(FloatComparisons.eq(x, y));
        TestCase.assertFalse(FloatComparisons.lt(x, y));
        TestCase.assertTrue(FloatComparisons.leq(x, y));
        TestCase.assertFalse(FloatComparisons.gt(x, y));
        TestCase.assertTrue(FloatComparisons.geq(x, y));

        // y == x
        TestCase.assertEquals(0, FloatComparisons.compare(y, x));
        TestCase.assertTrue(FloatComparisons.eq(y, x));
        TestCase.assertFalse(FloatComparisons.lt(y, x));
        TestCase.assertTrue(FloatComparisons.leq(y, x));
        TestCase.assertFalse(FloatComparisons.gt(y, x));
        TestCase.assertTrue(FloatComparisons.geq(y, x));

        TestCase.assertEquals(FloatComparisons.hashCode(x), FloatComparisons.hashCode(y));
    }

    private static void lt(float x, float y) {
        // x < y
        TestCase.assertTrue(FloatComparisons.compare(x, y) < 0);
        TestCase.assertFalse(FloatComparisons.eq(x, y));
        TestCase.assertTrue(FloatComparisons.lt(x, y));
        TestCase.assertTrue(FloatComparisons.leq(x, y));
        TestCase.assertFalse(FloatComparisons.gt(x, y));
        TestCase.assertFalse(FloatComparisons.geq(x, y));

        // y > x
        TestCase.assertTrue(FloatComparisons.compare(y, x) > 0);
        TestCase.assertFalse(FloatComparisons.eq(y, x));
        TestCase.assertFalse(FloatComparisons.lt(y, x));
        TestCase.assertFalse(FloatComparisons.leq(y, x));
        TestCase.assertTrue(FloatComparisons.gt(y, x));
        TestCase.assertTrue(FloatComparisons.geq(y, x));
    }

    private static void eq(double x, double y) {
        // x == y
        TestCase.assertEquals(0, DoubleComparisons.compare(x, y));
        TestCase.assertTrue(DoubleComparisons.eq(x, y));
        TestCase.assertFalse(DoubleComparisons.lt(x, y));
        TestCase.assertTrue(DoubleComparisons.leq(x, y));
        TestCase.assertFalse(DoubleComparisons.gt(x, y));
        TestCase.assertTrue(DoubleComparisons.geq(x, y));

        // y == x
        TestCase.assertEquals(0, DoubleComparisons.compare(y, x));
        TestCase.assertTrue(DoubleComparisons.eq(y, x));
        TestCase.assertFalse(DoubleComparisons.lt(y, x));
        TestCase.assertTrue(DoubleComparisons.leq(y, x));
        TestCase.assertFalse(DoubleComparisons.gt(y, x));
        TestCase.assertTrue(DoubleComparisons.geq(y, x));

        TestCase.assertEquals(DoubleComparisons.hashCode(x), DoubleComparisons.hashCode(y));
    }

    private static void lt(double x, double y) {
        // x < y
        TestCase.assertTrue(DoubleComparisons.compare(x, y) < 0);
        TestCase.assertFalse(DoubleComparisons.eq(x, y));
        TestCase.assertTrue(DoubleComparisons.lt(x, y));
        TestCase.assertTrue(DoubleComparisons.leq(x, y));
        TestCase.assertFalse(DoubleComparisons.gt(x, y));
        TestCase.assertFalse(DoubleComparisons.geq(x, y));

        // y > x
        TestCase.assertTrue(DoubleComparisons.compare(y, x) > 0);
        TestCase.assertFalse(DoubleComparisons.eq(y, x));
        TestCase.assertFalse(DoubleComparisons.lt(y, x));
        TestCase.assertFalse(DoubleComparisons.leq(y, x));
        TestCase.assertTrue(DoubleComparisons.gt(y, x));
        TestCase.assertTrue(DoubleComparisons.geq(y, x));
    }
}
