/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.function;

import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.deephaven.function.Basic.isNull;
import static io.deephaven.function.Numeric.max;
import static io.deephaven.function.Numeric.maxObj;
import static io.deephaven.function.Numeric.min;
import static io.deephaven.function.Numeric.minObj;
import static io.deephaven.function.Sort.sort;
import static io.deephaven.function.Sort.sortObj;
import static io.deephaven.function.Sort.sortDescending;
import static io.deephaven.function.Sort.sortDescendingObj;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static org.junit.Assert.*;

@SuppressWarnings({"ConstantConditions", "RedundantArrayCreation", "UnnecessaryBoxing", "deprecation", "RedundantCast"})
public class TestAmbiguity {

    @Test
    public void testIsNull() {
        assertTrue(isNull(QueryConstants.NULL_BYTE));
        assertTrue(isNull(QueryConstants.NULL_SHORT));
        assertTrue(isNull(QueryConstants.NULL_INT));
        assertTrue(isNull(QueryConstants.NULL_FLOAT));
        assertTrue(isNull(QueryConstants.NULL_LONG));
        assertTrue(isNull(QueryConstants.NULL_DOUBLE));
        assertTrue(isNull(QueryConstants.NULL_BOOLEAN));
        assertTrue(isNull(QueryConstants.NULL_CHAR));
        assertTrue(isNull(null));
        assertFalse(isNull(Integer.valueOf(QueryConstants.NULL_INT)));
        assertFalse(isNull(Integer.valueOf(5)));
    }

    @Test
    public void testMin() {
        assertEquals((byte) 1, min((byte) 1, (byte) 2, (byte) 3));
        assertEquals((byte) 1, min(new byte[] {1, 2, 3}));
        assertEquals((byte) 1, min((byte) 1, (byte) 2, (byte) 3));

        assertEquals((short) 1, min((short) 1, (short) 2, (short) 3));
        assertEquals((short) 1, min(new short[] {1, 2, 3}));
        assertEquals((short) 1, min((short) 1, (short) 2, (short) 3));

        assertEquals(1, min(1, 2, 3));
        assertEquals(1, min(1, 2, 3));
        assertEquals(1, min(new int[] {1, 2, 3}));

        assertEquals(1L, min(1L, 2L, 3L));
        assertEquals(1L, min(new long[] {1, 2, 3}));
        assertEquals(1L, min(1L, 2L, 3L));

        assertEquals(1f, min(1f, 2f, 3f), 0);
        assertEquals(1f, min(new float[] {1, 2, 3}), 0);
        assertEquals(1f, min(1f, 2f, 3f), 0);

        assertEquals(1d, min(1d, 2d, 3d), 0);
        assertEquals(1d, min(new double[] {1d, 2d, 3d}), 0);
        assertEquals(1d, min(1d, new Double(2), 3d), 0);

        assertTrue(new BigInteger("1").equals(minObj(new BigInteger("2"), new BigInteger("1"), new BigInteger("3"))));

        assertEquals("A", minObj("A", "B"));

        assertEquals(1, min((byte) 1, (byte) 2));
        assertEquals(1, min((byte) 1, (short) 2));
        assertEquals(1, min((byte) 1, 2));
        assertEquals(1, min((byte) 1, 2L));
        assertEquals(1, min((byte) 1, 2f), 0);
        assertEquals(1, min((byte) 1, 2d), 0);
        assertEquals(QueryConstants.NULL_INT, min(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE));
        assertEquals(26L, min(QueryConstants.NULL_BYTE, 26L));

        assertEquals((short) 1, min((short) 1, (byte) 2));
        assertEquals((short) 1, min((short) 1, (short) 2));
        assertEquals((short) 1, min((short) 1, 2));
        assertEquals((short) 1, min((short) 1, 2L));
        assertEquals((short) 1, min((short) 1, 2f), 0);
        assertEquals((short) 1, min((short) 1, 2d), 0);
        assertEquals(QueryConstants.NULL_FLOAT, min(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT), 0d);
        assertEquals(QueryConstants.NULL_FLOAT, min(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(26L, min(QueryConstants.NULL_SHORT, 26L), 0d);

        assertEquals(1, min(1, (byte) 2));
        assertEquals(1, min(1, (short) 2));
        assertEquals(1, min(1, 2));
        assertEquals(1, min(1, 2L));
        assertEquals(1, min(1, 2f), 0);
        assertEquals(1, min(1, 2d), 0);
        assertEquals(QueryConstants.NULL_INT, min(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT));
        assertEquals(26L, min(QueryConstants.NULL_INT, 26L));

        assertEquals(1L, min(1L, (byte) 2));
        assertEquals(1L, min(1L, (short) 2));
        assertEquals(1L, min(1L, 2));
        assertEquals(1L, min(1L, 2L));
        assertEquals(1L, min(1L, 2f), 0);
        assertEquals(1L, min(1L, 2d), 0);
        assertEquals(QueryConstants.NULL_DOUBLE, min(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(((double) QueryConstants.NULL_LONG) - 1d,
                min(QueryConstants.NULL_LONG, ((double) QueryConstants.NULL_LONG) - 1d), 0d);

        assertEquals(1, min(1f, (byte) 2), 0);
        assertEquals(1, min(1f, (short) 2), 0);
        assertEquals(1, min(1f, 2), 0);
        assertEquals(1, min(1f, 2L), 0);
        assertEquals(1, min(1f, 2f), 0);
        assertEquals(1, min(1f, 2d), 0);
        assertEquals(QueryConstants.NULL_DOUBLE, min(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(5L, min(QueryConstants.NULL_FLOAT, 5L), 0d);
        assertEquals(5L, min(Float.NaN, 5L), 0d);
        assertEquals(QueryConstants.NULL_DOUBLE, min(Float.NaN, QueryConstants.NULL_LONG), 0d);

        assertEquals(1, min(1d, (byte) 2), 0);
        assertEquals(1, min(1d, (short) 2), 0);
        assertEquals(1, min(1d, 2), 0);
        assertEquals(1, min(1d, 2L), 0);
        assertEquals(1, min(1d, 2f), 0);
        assertEquals(1, min(1d, 2d), 0);

        assertEquals(QueryConstants.NULL_DOUBLE, min(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(5L, min(QueryConstants.NULL_DOUBLE, 5L), 0d);

        assertEquals(1, min(1, new Double(2)), 0);

        // double value is just less than Long.MAX_VALUE
        assertEquals(9.2233720368547738E18, min(Long.MAX_VALUE, 9.2233720368547738E18, Double.NaN));
        // double value doesn't have enough precision
        assertEquals(1.23456E9, min(1.23456E9, 1234567890L, Long.MAX_VALUE));
        assertEquals(1234567890L, min(1.234567891E9, 1234567890L, Long.MAX_VALUE));
        assertEquals(1234567890L, min(1.2345678901E9, 1234567890L, Long.MAX_VALUE));
        // double and long values are equivalent
        assertEquals(1234567890L, min(1234567890L, 1.23456789E9, Long.MAX_VALUE));
        assertEquals(1.23456789E9, min(1.23456789E9, 1234567890L, Long.MAX_VALUE));
        // Both values are near to min values
        assertEquals(Long.MIN_VALUE + 1, min(Long.MIN_VALUE + 1, -Double.MAX_VALUE + 1d, 0d));
        // Positive and negative infinity values (Order matters when both Double.NEGATIVE_INFINITY and
        // Float.NEGATIVE_INFINITY are present)
        assertEquals(Double.NEGATIVE_INFINITY, min(Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        assertEquals(Float.NEGATIVE_INFINITY,
                min(Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN, Float.NaN));

        assertEquals(Double.NEGATIVE_INFINITY, min(Long.MIN_VALUE, Double.NEGATIVE_INFINITY, 0d));
        assertEquals(Double.NEGATIVE_INFINITY, min(Double.NEGATIVE_INFINITY, Long.MIN_VALUE, 0d));
        assertEquals(Double.NEGATIVE_INFINITY, min(Long.MIN_VALUE + 1, Double.NEGATIVE_INFINITY, 0d));
        assertEquals(Double.NEGATIVE_INFINITY, min(Double.NEGATIVE_INFINITY, Long.MIN_VALUE + 1, 0d));

        assertEquals(Float.NEGATIVE_INFINITY, min(Integer.MIN_VALUE, Float.NEGATIVE_INFINITY), 0d);
        assertEquals(Float.NEGATIVE_INFINITY, min(Float.NEGATIVE_INFINITY, Integer.MIN_VALUE), 0d);
        assertEquals(Float.NEGATIVE_INFINITY, min(Integer.MIN_VALUE + 1, Float.NEGATIVE_INFINITY), 0d);
        assertEquals(Float.NEGATIVE_INFINITY, min(Float.NEGATIVE_INFINITY, Integer.MIN_VALUE + 1), 0d);

        assertEquals((int) Math.pow(2, 23), (float) Math.pow(2, 23), 0);
        // float value is just less than Integer.MAX_VALUE
        assertEquals(2.14748344444E9f, min(2.14748344444E9f, Integer.MAX_VALUE), 0d);
        // double value doesn't have enough precision
        assertEquals(1.23456712354E8f, min(1.23456712354E8f, 123456789), 0d);
        assertEquals(1.23456E5f, min(1.23456E5f, 1234567), 0d);
        assertEquals(123456, min(1.234561E5f, 123456), 0d);
        // double and long values are equivalent
        assertEquals(123456, min(123456, 1.23456E5f), 0d);
        assertEquals(1.23456E5f, min(1.23456E5f, 123456), 0d);
        // Both values are near to min values
        assertEquals(Integer.MIN_VALUE + 1, min(-Float.MAX_VALUE + 1, Integer.MIN_VALUE + 1), 0d);
        assertEquals(new Float("-1.34758724E20"), min(Integer.MIN_VALUE + 1, new Float("-1.34758724E20")), 0d);
        assertEquals(new Float("-1.347587244542345673435434E20"),
                min(Integer.MIN_VALUE + 1, new Float("-1.347587244542345673435434E20")), 0d);
        // Positive and negative infinity values (Order matters when both Double.NEGATIVE_INFINITY and
        // Float.NEGATIVE_INFINITY are present)
        assertEquals(Double.NEGATIVE_INFINITY, min(Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        assertEquals(Float.NEGATIVE_INFINITY,
                min(Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN, Float.NaN));

        assertEquals(-123.45d, min(-123.45d, -123L, 0d));
        assertEquals(-123.45d, min(-123L, -123.45d, 0d));
        assertEquals(123L, min(123.45d, 123L, Long.MAX_VALUE));
        assertEquals(123L, min(123L, 123.45d, Long.MAX_VALUE));
        assertEquals(-0.2d, min(0L, -0.2d, Long.MAX_VALUE));
        assertEquals(-1.2d, min(-1L, -1.2d, Long.MAX_VALUE));
        assertEquals(-0.2d, min(-0.2d, 0L, Long.MAX_VALUE));
        assertEquals(-1.2d, min(-1.2d, -1L, Long.MAX_VALUE));

        assertEquals(QueryConstants.NULL_LONG, min(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE));
        assertEquals((double) -123.45f, min(-123.45f, -123), 0d);
        assertEquals((double) -123.45f, min(-123, -123.45f), 0d);

        // (double, long)
        assertEquals(9223372036753776384.0d, min(9223372036753776384.0d, 9223372036853776385L), 0d);
        assertEquals(123L, min(123.45, 123L), 0d);
        assertEquals(-123.45, min(-123.45, -123L), 0d);
        assertEquals(5L, min(io.deephaven.util.QueryConstants.NULL_DOUBLE, 5L), 0d);
        assertEquals(5L, min(Double.NaN, 5L), 0d);
        assertEquals(5d, min(5d, io.deephaven.util.QueryConstants.NULL_LONG), 0d);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                min(io.deephaven.util.QueryConstants.NULL_DOUBLE, io.deephaven.util.QueryConstants.NULL_LONG), 0d);
        assertEquals(NULL_DOUBLE, min(Double.NaN, io.deephaven.util.QueryConstants.NULL_LONG), 0d);

        // (long, double)
        assertEquals(9223372036753776384.0d, min(9223372036853776385L, 9223372036753776384.0d), 0d);
        assertEquals(9223372036853776385L, min(9223372036853876385L, 9223372036853776384.0d), 0d);
        assertEquals(123L, min(123L, 123.45), 0d);
        assertEquals(-123.45, min(-123L, -123.45), 0d);
        assertEquals(5L, min(5L, io.deephaven.util.QueryConstants.NULL_DOUBLE), 0d);
        assertEquals(5L, min(5L, Double.NaN), 0d);
        assertEquals(5d, min(io.deephaven.util.QueryConstants.NULL_LONG, 5d), 0d);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                min(io.deephaven.util.QueryConstants.NULL_LONG, io.deephaven.util.QueryConstants.NULL_DOUBLE), 0d);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                min(io.deephaven.util.QueryConstants.NULL_LONG, Double.NaN), 0d);

    }

    @Test
    public void testMax() {
        assertEquals((byte) 3, max((byte) 1, (byte) 2, (byte) 3));
        assertEquals((byte) 3, max(new byte[] {1, 2, 3}));
        assertEquals((byte) 3, max((byte) 1, (byte) 2, (byte) 3));
        assertEquals(QueryConstants.NULL_INT, max(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE));
        assertEquals((short) (QueryConstants.NULL_BYTE - 1),
                max(QueryConstants.NULL_BYTE, (short) (QueryConstants.NULL_BYTE - 1)));

        assertEquals((short) 3, max((short) 1, (short) 2, (short) 3));
        assertEquals((short) 3, max(new short[] {1, 2, 3}));
        assertEquals((short) 3, max((short) 1, (short) 2, (short) 3));
        assertEquals(QueryConstants.NULL_SHORT, max(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE));
        assertEquals(5, max(QueryConstants.NULL_SHORT, 5));

        assertEquals(3, max(1, 2, 3));
        assertEquals(3, max(1, 2, 3));
        assertEquals(3, max(new int[] {1, 2, 3}));
        assertEquals(QueryConstants.NULL_DOUBLE, max(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(5L, max(QueryConstants.NULL_INT, 5L));

        assertEquals(3L, max(1L, 2L, 3L));
        assertEquals(3L, max(new long[] {1, 2, 3}));
        assertEquals(3L, max(1L, 2L, 3L));
        assertEquals(QueryConstants.NULL_DOUBLE, max(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(5, max(QueryConstants.NULL_LONG, 5));

        assertEquals(3f, max(1f, 2f, 3f), 0d);
        assertEquals(3f, max(new float[] {1, 2, 3}), 0d);
        assertEquals(3f, max(1f, 2f, 3f), 0d);
        assertEquals(QueryConstants.NULL_DOUBLE, max(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(5L, max(QueryConstants.NULL_FLOAT, 5L), 0d);

        assertEquals(3d, max(1d, 2d, 3d), 0d);
        assertEquals(3d, max(new double[] {1d, 2d, 3d}), 0d);
        assertEquals(3d, max(1d, new Double(2), 3d), 0d);
        assertEquals(QueryConstants.NULL_DOUBLE, max(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT), 0d);
        assertEquals(5d, max(QueryConstants.NULL_DOUBLE, 5d), 0d);

        assertTrue(new BigInteger("3").equals(maxObj(new BigInteger("1"), new BigInteger("2"), new BigInteger("3"))));

        assertEquals("B", maxObj("A", "B"));

        assertEquals(2, max((byte) 1, (byte) 2));
        assertEquals(2, max((byte) 1, (short) 2));
        assertEquals(2, max((byte) 1, 2));
        assertEquals(2, max((byte) 1, 2L));
        assertEquals(2, max((byte) 1, 2f), 0);
        assertEquals(2, max((byte) 1, 2d), 0);

        assertEquals((short) 2, max((short) 1, (byte) 2));
        assertEquals((short) 2, max((short) 1, (short) 2));
        assertEquals((short) 2, max((short) 1, 2));
        assertEquals((short) 2, max((short) 1, 2L));
        assertEquals((short) 2, max((short) 1, 2f), 0);
        assertEquals((short) 2, max((short) 1, 2d), 0);

        assertEquals(2, max(2, (byte) 2));
        assertEquals(2, max(2, (short) 2));
        assertEquals(2, max(2, 2));
        assertEquals(2, max(2, 2L));
        assertEquals(2, max(2, 2f), 0);
        assertEquals(2, max(2, 2d), 0);

        assertEquals(2L, max(1L, (byte) 2));
        assertEquals(2L, max(1L, (short) 2));
        assertEquals(2L, max(1L, 2));
        assertEquals(2L, max(1L, 2L));
        assertEquals(2L, max(1L, 2f), 0);
        assertEquals(2L, max(1L, 2d), 0);

        assertEquals(2, max(1f, (byte) 2), 0);
        assertEquals(2, max(1f, (short) 2), 0);
        assertEquals(2, max(1f, 2), 0);
        assertEquals(2, max(1f, 2L), 0);
        assertEquals(2, max(1f, 2f), 0);
        assertEquals(2, max(1f, 2d), 0);

        assertEquals(2, max(1d, (byte) 2), 0);
        assertEquals(2, max(1d, (short) 2), 0);
        assertEquals(2, max(1d, 2), 0);
        assertEquals(2, max(1d, 2L), 0);
        assertEquals(2, max(1d, 2f), 0);
        assertEquals(2, max(1d, 2d), 0);

        assertEquals(new BigDecimal("2.5"), maxObj(new BigDecimal("1"), new BigDecimal("2.5")));
        assertEquals(2d, max(1, new Double(2)), 0);

        // double value is just greater than Long.MAX_VALUE
        assertEquals(9.223372036854776807E18, max(9.223372036854776807E18, Long.MAX_VALUE, Double.NaN));
        // double value doesn't have enough precision
        assertEquals(1234567890L, max(1234567890L, 1.23456E9, 0d));
        // double value is slightly higher than the long value
        assertEquals(1.234567891E9, max(1234567890L, 1.234567891E9, 0d));
        // double and long values are equivalent
        assertEquals(1234567890L, max(1234567890L, 1.23456789E9, 0d));
        assertEquals(1.23456789E9, max(1.23456789E9, 1234567890L, 0d));
        assertEquals(1234567891L, max(1.23456789E9, 1234567891L, 0d));
        // Both values are near to min values
        assertEquals(Double.MAX_VALUE, max(Long.MAX_VALUE, Double.MAX_VALUE, 0d));
        // Positive and negative infinity values (Order matters when both Double.POSITIVE_INFINITY and
        // Float.POSITIVE_INFINITY are present)
        assertEquals(Double.POSITIVE_INFINITY, max(Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY));
        assertEquals(Float.POSITIVE_INFINITY,
                max(Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN, Float.NaN));

        assertEquals(Double.POSITIVE_INFINITY, max(Long.MAX_VALUE, Double.POSITIVE_INFINITY, 0d));
        assertEquals(Double.POSITIVE_INFINITY, max(Double.POSITIVE_INFINITY, Long.MAX_VALUE, 0d));
        assertEquals(Double.POSITIVE_INFINITY, max(Long.MAX_VALUE - 1, Double.POSITIVE_INFINITY, 0d));
        assertEquals(Double.POSITIVE_INFINITY, max(Double.POSITIVE_INFINITY, Long.MAX_VALUE - 1, 0d));

        assertEquals(Float.POSITIVE_INFINITY, max(Integer.MAX_VALUE, Float.POSITIVE_INFINITY), 0d);
        assertEquals(Float.POSITIVE_INFINITY, max(Float.POSITIVE_INFINITY, Integer.MAX_VALUE), 0d);
        assertEquals(Float.POSITIVE_INFINITY, max(Integer.MAX_VALUE - 1, Float.POSITIVE_INFINITY), 0d);
        assertEquals(Float.POSITIVE_INFINITY, max(Float.POSITIVE_INFINITY, Integer.MAX_VALUE - 1), 0d);

        // float value is just less than Integer.MAX_VALUE
        assertEquals(Integer.MAX_VALUE, max(2.14748344444E9f, Integer.MAX_VALUE), 0d);
        assertEquals(2.147489E9f, max(2.147489E9f, Integer.MAX_VALUE), 0d);
        // float value doesn't have enough precision
        assertEquals(1.234568E8f, max(1.234568E8f, 123456789), 0d);
        assertEquals(1234567, max(1.23456E5f, 1234567), 0d);
        assertEquals(1.234561E5f, max(1.234561E5f, 123456), 0d);
        // int and long float are equivalent
        assertEquals(123456, max(123456, 1.23456E5f), 0d);
        assertEquals(1.23456E5f, max(1.23456E5f, 123456), 0d);
        // Both values are near to max values
        assertEquals(Float.MAX_VALUE, max(Float.MAX_VALUE, Integer.MAX_VALUE), 0d);
        assertEquals(new Float("1.34758724E20"), max(Integer.MAX_VALUE - 1, new Float("1.34758724E20")), 0d);
        assertEquals(new Float("1.347587244542345673435434E20"),
                max(Integer.MAX_VALUE - 1, new Float("1.347587244542345673435434E20")), 0d);

        assertEquals(5f, max(5f, Integer.MIN_VALUE), 0d);
        assertEquals(5, max(-Float.MAX_VALUE, 5), 0d);
        assertEquals(QueryConstants.NULL_DOUBLE, max(-Float.MAX_VALUE, Integer.MIN_VALUE), 0d);
        // Positive and negative infinity values (Order matters when both Double.NEGATIVE_INFINITY and
        // Float.NEGATIVE_INFINITY are present)
        assertEquals(Double.POSITIVE_INFINITY, max(Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY,
                Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY));
        assertEquals(Float.POSITIVE_INFINITY,
                max(Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN, Float.NaN));

        assertEquals(-123L, max(-123.45d, -123L, Long.MIN_VALUE));
        assertEquals(-123L, max(-123L, -123.45d, Long.MIN_VALUE));
        assertEquals(123.45d, max(123.45d, 123L, Long.MIN_VALUE));
        assertEquals(123.45d, max(123L, 123.45d, Long.MIN_VALUE));

        assertEquals((double) 123.45f, max(123.45f, 123), 0d);

        // (double, long)
        assertEquals(9223372036853776384.0d, max(9223372036853776384.0d, 9223372036753776385L), 0d);
        assertEquals(9223372036853876385.0d, max(9223372036853876385.0d, 9223372036853776385L), 0d);
        assertEquals(123.45, max(123.45, 123L), 0d);
        assertEquals(-123, max(-123.45, -123L), 0d);
        assertEquals(5L, max(io.deephaven.util.QueryConstants.NULL_DOUBLE, 5L), 0d);
        assertEquals(5d, max(5d, io.deephaven.util.QueryConstants.NULL_LONG), 0d);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                max(io.deephaven.util.QueryConstants.NULL_DOUBLE, io.deephaven.util.QueryConstants.NULL_LONG), 0d);

        // (long, double)
        assertEquals(9223372036853776384.0d, max(9223372036753776385L, 9223372036853776384.0d), 0d);
        assertEquals(9223372036853876385.0d, max(9223372036853776385L, 9223372036853876385.0d), 0d);
        assertEquals(123.45, max(123L, 123.45), 0d);
        assertEquals(-123, max(-123L, -123.45), 0d);
        assertEquals(5L, max(5L, io.deephaven.util.QueryConstants.NULL_DOUBLE), 0d);
        assertEquals(5d, max(io.deephaven.util.QueryConstants.NULL_LONG, 5d), 0d);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                max(io.deephaven.util.QueryConstants.NULL_LONG, io.deephaven.util.QueryConstants.NULL_DOUBLE), 0d);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                max(io.deephaven.util.QueryConstants.NULL_LONG, Double.NaN), 0d);

        assertEquals(NULL_DOUBLE, max(Double.NaN, io.deephaven.util.QueryConstants.NULL_LONG), 0d);
    }

    @Test
    public void testSort() {
        final byte[] expectedSortedPrimBytes = new byte[] {1, 2, 3};
        assertArrayEquals(expectedSortedPrimBytes, sort((byte) 1, (byte) 2, (byte) 3));
        assertArrayEquals(expectedSortedPrimBytes, sort(new byte[] {1, 2, 3}));
        assertArrayEquals(expectedSortedPrimBytes, sort((byte) 1, (byte) 2, (byte) 3));

        final short[] expectedSortedPrimShorts = new short[] {1, 2, 3};
        assertArrayEquals(expectedSortedPrimShorts, sort((short) 1, (short) 2, (short) 3));
        assertArrayEquals(expectedSortedPrimShorts, sort(new short[] {1, 2, 3}));
        assertArrayEquals(expectedSortedPrimShorts, sort((short) 1, (short) 2, (short) 3));

        final int[] expectedSortedPrimInts = new int[] {1, 2, 3};
        assertArrayEquals(expectedSortedPrimInts, sort(1, 2, 3));
        assertArrayEquals(expectedSortedPrimInts, sort(1, 2, 3));
        assertArrayEquals(expectedSortedPrimInts, sort(new int[] {1, 2, 3}));

        final long[] expectedSortedPrimLongs = new long[] {1, 2, 3};
        assertArrayEquals(expectedSortedPrimLongs, sort(1L, 2L, 3L));
        assertArrayEquals(expectedSortedPrimLongs, sort(new long[] {1, 2, 3}));
        assertArrayEquals(expectedSortedPrimLongs, sort(1L, 2L, 3L));

        final float[] expectedSortedPrimFloats = new float[] {1f, 2f, 3f};
        assertArrayEquals(expectedSortedPrimFloats, sort(1f, 2f, 3f), 0);
        assertArrayEquals(expectedSortedPrimFloats, sort(new float[] {1, 2, 3}), 0);
        assertArrayEquals(expectedSortedPrimFloats, sort(1f, 2f, 3f), 0);

        final double[] expectedSortedPrimDoubles = new double[] {1d, 2d, 3d};
        assertArrayEquals(expectedSortedPrimDoubles, sort(1d, 2d, 3d), 0);
        assertArrayEquals(expectedSortedPrimDoubles, sort(new double[] {1d, 2d, 3d}), 0);
        assertArrayEquals(expectedSortedPrimDoubles, sort(1d, new Double(2), 3d), 0);

        assertArrayEquals(new BigInteger[] {new BigInteger("1"), new BigInteger("2"), new BigInteger("3")},
                sortObj(new BigInteger("1"), new BigInteger("2"), new BigInteger("3")));

        assertArrayEquals(new double[] {1, 2d}, sort(2d, 1), 0d);

        assertArrayEquals(new String[] {"A", "B"}, sortObj("B", "A"));
        assertArrayEquals(new double[] {1d, 2d, 3d, Double.NaN}, sort(Double.NaN, 1d, 2d, 3d), 0d);
    }

    @Test
    public void testSortDescending() {
        final byte[] expectedSortedPrimBytes = new byte[] {3, 2, 1};
        assertArrayEquals(expectedSortedPrimBytes, sortDescending((byte) 1, (byte) 2, (byte) 3));
        assertArrayEquals(expectedSortedPrimBytes, sortDescending(new byte[] {1, 2, 3}));
        assertArrayEquals(expectedSortedPrimBytes, sortDescending((byte) 1, (byte) 2, (byte) 3));

        final short[] expectedSortedPrimShorts = new short[] {3, 2, 1};
        assertArrayEquals(expectedSortedPrimShorts, sortDescending((short) 1, (short) 2, (short) 3));
        assertArrayEquals(expectedSortedPrimShorts, sortDescending(new short[] {1, 2, 3}));
        assertArrayEquals(expectedSortedPrimShorts, sortDescending((short) 1, (short) 2, (short) 3));

        final int[] expectedSortedPrimInts = new int[] {3, 2, 1};
        assertArrayEquals(expectedSortedPrimInts, sortDescending(1, 2, 3));
        assertArrayEquals(expectedSortedPrimInts, sortDescending(1, 2, 3));
        assertArrayEquals(expectedSortedPrimInts, sortDescending(new int[] {1, 2, 3}));


        final long[] expectedSortedPrimLongs = new long[] {3L, 2L, 1L};
        assertArrayEquals(expectedSortedPrimLongs, sortDescending(1L, 2L, 3L));
        assertArrayEquals(expectedSortedPrimLongs, sortDescending(new long[] {1, 2, 3}));
        assertArrayEquals(expectedSortedPrimLongs, sortDescending(1L, 2L, 3L));

        final float[] expectedSortedPrimFloats = new float[] {3f, 2f, 1f};
        assertArrayEquals(expectedSortedPrimFloats, sortDescending(1f, 2f, 3f), 0);
        assertArrayEquals(expectedSortedPrimFloats, sortDescending(new float[] {1, 2, 3}), 0);
        assertArrayEquals(expectedSortedPrimFloats, sortDescending(1f, 2f, 3f), 0);

        final double[] expectedSortedPrimDoubles = new double[] {3d, 2d, 1d};
        assertArrayEquals(expectedSortedPrimDoubles, sortDescending(1d, 2d, 3d), 0);
        assertArrayEquals(expectedSortedPrimDoubles, sortDescending(new double[] {1d, 2d, 3d}), 0);
        assertArrayEquals(expectedSortedPrimDoubles, sortDescending(1d, new Double(2), 3d), 0);

        assertArrayEquals(new BigInteger[] {new BigInteger("3"), new BigInteger("2"), new BigInteger("1")},
                sortDescendingObj(new BigInteger("1"), new BigInteger("2"), new BigInteger("3")));

        assertArrayEquals(new double[] {2d, 1}, sortDescending(2d, 1), 0.0d);

        assertArrayEquals(new String[] {"B", "A"}, sortDescendingObj("A", "B"));
    }

    @Test
    public void testMinMaxException() {
        // (double, long)
        try {
            min(9223372036853776384.25E2, 9223372036853776385L);
            fail("Shouldn't come here for min(9223372036853776384.25E2, 9223372036853776385l)");
        } catch (final Cast.CastDoesNotPreserveValue uoe) {
            assertTrue(uoe.getMessage().contains("Not supported: min("));
        }

        try {
            max(9223372036753776384.25, 9223372036853776385L);
            fail("Shouldn't come here for max(9223372036853776384.25E2, 9223372036853776385l)");
        } catch (final Cast.CastDoesNotPreserveValue uoe) {
            assertTrue(uoe.getMessage().contains("Not supported: max("));
        }

        // (long, double)
        try {
            min(9223372036853776385L, 9223372036853776384.25E2);
            fail("Shouldn't come here for min(9223372036853776385l, 9223372036853776384.25E2)");
        } catch (final UnsupportedOperationException uoe) {
            assertTrue(uoe.getMessage().contains("Not supported: min("));
        }

        try {
            max(9223372036853776385L, 9223372036753776384.25);
            fail("Shouldn't come here for max(9223372036853776385l, 9223372036753776384.25)");
        } catch (final UnsupportedOperationException uoe) {
            assertTrue(uoe.getMessage().contains("Not supported: max("));
        }
    }
}
