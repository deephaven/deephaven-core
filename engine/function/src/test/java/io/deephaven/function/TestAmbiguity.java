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

import static io.deephaven.function.BooleanPrimitives.isNull;
import static io.deephaven.function.ByteNumericPrimitives.max;
import static io.deephaven.function.ByteNumericPrimitives.min;
import static io.deephaven.function.ByteNumericPrimitives.sort;
import static io.deephaven.function.ByteNumericPrimitives.sortDescending;
import static io.deephaven.function.BytePrimitives.isNull;
import static io.deephaven.function.CharacterPrimitives.isNull;
import static io.deephaven.function.ComparePrimitives.max;
import static io.deephaven.function.ComparePrimitives.min;
import static io.deephaven.function.DoubleNumericPrimitives.max;
import static io.deephaven.function.DoubleNumericPrimitives.min;
import static io.deephaven.function.DoubleNumericPrimitives.sort;
import static io.deephaven.function.DoubleNumericPrimitives.sortDescending;
import static io.deephaven.function.DoublePrimitives.isNull;
import static io.deephaven.function.FloatNumericPrimitives.max;
import static io.deephaven.function.FloatNumericPrimitives.min;
import static io.deephaven.function.FloatNumericPrimitives.sort;
import static io.deephaven.function.FloatNumericPrimitives.sortDescending;
import static io.deephaven.function.FloatPrimitives.isNull;
import static io.deephaven.function.IntegerNumericPrimitives.max;
import static io.deephaven.function.IntegerNumericPrimitives.min;
import static io.deephaven.function.IntegerNumericPrimitives.sort;
import static io.deephaven.function.IntegerNumericPrimitives.sortDescending;
import static io.deephaven.function.IntegerPrimitives.isNull;
import static io.deephaven.function.LongNumericPrimitives.max;
import static io.deephaven.function.LongNumericPrimitives.min;
import static io.deephaven.function.LongNumericPrimitives.sort;
import static io.deephaven.function.LongNumericPrimitives.sortDescending;
import static io.deephaven.function.LongPrimitives.isNull;
import static io.deephaven.function.ObjectPrimitives.isNull;
import static io.deephaven.function.ObjectPrimitives.max;
import static io.deephaven.function.ObjectPrimitives.min;
import static io.deephaven.function.ObjectPrimitives.sort;
import static io.deephaven.function.ObjectPrimitives.sortDescending;
import static io.deephaven.function.ShortNumericPrimitives.max;
import static io.deephaven.function.ShortNumericPrimitives.min;
import static io.deephaven.function.ShortNumericPrimitives.sort;
import static io.deephaven.function.ShortNumericPrimitives.sortDescending;
import static io.deephaven.function.ShortPrimitives.isNull;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static org.junit.Assert.*;

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
        assertFalse(isNull(new Integer(QueryConstants.NULL_INT)));
        assertFalse(isNull(new Integer(5)));
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

        assertEquals(1l, min(1l, 2l, 3l));
        assertEquals(1l, min(new long[] {1, 2, 3}));
        assertEquals(1l, min(1l, 2l, 3l));

        assertEquals(1f, min(1f, 2f, 3f), 0);
        assertEquals(1f, min(new float[] {1, 2, 3}), 0);
        assertEquals(1f, min(1f, 2f, 3f), 0);

        assertEquals(1d, min(1d, 2d, 3d), 0);
        assertEquals(1d, min(new double[] {1d, 2d, 3d}), 0);
        assertEquals(1d, min(1d, new Double(2), 3d), 0);

        assertTrue(new BigInteger("1").equals(min(new BigInteger("2"), new BigInteger("1"), new BigInteger("3"))));

        assertEquals(new BigDecimal("0.5236598874"),
                min(new BigInteger("2"), 10, 2.0, 5.6f, 5l, (byte) 3, (short) 1, new BigDecimal("0.5236598874")));

        assertEquals("A", min("A", "B"));

        assertEquals(1, min((byte) 1, (byte) 2));
        assertEquals(1, min((byte) 1, (short) 2));
        assertEquals(1, min((byte) 1, 2));
        assertEquals(1, min((byte) 1, 2l));
        assertEquals(1, min((byte) 1, 2f), 0);
        assertEquals(1, min((byte) 1, 2d), 0);
        assertEquals(QueryConstants.NULL_INT, min(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE));
        assertEquals(26l, min(QueryConstants.NULL_BYTE, 26l));

        assertEquals((short) 1, min((short) 1, (byte) 2));
        assertEquals((short) 1, min((short) 1, (short) 2));
        assertEquals((short) 1, min((short) 1, 2));
        assertEquals((short) 1, min((short) 1, 2l));
        assertEquals((short) 1, min((short) 1, 2f), 0);
        assertEquals((short) 1, min((short) 1, 2d), 0);
        assertEquals(QueryConstants.NULL_FLOAT, min(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT), 0d);
        assertEquals(QueryConstants.NULL_FLOAT, min(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(26l, min(QueryConstants.NULL_SHORT, 26l), 0d);

        assertEquals(1, min(1, (byte) 2));
        assertEquals(1, min(1, (short) 2));
        assertEquals(1, min(1, 2));
        assertEquals(1, min(1, 2l));
        assertEquals(1, min(1, 2f), 0);
        assertEquals(1, min(1, 2d), 0);
        assertEquals(QueryConstants.NULL_INT, min(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT));
        assertEquals(26l, min(QueryConstants.NULL_INT, 26l));

        assertEquals(1l, min(1l, (byte) 2));
        assertEquals(1l, min(1l, (short) 2));
        assertEquals(1l, min(1l, 2));
        assertEquals(1l, min(1l, 2l));
        assertEquals(1l, min(1l, 2f), 0);
        assertEquals(1l, min(1l, 2d), 0);
        assertEquals(QueryConstants.NULL_DOUBLE, min(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(((double) QueryConstants.NULL_LONG) - 1d,
                min(QueryConstants.NULL_LONG, ((double) QueryConstants.NULL_LONG) - 1d), 0d);

        assertEquals(1, min(1f, (byte) 2), 0);
        assertEquals(1, min(1f, (short) 2), 0);
        assertEquals(1, min(1f, 2), 0);
        assertEquals(1, min(1f, 2l), 0);
        assertEquals(1, min(1f, 2f), 0);
        assertEquals(1, min(1f, 2d), 0);
        assertEquals(QueryConstants.NULL_DOUBLE, min(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(5l, min(QueryConstants.NULL_FLOAT, 5l), 0d);
        assertEquals(5l, min(Float.NaN, 5l), 0d);
        assertEquals(QueryConstants.NULL_DOUBLE, min(Float.NaN, QueryConstants.NULL_LONG), 0d);

        assertEquals(1, min(1d, (byte) 2), 0);
        assertEquals(1, min(1d, (short) 2), 0);
        assertEquals(1, min(1d, 2), 0);
        assertEquals(1, min(1d, 2l), 0);
        assertEquals(1, min(1d, 2f), 0);
        assertEquals(1, min(1d, 2d), 0);

        assertEquals(QueryConstants.NULL_DOUBLE, min(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(5l, min(QueryConstants.NULL_DOUBLE, 5l), 0d);

        assertEquals(1, min(1, new BigDecimal("2.5")));

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
        assertEquals(new Double("-1.34758724E20"),
                min(Long.MIN_VALUE + 1, new Double("-1.34758724E20"), Long.MAX_VALUE));
        assertEquals(new Double("-1.347587244542345673435434E20"),
                min(Long.MIN_VALUE + 1, new Double("-1.347587244542345673435434E20"), Long.MAX_VALUE));
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

        assertEquals(-123.45d, min(-123.45d, -123l, 0d));
        assertEquals(-123.45d, min(-123l, -123.45d, 0d));
        assertEquals(123l, min(123.45d, 123l, Long.MAX_VALUE));
        assertEquals(123l, min(123l, 123.45d, Long.MAX_VALUE));
        assertEquals(-0.2d, min(0l, -0.2d, Long.MAX_VALUE));
        assertEquals(-1.2d, min(-1l, -1.2d, Long.MAX_VALUE));
        assertEquals(-0.2d, min(-0.2d, 0l, Long.MAX_VALUE));
        assertEquals(-1.2d, min(-1.2d, -1l, Long.MAX_VALUE));

        assertEquals(QueryConstants.NULL_LONG, min(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE));
        assertEquals((double) -123.45f, min(-123.45f, -123), 0d);
        assertEquals((double) -123.45f, min(-123, -123.45f), 0d);

        // (double, long)
        assertEquals(9223372036753776384.0d, min(9223372036753776384.0d, 9223372036853776385l), 0d);
        assertEquals(123l, min(123.45, 123l), 0d);
        assertEquals(-123.45, min(-123.45, -123l), 0d);
        assertEquals(5l, min(io.deephaven.util.QueryConstants.NULL_DOUBLE, 5l), 0d);
        assertEquals(5l, min(Double.NaN, 5l), 0d);
        assertEquals(5d, min(5d, io.deephaven.util.QueryConstants.NULL_LONG), 0d);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                min(io.deephaven.util.QueryConstants.NULL_DOUBLE, io.deephaven.util.QueryConstants.NULL_LONG), 0d);
        assertEquals(NULL_DOUBLE, min(Double.NaN, io.deephaven.util.QueryConstants.NULL_LONG), 0d);

        // (long, double)
        assertEquals(9223372036753776384.0d, min(9223372036853776385l, 9223372036753776384.0d), 0d);
        assertEquals(9223372036853776385l, min(9223372036853876385l, 9223372036853776384.0d), 0d);
        assertEquals(123l, min(123l, 123.45), 0d);
        assertEquals(-123.45, min(-123l, -123.45), 0d);
        assertEquals(5l, min(5l, io.deephaven.util.QueryConstants.NULL_DOUBLE), 0d);
        assertEquals(5l, min(5l, Double.NaN), 0d);
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
        assertEquals(5l, max(QueryConstants.NULL_INT, 5l));

        assertEquals(3l, max(1l, 2l, 3l));
        assertEquals(3l, max(new long[] {1, 2, 3}));
        assertEquals(3l, max(1l, 2l, 3l));
        assertEquals(QueryConstants.NULL_DOUBLE, max(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(5, max(QueryConstants.NULL_LONG, 5));

        assertEquals(3f, max(1f, 2f, 3f), 0d);
        assertEquals(3f, max(new float[] {1, 2, 3}), 0d);
        assertEquals(3f, max(1f, 2f, 3f), 0d);
        assertEquals(QueryConstants.NULL_DOUBLE, max(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT), 0d);
        assertEquals(5l, max(QueryConstants.NULL_FLOAT, 5l), 0d);

        assertEquals(3d, max(1d, 2d, 3d), 0d);
        assertEquals(3d, max(new double[] {1d, 2d, 3d}), 0d);
        assertEquals(3d, max(1d, new Double(2), 3d), 0d);
        assertEquals(QueryConstants.NULL_DOUBLE, max(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT), 0d);
        assertEquals(5d, max(QueryConstants.NULL_DOUBLE, 5d), 0d);

        assertTrue(new BigInteger("3").equals(max(new BigInteger("1"), new BigInteger("2"), new BigInteger("3"))));

        assertEquals((short) 10, max(new BigInteger("2"), 1, 2.0, 5.6f, 5l, (byte) 3, (short) 10));

        assertEquals("B", max("A", "B"));

        assertEquals(2, max((byte) 1, (byte) 2));
        assertEquals(2, max((byte) 1, (short) 2));
        assertEquals(2, max((byte) 1, 2));
        assertEquals(2, max((byte) 1, 2l));
        assertEquals(2, max((byte) 1, 2f), 0);
        assertEquals(2, max((byte) 1, 2d), 0);

        assertEquals((short) 2, max((short) 1, (byte) 2));
        assertEquals((short) 2, max((short) 1, (short) 2));
        assertEquals((short) 2, max((short) 1, 2));
        assertEquals((short) 2, max((short) 1, 2l));
        assertEquals((short) 2, max((short) 1, 2f), 0);
        assertEquals((short) 2, max((short) 1, 2d), 0);

        assertEquals(2, max(2, (byte) 2));
        assertEquals(2, max(2, (short) 2));
        assertEquals(2, max(2, 2));
        assertEquals(2, max(2, 2l));
        assertEquals(2, max(2, 2f), 0);
        assertEquals(2, max(2, 2d), 0);

        assertEquals(2l, max(1l, (byte) 2));
        assertEquals(2l, max(1l, (short) 2));
        assertEquals(2l, max(1l, 2));
        assertEquals(2l, max(1l, 2l));
        assertEquals(2l, max(1l, 2f), 0);
        assertEquals(2l, max(1l, 2d), 0);

        assertEquals(2, max(1f, (byte) 2), 0);
        assertEquals(2, max(1f, (short) 2), 0);
        assertEquals(2, max(1f, 2), 0);
        assertEquals(2, max(1f, 2l), 0);
        assertEquals(2, max(1f, 2f), 0);
        assertEquals(2, max(1f, 2d), 0);

        assertEquals(2, max(1d, (byte) 2), 0);
        assertEquals(2, max(1d, (short) 2), 0);
        assertEquals(2, max(1d, 2), 0);
        assertEquals(2, max(1d, 2l), 0);
        assertEquals(2, max(1d, 2f), 0);
        assertEquals(2, max(1d, 2d), 0);

        assertEquals(new BigDecimal("2.5"), max(1, new BigDecimal("2.5")));
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
        assertEquals(1234567891l, max(1.23456789E9, 1234567891L, 0d));
        // Both values are near to min values
        assertEquals(Double.MAX_VALUE, max(Long.MAX_VALUE, Double.MAX_VALUE, 0d));
        assertEquals(new Double("1.34758724E20"), max(Long.MAX_VALUE, new Double("1.34758724E20"), Double.NaN));
        assertEquals(new Double("1.347587244542345673435434E20"),
                max(Long.MAX_VALUE, new Double("1.347587244542345673435434E20"), 0d));
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

        assertEquals(-123l, max(-123.45d, -123l, Long.MIN_VALUE));
        assertEquals(-123l, max(-123l, -123.45d, Long.MIN_VALUE));
        assertEquals(123.45d, max(123.45d, 123l, Long.MIN_VALUE));
        assertEquals(123.45d, max(123l, 123.45d, Long.MIN_VALUE));

        assertEquals((double) 123.45f, max(123.45f, 123), 0d);

        // (double, long)
        assertEquals(9223372036853776384.0d, max(9223372036853776384.0d, 9223372036753776385l), 0d);
        assertEquals(9223372036853876385.0d, max(9223372036853876385.0d, 9223372036853776385l), 0d);
        assertEquals(123.45, max(123.45, 123l), 0d);
        assertEquals(-123, max(-123.45, -123l), 0d);
        assertEquals(5l, max(io.deephaven.util.QueryConstants.NULL_DOUBLE, 5l), 0d);
        assertEquals(5d, max(5d, io.deephaven.util.QueryConstants.NULL_LONG), 0d);
        assertEquals(io.deephaven.util.QueryConstants.NULL_DOUBLE,
                max(io.deephaven.util.QueryConstants.NULL_DOUBLE, io.deephaven.util.QueryConstants.NULL_LONG), 0d);

        // (long, double)
        assertEquals(9223372036853776384.0d, max(9223372036753776385l, 9223372036853776384.0d), 0d);
        assertEquals(9223372036853876385.0d, max(9223372036853776385l, 9223372036853876385.0d), 0d);
        assertEquals(123.45, max(123l, 123.45), 0d);
        assertEquals(-123, max(-123l, -123.45), 0d);
        assertEquals(5l, max(5l, io.deephaven.util.QueryConstants.NULL_DOUBLE), 0d);
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
        assertArrayEquals(expectedSortedPrimLongs, sort(1l, 2l, 3l));
        assertArrayEquals(expectedSortedPrimLongs, sort(new long[] {1, 2, 3}));
        assertArrayEquals(expectedSortedPrimLongs, sort(1l, 2l, 3l));

        final float[] expectedSortedPrimFloats = new float[] {1f, 2f, 3f};
        assertArrayEquals(expectedSortedPrimFloats, sort(1f, 2f, 3f), 0);
        assertArrayEquals(expectedSortedPrimFloats, sort(new float[] {1, 2, 3}), 0);
        assertArrayEquals(expectedSortedPrimFloats, sort(1f, 2f, 3f), 0);

        final double[] expectedSortedPrimDoubles = new double[] {1d, 2d, 3d};
        assertArrayEquals(expectedSortedPrimDoubles, sort(1d, 2d, 3d), 0);
        assertArrayEquals(expectedSortedPrimDoubles, sort(new double[] {1d, 2d, 3d}), 0);
        assertArrayEquals(expectedSortedPrimDoubles, sort(1d, new Double(2), 3d), 0);

        assertArrayEquals(new BigInteger[] {new BigInteger("1"), new BigInteger("2"), new BigInteger("3")},
                sort(new BigInteger("1"), new BigInteger("2"), new BigInteger("3")));

        assertArrayEquals(new Number[] {1, 2d}, sort(2d, 1));

        assertArrayEquals(new String[] {"A", "B"}, sort("B", "A"));
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


        final long[] expectedSortedPrimLongs = new long[] {3l, 2l, 1l};
        assertArrayEquals(expectedSortedPrimLongs, sortDescending(1l, 2l, 3l));
        assertArrayEquals(expectedSortedPrimLongs, sortDescending(new long[] {1, 2, 3}));
        assertArrayEquals(expectedSortedPrimLongs, sortDescending(1l, 2l, 3l));

        final float[] expectedSortedPrimFloats = new float[] {3f, 2f, 1f};
        assertArrayEquals(expectedSortedPrimFloats, sortDescending(1f, 2f, 3f), 0);
        assertArrayEquals(expectedSortedPrimFloats, sortDescending(new float[] {1, 2, 3}), 0);
        assertArrayEquals(expectedSortedPrimFloats, sortDescending(1f, 2f, 3f), 0);

        final double[] expectedSortedPrimDoubles = new double[] {3d, 2d, 1d};
        assertArrayEquals(expectedSortedPrimDoubles, sortDescending(1d, 2d, 3d), 0);
        assertArrayEquals(expectedSortedPrimDoubles, sortDescending(new double[] {1d, 2d, 3d}), 0);
        assertArrayEquals(expectedSortedPrimDoubles, sortDescending(1d, new Double(2), 3d), 0);

        assertArrayEquals(new BigInteger[] {new BigInteger("3"), new BigInteger("2"), new BigInteger("1")},
                sortDescending(new BigInteger("1"), new BigInteger("2"), new BigInteger("3")));

        assertArrayEquals(new Number[] {2d, 1}, sortDescending(2d, 1));

        assertArrayEquals(new String[] {"B", "A"}, sortDescending("A", "B"));
    }

    @Test
    public void testMinMaxException() {
        // (double, long)
        try {
            min(9223372036853776384.25E2, 9223372036853776385l);
            fail("Shouldn't come here for min(9223372036853776384.25E2, 9223372036853776385l)");
        } catch (final Casting.LosingPrecisionWhileCastingException uoe) {
            assertTrue(uoe.getMessage().contains("Not supported: min("));
        }

        try {
            max(9223372036753776384.25, 9223372036853776385l);
            fail("Shouldn't come here for max(9223372036853776384.25E2, 9223372036853776385l)");
        } catch (final Casting.LosingPrecisionWhileCastingException uoe) {
            assertTrue(uoe.getMessage().contains("Not supported: max("));
        }

        // (long, double)
        try {
            min(9223372036853776385l, 9223372036853776384.25E2);
            fail("Shouldn't come here for min(9223372036853776385l, 9223372036853776384.25E2)");
        } catch (final UnsupportedOperationException uoe) {
            assertTrue(uoe.getMessage().contains("Not supported: min("));
        }

        try {
            max(9223372036853776385l, 9223372036753776384.25);
            fail("Shouldn't come here for max(9223372036853776385l, 9223372036753776384.25)");
        } catch (final UnsupportedOperationException uoe) {
            assertTrue(uoe.getMessage().contains("Not supported: max("));
        }
    }
}
