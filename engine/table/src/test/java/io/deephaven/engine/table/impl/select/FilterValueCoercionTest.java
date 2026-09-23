//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FilterValueCoercionTest {

    private static void assertRejected(final Object value, final Class<?> columnType) {
        assertThrows(IllegalArgumentException.class, () -> FilterValueCoercion.toPrimitive(value, columnType));
    }

    @Test
    public void exactNarrowingIsAccepted() {
        assertEquals((byte) 44, FilterValueCoercion.toPrimitive(44, byte.class));
        assertEquals((short) -7, FilterValueCoercion.toPrimitive(-7L, short.class));
        assertEquals(5, FilterValueCoercion.toPrimitive(5L, int.class));
        assertEquals(5, FilterValueCoercion.toPrimitive(5.0, int.class));
        assertEquals(0, FilterValueCoercion.toPrimitive(-0.0, int.class));
        assertEquals(5L, FilterValueCoercion.toPrimitive(5.0f, long.class));
        assertEquals(0.5f, FilterValueCoercion.toPrimitive(0.5, float.class));
        assertEquals('A', FilterValueCoercion.toPrimitive(65, char.class));
        assertEquals(65, FilterValueCoercion.toPrimitive('A', int.class));
    }

    @Test
    public void wideningIsAcceptedAsJavaWouldCompare() {
        assertEquals(5L, FilterValueCoercion.toPrimitive(5, long.class));
        assertEquals(5.0, FilterValueCoercion.toPrimitive(5, double.class));
        assertEquals((double) 5.7f, FilterValueCoercion.toPrimitive(5.7f, double.class));
        // int to float rounds, and so does Java's comparison of an int with a float
        assertEquals(16777216f, FilterValueCoercion.toPrimitive(16777217, float.class));
        // long to double rounds, and so does Java's comparison of a long with a double
        assertEquals(9007199254740992.0, FilterValueCoercion.toPrimitive(9007199254740993L, double.class));
    }

    @Test
    public void fractionsAreRejectedForIntegralColumns() {
        assertRejected(5.7, int.class);
        assertRejected(5.7f, long.class);
        assertRejected(0.5, char.class);
    }

    @Test
    public void outOfRangeValuesAreRejected() {
        assertRejected(300, byte.class);
        assertRejected(5_000_000_000L, int.class);
        assertRejected(-1, char.class);
        assertRejected(1e19, long.class);
        assertRejected(0x1p63, long.class);
    }

    @Test
    public void inexactFloatNarrowingIsRejected() {
        assertRejected(5.7, float.class);
        assertRejected(1e300, float.class);
    }

    @Test
    public void nonFiniteValues() {
        assertTrue(Float.isNaN((Float) FilterValueCoercion.toPrimitive(Double.NaN, float.class)));
        assertEquals(Float.POSITIVE_INFINITY, FilterValueCoercion.toPrimitive(Double.POSITIVE_INFINITY, float.class));
        assertRejected(Double.NaN, int.class);
        assertRejected(Float.POSITIVE_INFINITY, long.class);
    }

    @Test
    public void ambiguousFloatingPointValuesAreRejected() {
        // 2^53 + 1 rounds to 2^53 as a double, so two longs compare equal to 2^53
        assertRejected(0x1p53, long.class);
        // 2^24 + 1 rounds to 2^24 as a float, so two ints compare equal to 2^24
        assertRejected(0x1p24f, int.class);
        // below the precision limit, the equivalent is unique
        assertEquals(1L << 52, FilterValueCoercion.toPrimitive(0x1p52, long.class));
        assertEquals(1 << 23, FilterValueCoercion.toPrimitive(0x1p23f, int.class));
        // every int is exactly a double, so no int is ambiguous
        assertEquals(Integer.MAX_VALUE, FilterValueCoercion.toPrimitive((double) Integer.MAX_VALUE, int.class));
    }

    @Test
    public void nullSentinelsBecomeTheColumnTypesNull() {
        assertEquals(QueryConstants.NULL_INT_BOXED,
                FilterValueCoercion.toPrimitive(QueryConstants.NULL_LONG_BOXED, int.class));
        assertEquals(QueryConstants.NULL_DOUBLE_BOXED,
                FilterValueCoercion.toPrimitive(QueryConstants.NULL_FLOAT_BOXED, double.class));
        assertEquals(QueryConstants.NULL_BYTE_BOXED,
                FilterValueCoercion.toPrimitive(QueryConstants.NULL_DOUBLE_BOXED, byte.class));
        assertEquals(QueryConstants.NULL_CHAR_BOXED,
                FilterValueCoercion.toPrimitive(QueryConstants.NULL_INT_BOXED, char.class));
    }

    @Test
    public void valuesThatWouldAliasNullAreRejected() {
        // Integer.MIN_VALUE is an ordinary long, but it is NULL_INT
        assertRejected((long) Integer.MIN_VALUE, int.class);
        assertRejected((int) Short.MIN_VALUE, short.class);
        assertRejected((double) -Float.MAX_VALUE, float.class);
        assertRejected((int) Character.MAX_VALUE, char.class);
    }

    @Test
    public void nonNumbersAreRejected() {
        assertRejected("5", int.class);
        assertRejected(Boolean.TRUE, double.class);
    }

    @Test
    public void bigValuesToPrimitives() {
        assertEquals(5, FilterValueCoercion.toPrimitive(BigInteger.valueOf(5), int.class));
        assertEquals(5, FilterValueCoercion.toPrimitive(new BigDecimal("5.000"), int.class));
        assertEquals(0.5, FilterValueCoercion.toPrimitive(new BigDecimal("0.5"), double.class));
        assertRejected(new BigDecimal("5.7"), int.class);
        assertRejected(new BigDecimal("5.7"), double.class);
        assertRejected(BigInteger.ONE.shiftLeft(64), long.class);
        assertRejected(BigInteger.valueOf(300), byte.class);
    }

    @Test
    public void toBigInteger() {
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE), FilterValueCoercion.toBigInteger(Long.MAX_VALUE));
        assertEquals(BigInteger.valueOf(5), FilterValueCoercion.toBigInteger(5.0));
        assertEquals(BigInteger.valueOf(5), FilterValueCoercion.toBigInteger(new BigDecimal("5.00")));
        assertEquals(null, FilterValueCoercion.toBigInteger(QueryConstants.NULL_INT_BOXED));
        assertThrows(IllegalArgumentException.class, () -> FilterValueCoercion.toBigInteger(5.7));
        assertThrows(IllegalArgumentException.class, () -> FilterValueCoercion.toBigInteger(new BigDecimal("5.7")));
        assertThrows(IllegalArgumentException.class, () -> FilterValueCoercion.toBigInteger(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> FilterValueCoercion.toBigInteger("5"));
    }

    @Test
    public void toBigDecimal() {
        // integral values convert exactly, even beyond double precision
        assertEquals(BigDecimal.valueOf(9007199254740993L), FilterValueCoercion.toBigDecimal(9007199254740993L));
        assertEquals(new BigDecimal("5.7"), FilterValueCoercion.toBigDecimal(5.7));
        assertEquals(new BigDecimal(BigInteger.TEN), FilterValueCoercion.toBigDecimal(BigInteger.TEN));
        assertEquals(null, FilterValueCoercion.toBigDecimal(QueryConstants.NULL_LONG_BOXED));
        assertThrows(IllegalArgumentException.class, () -> FilterValueCoercion.toBigDecimal(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> FilterValueCoercion.toBigDecimal("5"));
    }
}
