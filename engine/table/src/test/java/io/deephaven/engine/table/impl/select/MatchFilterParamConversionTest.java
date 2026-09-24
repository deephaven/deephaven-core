//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The conversion of a numeric query-scope parameter or direct match value to the column's type: a cast that must not
 * lose anything, as {@link MatchFilterCoercionTest} checks against the {@link ConditionFilter} failover.
 */
public class MatchFilterParamConversionTest {

    private static Object convert(final Object value, final Class<?> columnType) {
        return MatchFilter.ColumnTypeConvertorFactory.getConvertor(columnType).convertParamValue(value);
    }

    private static void assertRejected(final Object value, final Class<?> columnType) {
        assertThrows(RuntimeException.class, () -> convert(value, columnType));
    }

    @Test
    public void exactNarrowingIsAccepted() {
        assertEquals((byte) 44, convert(44, byte.class));
        assertEquals((short) -7, convert(-7L, short.class));
        assertEquals(5, convert(5L, int.class));
        assertEquals(5, convert(5.0, int.class));
        assertEquals(0, convert(-0.0, int.class));
        assertEquals(5L, convert(5.0f, long.class));
        assertEquals(0.5f, convert(0.5, float.class));
        assertEquals('A', convert(65, char.class));
    }

    @Test
    public void exactWideningIsAccepted() {
        assertEquals(5L, convert(5, long.class));
        assertEquals(5.0, convert(5, double.class));
        assertEquals((double) 5.7f, convert(5.7f, double.class));
        assertEquals(16777216f, convert(16777216, float.class));
        assertEquals(9007199254740992.0, convert(9007199254740992L, double.class));
    }

    @Test
    public void inexactWideningIsRejected() {
        // the query language compares a float or double column with a long exactly, so the rounded value would not
        // select the same rows
        assertRejected(16777217, float.class);
        assertRejected(9007199254740993L, double.class);
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
        assertRejected(65536 + 65, char.class);
        assertRejected(1e19, long.class);
        // saturates to Long.MAX_VALUE, which (double) Long.MAX_VALUE rounds back to
        assertRejected(0x1p63, long.class);
    }

    @Test
    public void inexactFloatNarrowingIsRejected() {
        assertRejected(5.7, float.class);
        assertRejected(1e300, float.class);
    }

    @Test
    public void nonFiniteValues() {
        assertTrue(Float.isNaN((Float) convert(Double.NaN, float.class)));
        assertTrue(Double.isNaN((Double) convert(Float.NaN, double.class)));
        assertEquals(Float.POSITIVE_INFINITY, convert(Double.POSITIVE_INFINITY, float.class));
        assertRejected(Double.NaN, int.class);
        assertRejected(Float.POSITIVE_INFINITY, long.class);
    }

    @Test
    public void nullSentinelsBecomeTheColumnTypesNull() {
        assertEquals(QueryConstants.NULL_INT_BOXED, convert(QueryConstants.NULL_LONG_BOXED, int.class));
        assertEquals(QueryConstants.NULL_DOUBLE_BOXED, convert(QueryConstants.NULL_FLOAT_BOXED, double.class));
        assertEquals(QueryConstants.NULL_BYTE_BOXED, convert(QueryConstants.NULL_DOUBLE_BOXED, byte.class));
        assertEquals(QueryConstants.NULL_CHAR_BOXED, convert(QueryConstants.NULL_INT_BOXED, char.class));
    }

    @Test
    public void valuesThatConvertExactlyToTheNullValueAreNull() {
        // (int) (long) Integer.MIN_VALUE is NULL_INT, as the int Integer.MIN_VALUE is
        assertEquals(QueryConstants.NULL_INT_BOXED, convert((long) Integer.MIN_VALUE, int.class));
        assertEquals(QueryConstants.NULL_SHORT_BOXED, convert((int) Short.MIN_VALUE, short.class));
        assertEquals(QueryConstants.NULL_FLOAT_BOXED, convert((double) -Float.MAX_VALUE, float.class));
        assertEquals(QueryConstants.NULL_CHAR_BOXED, convert((int) Character.MAX_VALUE, char.class));
        // a value that only wraps to the null value is still rejected
        assertRejected(1L << 31, int.class);
    }

    @Test
    public void nonNumbersAreRejected() {
        assertRejected("5", int.class);
        assertRejected(Boolean.TRUE, double.class);
        // as on main; the filter fails over, and the query language promotes the char to int
        assertRejected('A', int.class);
        assertRejected('A', BigInteger.class);
    }

    @Test
    public void bigValuesToPrimitives() {
        assertEquals(5, convert(BigInteger.valueOf(5), int.class));
        assertEquals(5, convert(new BigDecimal("5.000"), int.class));
        assertRejected(new BigDecimal("5.7"), int.class);
        assertRejected(BigInteger.ONE.shiftLeft(64), long.class);
        assertRejected(BigInteger.valueOf(300), byte.class);
        // the query language compares these with a float or double column as decimal strings, not exactly
        assertRejected(new BigDecimal("0.5"), double.class);
        assertRejected(BigInteger.valueOf(5), float.class);
    }

    @Test
    public void toBigInteger() {
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE), convert(Long.MAX_VALUE, BigInteger.class));
        assertEquals(BigInteger.valueOf(5), convert(5.0, BigInteger.class));
        assertEquals(BigInteger.valueOf(5), convert(new BigDecimal("5.00"), BigInteger.class));
        // as BigDecimal.valueOf(1e30) is, which is how the query language compares 1e30 with a BigInteger
        assertEquals(BigInteger.TEN.pow(30), convert(1e30, BigInteger.class));
        assertNull(convert(QueryConstants.NULL_INT_BOXED, BigInteger.class));
        assertRejected(5.7, BigInteger.class);
        assertRejected(new BigDecimal("5.7"), BigInteger.class);
        assertRejected(Double.NaN, BigInteger.class);
        assertRejected("5", BigInteger.class);
    }

    @Test
    public void toBigDecimal() {
        // integral values convert exactly, even beyond double precision
        assertEquals(BigDecimal.valueOf(9007199254740993L), convert(9007199254740993L, BigDecimal.class));
        assertEquals(new BigDecimal("5.7"), convert(5.7, BigDecimal.class));
        assertEquals(new BigDecimal("0.10000000149011612"), convert(0.1f, BigDecimal.class));
        assertEquals(new BigDecimal(BigInteger.TEN), convert(BigInteger.TEN, BigDecimal.class));
        assertNull(convert(QueryConstants.NULL_LONG_BOXED, BigDecimal.class));
        assertRejected(Double.NaN, BigDecimal.class);
        assertRejected("5", BigDecimal.class);
    }
}
