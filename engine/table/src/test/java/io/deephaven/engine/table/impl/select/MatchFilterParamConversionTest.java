//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.util.QueryConstants;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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

    /** A value the convertor leaves as it is. */
    private static void assertNotConverted(final Object value, final Class<?> columnType) {
        assertSame(value, convert(value, columnType));
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
        // as is the column type's own null value
        assertEquals(QueryConstants.NULL_BYTE_BOXED, convert(QueryConstants.NULL_BYTE_BOXED, byte.class));
    }

    @Test
    public void valuesThatConvertExactlyToTheNullValueAreRejected() {
        // (int) (long) Integer.MIN_VALUE is NULL_INT, but the query language compares the long as a number below every
        // int; only the int Integer.MIN_VALUE is null
        assertRejected((long) Integer.MIN_VALUE, int.class);
        assertRejected((int) Short.MIN_VALUE, short.class);
        assertRejected(-128, byte.class);
        assertRejected((double) -Float.MAX_VALUE, float.class);
        assertRejected(-0x1p63, long.class);
        // 65535 converts to NULL_CHAR, the highest char, where the null values of the other types are their lowest
        assertRejected((int) Character.MAX_VALUE, char.class);
        // a value that only wraps to the null value is rejected too
        assertRejected(1L << 31, int.class);
    }

    /**
     * A value that is not a number cannot convert to a primitive column's type, so the filter fails over, as on main. A
     * char does not convert to a BigDecimal or BigInteger column either, so the filter fails over to the query
     * language, which compares the two by code point. Any other value can never equal a BigInteger, so it is left as it
     * is.
     */
    @Test
    public void nonNumbersAreNotConverted() {
        assertRejected("5", int.class);
        assertRejected(Boolean.TRUE, double.class);
        assertRejected('A', int.class);
        assertRejected("A", char.class);
        assertRejected('A', BigInteger.class);
        assertRejected('A', BigDecimal.class);
        assertNotConverted("A", BigInteger.class);
    }

    /** A Number of another type has no exact value to compare: longValue() would drop the fraction of 5.7. */
    @Test
    public void otherNumberTypesAreRejected() {
        final DoubleAdder fraction = new DoubleAdder();
        fraction.add(5.7);
        for (final Class<?> columnType : new Class<?>[] {int.class, long.class, double.class, BigInteger.class,
                BigDecimal.class}) {
            assertRejected(fraction, columnType);
            assertRejected(new AtomicLong(5), columnType);
        }
    }

    @Test
    public void bigValuesToPrimitives() {
        assertEquals(5, convert(BigInteger.valueOf(5), int.class));
        assertEquals(5, convert(new BigDecimal("5.000"), int.class));
        assertRejected(new BigDecimal("5.7"), int.class);
        assertRejected(BigInteger.ONE.shiftLeft(64), long.class);
        assertRejected(BigInteger.valueOf(300), byte.class);
        // the query language compares these with a float or double column through BigDecimal.valueOf, the column
        // value's shortest decimal, so a value converts when it is the shortest decimal of the converted value
        assertEquals(0.5, convert(new BigDecimal("0.5"), double.class));
        assertEquals(0.1, convert(new BigDecimal("0.1"), double.class));
        assertEquals(5.0f, convert(BigInteger.valueOf(5), float.class));
        // exactly 0.1 as a double, but no double's shortest decimal
        assertRejected(new BigDecimal(0.1), double.class);
        // 0.1f widens to the double 0.10000000149011612, the decimal the query language compares
        assertRejected(new BigDecimal("0.1"), float.class);
        assertEquals(0.10000000149011612f, convert(new BigDecimal("0.10000000149011612"), float.class));
        // exactly a double, but BigDecimal.valueOf(0x1p60) is 1152921504606846980
        assertRejected(BigInteger.ONE.shiftLeft(60), double.class);
        assertRejected(new BigDecimal("1e400"), double.class);
        assertRejected(new BigDecimal("1e39"), float.class);
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
        assertNotConverted("5", BigInteger.class);
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
        assertNotConverted("5", BigDecimal.class);
    }
}
