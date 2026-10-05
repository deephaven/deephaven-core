//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.literal.Literal;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.byteCol;
import static io.deephaven.engine.util.TableTools.charCol;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.shortCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_CHAR_BOXED;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE_BOXED;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_INT_BOXED;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Filter values -- query-scope parameters and directly supplied match values -- must select exactly the rows that the
 * {@link ConditionFilter} failover, which evaluates the filter in the query language, would select. A value that cannot
 * be converted to the column's type exactly is never truncated or wrapped: filters that have a failover use it, and the
 * rest reject the value. A value that converts exactly to the column type's null value, an int -128 for a byte column
 * for instance, is not representable either: the query language compares it as a number, and only a value of the
 * column's own type, the byte -128, is null. There are two exceptions. A large floating-point value against an int or
 * long column matches only its exact equivalent, where the query language, comparing in floating point, would also
 * match the integers that round to it. And a literal is read in the column's type, so the literal -128 against a byte
 * column is null, where the query language reads it as an int.
 */
public class MatchFilterCoercionTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static void assertRejected(final Runnable filterOperation) {
        final RuntimeException err = assertThrows(RuntimeException.class, filterOperation::run);
        Throwable cause = err;
        while (cause != null) {
            if (cause instanceof IllegalArgumentException && cause.getMessage() != null
                    && (cause.getMessage().startsWith("Cannot convert value")
                            || cause.getMessage().contains("cannot be matched against column"))) {
                return;
            }
            cause = cause.getCause();
        }
        throw new AssertionError("expected a value conversion error", err);
    }

    /**
     * Asserts that each filter selects the same rows as the {@link ConditionFilter} it would fail over to, whether or
     * not it actually fails over.
     */
    private static void assertSameRowsAsFailover(final Table table, final String... filters) {
        final List<String> mismatches = new ArrayList<>();
        for (final String filter : filters) {
            final Table normal = table.where(filter);
            final Table failover = table.where(ConditionFilter.createConditionFilter(filter));
            if (!normal.getRowSet().subsetOf(failover.getRowSet())
                    || !failover.getRowSet().subsetOf(normal.getRowSet())) {
                mismatches.add(String.format("%s selected %s; the failover selects %s",
                        filter, columnValues(normal), columnValues(failover)));
            }
        }
        assertTrue(String.join("\n", mismatches), mismatches.isEmpty());
    }

    private static String columnValues(final Table table) {
        final List<String> values = new ArrayList<>();
        table.columnIterator(table.getDefinition().getColumnNames().get(0))
                .forEachRemaining(value -> values.add(String.valueOf(value)));
        return values.toString();
    }

    @Test
    public void byteAndShortColumnsSelectWhatTheFailoverSelects() {
        QueryScope.addParam("v300", 300);
        QueryScope.addParam("v40000", 40000);

        assertSameRowsAsFailover(newTable(byteCol("X", (byte) 0, (byte) 44, (byte) 127, NULL_BYTE)),
                "X == v300", "X != v300", "X < v300");
        assertSameRowsAsFailover(newTable(shortCol("X", (short) -25536, (short) 0, NULL_SHORT)),
                "X == v40000", "X < v40000");
    }

    @Test
    public void intColumnSelectsWhatTheFailoverSelects() {
        QueryScope.addParam("v25", 2.5);
        QueryScope.addParam("v5", 5.0);
        QueryScope.addParam("vnan", Double.NaN);
        QueryScope.addParam("v1e10", 1e10);
        QueryScope.addParam("vinf", Double.POSITIVE_INFINITY);
        QueryScope.addParam("vneginf", Double.NEGATIVE_INFINITY);
        QueryScope.addParam("lwrap", (1L << 32) + 5);
        QueryScope.addParam("lnull", 1L << 31); // wraps to Integer.MIN_VALUE, which is NULL_INT
        QueryScope.addParam("cA", 'A');
        QueryScope.addParam("nullD", NULL_DOUBLE_BOXED);
        final Table t = newTable(intCol("X", 0, 2, 3, 5, 65, 1 << 24, (1 << 24) + 1, Integer.MAX_VALUE, NULL_INT));

        assertSameRowsAsFailover(t,
                "X == v25", "X < v25", "X >= v25", "X == v5", "X <= v5",
                "X == vnan", "X != vnan", "X < vnan",
                "X == v1e10", "X < v1e10", "X < vinf", "X > vneginf",
                "X == lwrap", "X == lnull", "X == cA", "X < cA",
                "X == nullD", "X < nullD");
    }

    @Test
    public void longColumnSelectsWhatTheFailoverSelects() {
        QueryScope.addParam("d53", 0x1p53);
        QueryScope.addParam("d63", 0x1p63); // saturates to Long.MAX_VALUE
        QueryScope.addParam("v25", 2.5);
        final Table t = newTable(longCol("X", 2L, 1L << 53, (1L << 53) + 1, Long.MAX_VALUE, NULL_LONG));

        assertSameRowsAsFailover(t,
                // the query language compares a long with a double exactly, so ranges agree
                "X <= d53", "X > d53", "X == d63", "X < d63", "X == v25", "X > v25");
    }

    @Test
    public void floatingPointColumnsSelectWhatTheFailoverSelects() {
        QueryScope.addParam("v01", 0.1);
        QueryScope.addParam("vnan", Double.NaN);
        QueryScope.addParam("l24", (1L << 24) + 1); // rounds to 2^24 as a float
        QueryScope.addParam("l53", (1L << 53) + 1); // rounds to 2^53 as a double
        QueryScope.addParam("i5", 5);

        assertSameRowsAsFailover(newTable(floatCol("X", 0.1f, 0.2f, 0x1p24f, 0x1p24f + 2, NULL_FLOAT)),
                "X == v01", "X != v01", "X <= v01", "X == vnan",
                "X == l24", "X < l24", "X >= l24");
        assertSameRowsAsFailover(newTable(doubleCol("X", 0.5, 5.0, 0x1p53, 0x1p53 + 2, NULL_DOUBLE)),
                "X == l53", "X < l53", "X >= l53", "X == i5", "X < i5");
    }

    @Test
    public void valueThatConvertsToTheNullValueIsANumber() {
        // an int -128 converts exactly to the byte -128, which is NULL_BYTE, but the query language compares it with
        // a byte as an int: a number below every byte, which X == -128 matches no row of, and X < -128 only the nulls
        QueryScope.addParam("vm128", -128);
        QueryScope.addParam("lmin", (long) Integer.MIN_VALUE);
        QueryScope.addParam("dmin", -0x1p31);
        QueryScope.addParam("vnegmax", -(double) Float.MAX_VALUE);
        final String[] filters = {"X == vm128", "X != vm128", "X < vm128", "X <= vm128", "X > vm128", "X >= vm128"};
        final Table bytes = newTable(byteCol("X", (byte) -127, (byte) 0, (byte) 5, NULL_BYTE));
        assertSameRowsAsFailover(bytes, filters);
        assertRejected(() -> bytes.where("X in vm128"));
        assertRejected(() -> bytes.where(new MatchFilter(MatchOptions.REGULAR, "X", -128)));

        assertSameRowsAsFailover(newTable(intCol("X", -Integer.MAX_VALUE, 0, NULL_INT)),
                "X == lmin", "X != lmin", "X < lmin", "X >= lmin", "X == dmin", "X < dmin");
        assertSameRowsAsFailover(newTable(floatCol("X", Float.NEGATIVE_INFINITY, 0.5f, NULL_FLOAT)),
                "X == vnegmax", "X != vnegmax", "X <= vnegmax", "X > vnegmax");
    }

    @Test
    public void nullValueOfTheColumnTypeIsNull() {
        // a value of the column's own type at its null value is null, in the query language as here
        QueryScope.addParam("vm128Byte", (byte) -128);
        QueryScope.addParam("nullI", NULL_INT_BOXED);
        final Table bytes = newTable(byteCol("X", (byte) 0, (byte) 5, NULL_BYTE));
        final Table nullByte = bytes.where("isNull(X)");

        assertSameRowsAsFailover(bytes, "X == vm128Byte", "X != vm128Byte", "X < vm128Byte", "X == nullI");
        assertTableEquals(nullByte, bytes.where("X == vm128Byte"));
        assertTableEquals(nullByte, bytes.where("X in vm128Byte"));
        assertTableEquals(nullByte, bytes.where(new MatchFilter(MatchOptions.REGULAR, "X", (byte) -128)));
        assertTableEquals(nullByte, bytes.where("X == nullI"));

    }

    @Test
    public void literalAtTheNullValueIsNull() {
        // A literal is read in the column's type, so -128 against a byte column is NULL_BYTE, as the Byte -128 is. The
        // query language, reading -128 as an int, compares it as a number instead, so these differ from the failover.
        final Table bytes = newTable(byteCol("X", (byte) -127, (byte) 0, NULL_BYTE));
        final Table nullByte = bytes.where("isNull(X)");
        final Table nonNullByte = bytes.where("!isNull(X)");
        assertTableEquals(nullByte, bytes.where("X == -128"));
        assertTableEquals(nullByte, bytes.where("X in -128"));
        assertTableEquals(nonNullByte, bytes.where("X != -128"));
        // null orders below every value, so no row is below it, and every row is at or above it
        assertTableEquals(bytes.head(0), bytes.where("X < -128"));
        assertTableEquals(bytes, bytes.where("X >= -128"));

        final Table shorts = newTable(shortCol("X", (short) 0, NULL_SHORT));
        assertTableEquals(shorts.where("isNull(X)"), shorts.where("X == -32768"));
        final Table ints = newTable(intCol("X", 0, NULL_INT));
        assertTableEquals(ints.where("isNull(X)"), ints.where("X == -2147483648"));
        final Table longs = newTable(longCol("X", 0L, NULL_LONG));
        assertTableEquals(longs.where("isNull(X)"), longs.where("X == -9223372036854775808"));
    }

    @Test
    public void negativeZeroSelectsWhatTheFailoverSelects() {
        // the query language orders -0.0 below 0 when it compares a byte, short, int or char with it
        QueryScope.addParam("z", -0.0);
        QueryScope.addParam("zf", -0.0f);
        final String[] filters = {"X == z", "X < z", "X <= z", "X > z", "X >= z", "X <= zf", "X > zf"};

        assertSameRowsAsFailover(newTable(intCol("X", -1, 0, 1, NULL_INT)), filters);
        assertSameRowsAsFailover(newTable(longCol("X", -1L, 0L, 1L, NULL_LONG)), filters);
        assertSameRowsAsFailover(newTable(byteCol("X", (byte) -1, (byte) 0, (byte) 1)), filters);
        assertSameRowsAsFailover(newTable(shortCol("X", (short) -1, (short) 0, (short) 1)), filters);
        assertSameRowsAsFailover(newTable(charCol("X", '\0', 'A', NULL_CHAR)), filters);
    }

    @Test
    public void charColumnSelectsWhatTheFailoverSelects() {
        QueryScope.addParam("v65", 65);
        QueryScope.addParam("vwrap", 65536 + 65); // wraps to 'A'
        QueryScope.addParam("d65", 65.0);
        // converts to NULL_CHAR, which orders below every char, where the query language orders 65535 above them
        QueryScope.addParam("v65535", 65535);
        final Table t = newTable(charCol("X", 'A', 'B', NULL_CHAR));

        assertSameRowsAsFailover(t, "X == v65", "X > v65", "X == vwrap", "X < vwrap", "X == d65",
                "X == v65535", "X != v65535", "X < v65535", "X <= v65535", "X > v65535", "X >= v65535");
        assertRejected(() -> t.where("X in v65535"));
    }

    @Test
    public void charColumnReadsAnUnquotedIntegerLiteralAsACodePoint() {
        // as the query language does: 5 is (char) 5, not '5'
        final Table t = newTable(charCol("X", (char) 0, (char) 5, '5', 'A', (char) 65534, NULL_CHAR));
        final List<String> filters = new ArrayList<>();
        for (final String literal : new String[] {"0", "5", "65", "65534", "65535", "65536", "-1"}) {
            for (final String operator : new String[] {"==", "!=", "<", "<=", ">", ">="}) {
                filters.add("X " + operator + " " + literal);
            }
        }
        assertSameRowsAsFailover(t, filters.toArray(String[]::new));
        // a code point of a char keeps the typed filter; 65535 (NULL_CHAR) and beyond fail over
        for (final String filter : new String[] {"X == 5", "X != 0", "X < 65", "X >= 65534"}) {
            assertFalse(filter, failsOver(t, filter));
        }
        assertTrue(failsOver(t, "X < 65535"));
        assertTableEquals(t.where("X == 5 || X == 65"), t.where("X in 5, 65"));
        // in has no failover
        assertThrows(RuntimeException.class, () -> t.where("X in 65535"));

        // a quoted digit, or an unquoted letter, is still a char
        final Table five = newTable(charCol("X", '5'));
        assertTableEquals(five, t.where("X == '5'"));
        assertTableEquals(five, t.where("X == \"5\""));
        assertTableEquals(newTable(charCol("X", 'A')), t.where(new MatchFilter(null, MatchOptions.REGULAR, "X",
                new String[] {"A"}, null)));

        // the Filter API quotes a char for the range filter, which would otherwise read '5' as (char) 5
        assertTableEquals(newTable(charCol("X", (char) 0, (char) 5, NULL_CHAR)),
                t.where(FilterComparison.lt(ColumnName.of("X"), Literal.of('5'))));
        assertTableEquals(newTable(charCol("X", (char) 0, NULL_CHAR)),
                t.where(FilterComparison.lt(ColumnName.of("X"), Literal.of(5))));
    }

    private static boolean failsOver(final Table table, final String filter) {
        final WhereFilter whereFilter = WhereFilterFactory.getExpression(filter);
        whereFilter.init(table.getDefinition());
        if (whereFilter instanceof RangeFilter) {
            return ((RangeFilter) whereFilter).getRealFilter() instanceof ConditionFilter;
        }
        return whereFilter instanceof MatchFilter && ((MatchFilter) whereFilter).getFailoverFilter() != null;
    }

    @Test
    public void bigColumnsSelectWhatTheFailoverSelects() {
        // the query language compares a floating-point value with a BigInteger or BigDecimal through
        // BigDecimal.valueOf, the value's shortest decimal representation, not its exact binary value
        QueryScope.addParam("dv", 1e30);
        QueryScope.addParam("d60", 0x1p60);
        QueryScope.addParam("v01", 0.1);
        QueryScope.addParam("f01", 0.1f);
        QueryScope.addParam("l5", 5L);
        // and a char by its code point
        QueryScope.addParam("cA", 'A');
        final BigInteger p60 = BigInteger.ONE.shiftLeft(60);

        assertSameRowsAsFailover(
                newTable(col("X", BigInteger.TEN.pow(30), new BigDecimal(1e30).toBigIntegerExact(), p60,
                        BigDecimal.valueOf(0x1p60).toBigIntegerExact(), BigInteger.valueOf(5), BigInteger.valueOf(65),
                        null)),
                "X == dv", "X >= dv", "X == d60", "X < d60", "X == l5", "X == v01", "X == cA", "X != cA");
        assertSameRowsAsFailover(
                newTable(col("X", new BigDecimal("0.1"), new BigDecimal(0.1), BigDecimal.valueOf(0.1f),
                        new BigDecimal(0.1f), new BigDecimal("5"), new BigDecimal("65.0"), null)),
                "X == v01", "X <= v01", "X == f01", "X == l5", "X == cA", "X != cA");
    }

    @Test
    public void charValuesSelectWhatTheFailoverSelects() {
        // the query language, as Java, compares a char with a number by its code point
        QueryScope.addParam("c5", '5');
        QueryScope.addParam("cE", 'é'); // 233, beyond a byte
        QueryScope.addParam("cHigh", '耀'); // 32768, beyond a short
        QueryScope.addParam("cMax", '￾');
        QueryScope.addParam("cNull", NULL_CHAR_BOXED);
        final String[] filters = {"X == c5", "X != c5", "X < c5", "X >= c5", "X == cE", "X < cE", "X == cHigh",
                "X > cHigh", "X == cMax", "X <= cMax", "X == cNull", "X != cNull"};

        final Table bytes = newTable(byteCol("X", (byte) 0, (byte) 53, (byte) 127, (byte) 233, NULL_BYTE));
        final Table shorts = newTable(shortCol("X", (short) 0, (short) 53, Short.MAX_VALUE, (short) -1, NULL_SHORT));
        final Table ints = newTable(intCol("X", 0, 53, 233, 32768, 65534, NULL_INT));
        final Table longs = newTable(longCol("X", 0L, 53L, 65534L, NULL_LONG));
        final Table floats = newTable(floatCol("X", 0.5f, 53f, 65534f, NULL_FLOAT));
        final Table doubles = newTable(doubleCol("X", 0.5, 53.0, 65534.0, NULL_DOUBLE));
        final Table bigIntegers = newTable(col("X", BigInteger.valueOf(53), BigInteger.valueOf(65534), null));
        final Table bigDecimals = newTable(col("X", new BigDecimal("53"), new BigDecimal("53.0"),
                new BigDecimal("53.5"), null));
        for (final Table t : new Table[] {bytes, shorts, ints, longs, floats, doubles, bigIntegers, bigDecimals}) {
            assertSameRowsAsFailover(t, filters);
            assertFalse(failsOver(t, "X == c5"));
            assertFalse(failsOver(t, "X < c5"));
        }
        assertTrue(failsOver(bytes, "X == cE"));
        assertTrue(failsOver(shorts, "X == cHigh"));
        assertFalse(failsOver(ints, "X == cHigh"));

        assertTableEquals(newTable(intCol("X", 53)), ints.where("X in c5"));
        assertTableEquals(newTable(byteCol("X", (byte) 53)), bytes.where("X in c5"));
        assertTableEquals(newTable(intCol("X", 53)),
                ints.where(FilterComparison.eq(ColumnName.of("X"), Literal.of('5'))));
        assertTableEquals(newTable(col("X", new BigDecimal("53"), new BigDecimal("53.0"))),
                bigDecimals.where(new MatchFilter(MatchOptions.REGULAR, "X", '5')));
        assertRejected(() -> bytes.where("X in cE"));
        assertRejected(() -> shorts.where(new MatchFilter(MatchOptions.REGULAR, "X", '耀')));
    }

    @Test
    public void bigValuesAgainstFloatingPointColumnsSelectWhatTheFailoverSelects() {
        QueryScope.addParam("bi60", BigInteger.ONE.shiftLeft(60));
        QueryScope.addParam("bd01", new BigDecimal("0.1"));
        QueryScope.addParam("bdExact", new BigDecimal(0.1)); // the exact binary value of 0.1
        QueryScope.addParam("bdExactF", new BigDecimal(0.1f)); // the exact binary value of 0.1f
        QueryScope.addParam("bd05", new BigDecimal("0.5"));
        QueryScope.addParam("bd0", BigDecimal.ZERO);
        QueryScope.addParam("bi5", BigInteger.valueOf(5));

        final Table doubles = newTable(doubleCol("X", 0x1p60, 0.1, 0.2, 5.0, -0.0, 0.0, Double.NaN, NULL_DOUBLE));
        final String[] doubleFilters = {"X == bi60", "X == bd01", "X != bd01", "X < bd01", "X >= bd01",
                "X == bdExact", "X < bdExact", "X == bi5", "X <= bi5", "X == bd0", "X < bd0", "X <= bd0", "X > bd0"};
        assertSameRowsAsFailover(doubles, doubleFilters);
        final Table floats = newTable(floatCol("X", 0.1f, 0.2f, 0.5f, 5.0f, -0.0f, 0.0f, Float.NaN, NULL_FLOAT));
        final String[] floatFilters = {"X == bd01", "X < bd01", "X == bdExactF", "X <= bdExactF", "X == bd05",
                "X > bd05", "X == bi5", "X < bi5", "X == bd0", "X <= bd0"};
        assertSameRowsAsFailover(floats, floatFilters);

        // a value that is the shortest decimal of the converted value keeps the typed filter, and in accepts it
        for (final String filter : new String[] {"X == bd01", "X >= bd01", "X == bi5", "X == bd0", "X < bd0"}) {
            assertFalse(filter, failsOver(doubles, filter));
        }
        for (final String filter : new String[] {"X == bi60", "X == bdExact", "X < bdExact"}) {
            assertTrue(filter, failsOver(doubles, filter));
        }
        assertFalse(failsOver(floats, "X == bd05"));
        assertTrue(failsOver(floats, "X == bd01"));
        assertTableEquals(newTable(doubleCol("X", 0.1)), doubles.where("X in bd01"));
        assertTableEquals(newTable(doubleCol("X", -0.0, 0.0)), doubles.where("X in bd0"));
        assertTableEquals(newTable(floatCol("X", 0.5f)), floats.where("X in bd05"));
        assertRejected(() -> doubles.where("X in bdExact"));
        assertRejected(() -> floats.where("X in bd01"));
    }

    @Test
    public void fractionalParamRangeFallsBackToTheLanguage() {
        QueryScope.addParam("coercionVal", 5.7);
        final Table t = newTable(intCol("X", 4, 5, 6));

        // 5 < 5.7 is true; the parameter was narrowed to 5, which made it false
        assertEquals(2, t.where("X < coercionVal").size());
        assertEquals(1, t.where("X > coercionVal").size());
    }

    @Test
    public void fractionalParamEqualityFallsBackToTheLanguage() {
        QueryScope.addParam("coercionVal", 5.7);
        final Table t = newTable(intCol("X", 5));

        assertEquals(0, t.where("X == coercionVal").size());
        assertEquals(1, t.where("X != coercionVal").size());
    }

    @Test
    public void fractionalParamInIsRejected() {
        QueryScope.addParam("coercionVal", 5.7);
        final Table t = newTable(intCol("X", 5));

        // `in` has no fallback, the same as the literal `X in 5.7`
        assertRejected(() -> t.where("X in coercionVal"));
    }

    @Test
    public void outOfRangeLongParam() {
        QueryScope.addParam("coercionVal", 5_000_000_000L);
        // 705032704 is (int) 5_000_000_000L
        final Table t = newTable(intCol("X", 705032704));

        assertEquals(0, t.where("X == coercionVal").size());
        assertEquals(1, t.where("X < coercionVal").size());
        assertRejected(() -> t.where("X in coercionVal"));
    }

    @Test
    public void inexactDoubleParamAgainstFloatColumn() {
        QueryScope.addParam("coercionVal", 5.7);
        final Table t = newTable(floatCol("F", 5.7f));

        // Java compares (double) 5.7f == 5.7, which is false, and (double) 5.7f < 5.7, which is true
        assertEquals(0, t.where("F == coercionVal").size());
        assertEquals(1, t.where("F < coercionVal").size());
        assertRejected(() -> t.where("F in coercionVal"));
    }

    @Test
    public void losslessParamsStillMatch() {
        QueryScope.addParam("coercionLong", 5L);
        QueryScope.addParam("coercionDouble", 5.0);
        QueryScope.addParam("coercionArray", new double[] {4.0, 6.0});
        QueryScope.addParam("coercionInt", 65);
        final Table t = newTable(intCol("X", 4, 5, 6), charCol("C", 'A', 'B', 'C'));

        assertEquals(1, t.where("X in coercionLong").size());
        assertEquals(1, t.where("X in coercionDouble").size());
        assertEquals(2, t.where("X in coercionArray").size());
        assertEquals(2, t.where("X <= coercionDouble").size());
        assertEquals(1, t.where("C in coercionInt").size());
    }

    @Test
    public void lossyValueInAnArrayParamIsRejected() {
        QueryScope.addParam("coercionArray", new double[] {4.0, 5.5});
        final Table t = newTable(intCol("X", 4, 5, 6));

        assertRejected(() -> t.where("X in coercionArray"));
    }

    @Test
    public void bigIntegerParamAgainstLongColumn() {
        QueryScope.addParam("coercionBig", BigInteger.valueOf(5));
        QueryScope.addParam("coercionHuge", BigInteger.ONE.shiftLeft(64));
        final Table t = newTable(longCol("X", 5L, 0L));

        assertEquals(1, t.where("X in coercionBig").size());
        assertRejected(() -> t.where("X in coercionHuge"));
    }

    @Test
    public void directValueOutOfRangeIsRejected() {
        // 44 is (byte) 300
        final Table t = newTable(byteCol("X", (byte) 44));

        assertRejected(() -> t.where(new MatchFilter(MatchOptions.REGULAR, "X", 300)));
    }

    @Test
    public void directValuesAreConvertedWithoutModifyingTheCallersArray() {
        final Object[] supplied = {5L, 6.0};
        final MatchFilter filter = new MatchFilter(MatchOptions.REGULAR, "X", supplied);
        filter.init(TableDefinition.of(ColumnDefinition.ofInt("X")));

        assertArrayEquals(new Object[] {5, 6}, filter.getValues());
        assertArrayEquals(new Object[] {5L, 6.0}, supplied);
    }

    @Test
    public void literalFilterInAgainstNarrowerColumn() {
        // literals arrive as long and double; they match whenever they are exactly representable
        final Table t = newTable(intCol("X", 4, 5, 6));
        final Filter in = FilterIn.of(ColumnName.of("X"), Literal.of(5L), Literal.of(6.0));
        assertEquals(2, t.where(in).size());

        final Filter lossy = FilterIn.of(ColumnName.of("X"), Literal.of(5L), Literal.of(5.5));
        assertRejected(() -> t.where(lossy));
    }

    @Test
    public void isNaNOnIntegralColumnsMatchesNothing() {
        // Float.NaN converted to int is 0; an int column has no NaN, so neither 0 nor anything else matches
        final Table t = newTable(intCol("X", 0, 1), longCol("L", 0L, 1L), charCol("C", '\0', 'a'));

        assertEquals(0, t.where(Filter.isNaN(ColumnName.of("X"))).size());
        assertEquals(2, t.where(Filter.not(Filter.isNaN(ColumnName.of("X")))).size());
        assertEquals(0, t.where(Filter.isNaN(ColumnName.of("L"))).size());
        assertEquals(0, t.where(Filter.isNaN(ColumnName.of("C"))).size());
    }

    @Test
    public void nanOnIntegralColumnsMatchesNothingWithoutNanMatch() {
        // as in Java, where X == NaN is false for every int X
        final Table t = newTable(intCol("X", 0, 1));

        assertEquals(0, t.where(new MatchFilter(MatchOptions.REGULAR, "X", Double.NaN)).size());
        assertEquals(2, t.where(new MatchFilter(MatchOptions.INVERTED, "X", Double.NaN)).size());
        assertEquals(1, t.where(new MatchFilter(MatchOptions.REGULAR, "X", Double.NaN, 1)).size());
    }

    @Test
    public void nanInQueryScopeValuesMatchesNothingWithoutNanMatch() {
        // as when the same values are supplied directly, above
        QueryScope.addParam("coercionNan", Double.NaN);
        QueryScope.addParam("coercionNanArray", new double[] {1.0, Double.NaN});
        QueryScope.addParam("coercionNanList", List.of(1.0, Double.NaN));
        final Table t = newTable(intCol("X", 0, 1));

        assertEquals(0, t.where("X in coercionNan").size());
        assertEquals(2, t.where("X not in coercionNan").size());
        assertTableEquals(newTable(intCol("X", 1)), t.where("X in coercionNanArray"));
        assertTableEquals(newTable(intCol("X", 1)), t.where("X in coercionNanList"));
        assertTableEquals(newTable(intCol("X", 0)), t.where("X not in coercionNanArray"));
    }

    @Test
    public void unconvertibleValueIsRejectedWithAConversionError() {
        // each used to escape as a ClassCastException, ArithmeticException or NumberFormatException, or to name the
        // wrong type
        assertConversionError("Cannot convert value <a> of type java.lang.String to Integer: it is not a number",
                ColumnDefinition.ofInt("X"), "a");
        assertConversionError("Cannot convert value <2.5> of type java.lang.Double to BigInteger",
                ColumnDefinition.fromGenericType("X", BigInteger.class), 2.5);
        assertConversionError("Cannot convert value <Infinity> of type java.lang.Double to BigDecimal",
                ColumnDefinition.fromGenericType("X", BigDecimal.class), Double.POSITIVE_INFINITY);
        assertConversionError("Cannot convert value <70000> of type java.lang.Integer to Character",
                ColumnDefinition.ofChar("X"), 70000);
    }

    private static void assertConversionError(
            final String expectedMessagePrefix,
            final ColumnDefinition<?> column,
            final Object value) {
        final IllegalArgumentException err = assertThrows(IllegalArgumentException.class,
                () -> new MatchFilter(MatchOptions.REGULAR, "X", value).init(TableDefinition.of(column)));
        assertTrue(err.getMessage(), err.getMessage().startsWith(expectedMessagePrefix));
    }

    @Test
    public void nullValuesArrayIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new MatchFilter(MatchOptions.REGULAR, "X", (Object[]) null));
    }

    @Test
    public void nullIsAlwaysAccepted() {
        final Table t = newTable(stringCol("S", "a", null), intCol("X", 1, NULL_INT));

        assertEquals(1, t.where(new MatchFilter(MatchOptions.REGULAR, "S", (Object) null)).size());
        assertEquals(1, t.where(new MatchFilter(MatchOptions.REGULAR, "X", (Object) null)).size());
    }

    @Test
    public void conversionFailureWithFailoverUsesTheFailover() {
        // 2^63 is beyond a long, but the query language's == rounds the largest longs to it, so it matches them
        QueryScope.addParam("coercionVal", 0x1p63);
        final MatchFilter filter = (MatchFilter) WhereFilterFactory.getExpression("X == coercionVal");
        filter.init(TableDefinition.of(ColumnDefinition.ofLong("X")));

        assertNotNull(filter.getFailoverFilter());
        assertNull(filter.getValues());
        assertTrue(MatchFilter.extractMatchFilter(filter).isEmpty());
    }

    @Test
    public void failoverReportsItsVirtualRowVariables() {
        final MatchFilter filter = (MatchFilter) WhereFilterFactory.getExpression("X == i");
        filter.init(TableDefinition.of(ColumnDefinition.ofInt("X")));

        assertNotNull(filter.getFailoverFilter());
        assertTrue(filter.hasVirtualRowVariables());
    }

    @Test
    public void failoverValidatesSafeForRefresh() {
        final QueryTable table = (QueryTable) newTable(intCol("X", 0, 1));
        table.setRefreshing(true);
        final MatchFilter filter = (MatchFilter) WhereFilterFactory.getExpression("X == i");
        filter.init(table.getDefinition());

        assertThrows(IllegalArgumentException.class, () -> filter.validateSafeForRefresh(table));
    }
}
