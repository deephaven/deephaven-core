//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.literal.Literal;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
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
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE_BOXED;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Filter values -- query-scope parameters and directly supplied match values -- must select exactly the rows that the
 * {@link ConditionFilter} failover, which evaluates the filter in the query language, would select. A value that cannot
 * be converted to the column's type exactly is never truncated or wrapped: filters that have a failover use it, and the
 * rest reject the value. There are two exceptions. A value that converts exactly to the column type's null value, an
 * int -128 for a byte column for instance, is null, as the byte -128 is. And a large floating-point value against an
 * int or long column matches only its exact equivalent, where the query language, comparing in floating point, would
 * also match the integers that round to it.
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
                "X == lwrap", "X == lnull", "X == cA",
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
    public void valueThatConvertsToTheNullValueIsNull() {
        // an int -128 converts exactly to the byte -128, which is NULL_BYTE: it means null, as the Byte -128 and the
        // literal -128 do. The query language, which compares it with a byte as an int, would match no rows instead.
        QueryScope.addParam("vm128", -128);
        QueryScope.addParam("vm128Byte", (byte) -128);
        QueryScope.addParam("lmin", (long) Integer.MIN_VALUE);
        QueryScope.addParam("vnegmax", -(double) Float.MAX_VALUE);
        final Table bytes = newTable(byteCol("X", (byte) 0, (byte) 5, NULL_BYTE));
        final Table nullByte = bytes.where("isNull(X)");

        assertTableEquals(nullByte, bytes.where("X == vm128"));
        assertTableEquals(nullByte, bytes.where("X in vm128"));
        assertTableEquals(nullByte, bytes.where(new MatchFilter(MatchOptions.REGULAR, "X", -128)));
        assertTableEquals(nullByte, bytes.where("X == vm128Byte"));
        assertTableEquals(nullByte, bytes.where("X == -128"));
        assertTableEquals(bytes.where("!isNull(X)"), bytes.where("X > vm128"));

        final Table ints = newTable(intCol("X", 0, NULL_INT));
        assertTableEquals(ints.where("isNull(X)"), ints.where("X == lmin"));
        final Table floats = newTable(floatCol("X", 0.5f, NULL_FLOAT));
        assertTableEquals(floats.where("isNull(X)"), floats.where("X == vnegmax"));
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
        final Table t = newTable(charCol("X", 'A', 'B', NULL_CHAR));

        assertSameRowsAsFailover(t, "X == v65", "X > v65", "X == vwrap", "X < vwrap", "X == d65");
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
        final BigInteger p60 = BigInteger.ONE.shiftLeft(60);

        assertSameRowsAsFailover(
                newTable(col("X", BigInteger.TEN.pow(30), new BigDecimal(1e30).toBigIntegerExact(), p60,
                        BigDecimal.valueOf(0x1p60).toBigIntegerExact(), BigInteger.valueOf(5), null)),
                "X == dv", "X >= dv", "X == d60", "X < d60", "X == l5", "X == v01");
        assertSameRowsAsFailover(
                newTable(col("X", new BigDecimal("0.1"), new BigDecimal(0.1), BigDecimal.valueOf(0.1f),
                        new BigDecimal(0.1f), new BigDecimal("5"), null)),
                "X == v01", "X <= v01", "X == f01", "X == l5");
    }

    @Test
    public void bigValuesAgainstFloatingPointColumnsSelectWhatTheFailoverSelects() {
        QueryScope.addParam("bi60", BigInteger.ONE.shiftLeft(60));
        QueryScope.addParam("bd01", new BigDecimal("0.1"));
        QueryScope.addParam("bdExact", new BigDecimal(0.1)); // the exact binary value of 0.1
        QueryScope.addParam("bdExactF", new BigDecimal(0.1f)); // the exact binary value of 0.1f

        assertSameRowsAsFailover(newTable(doubleCol("X", 0x1p60, 0.1, 0.2)),
                "X == bi60", "X == bd01", "X == bdExact", "X < bdExact");
        assertSameRowsAsFailover(newTable(floatCol("X", 0.1f, 0.2f)),
                "X == bd01", "X == bdExactF", "X <= bdExactF");
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
    public void wronglyTypedParamIsRejected() {
        QueryScope.addParam("coercionVal", 42);
        final Table t = newTable(stringCol("S", "a", "42"));

        assertRejected(() -> t.where("S in coercionVal"));
    }

    @Test
    public void wronglyTypedDirectValueIsRejected() {
        final Table t = newTable(stringCol("S", "aaa", "bbb", "ccc"));

        // the unsorted path used to answer "no match" and the sorted binary search used to throw
        // ClassCastException; both now reject the value when the filter is initialized
        assertRejected(() -> t.where(new MatchFilter(MatchOptions.REGULAR, "S", 42)));
        assertRejected(() -> new MatchFilter(MatchOptions.REGULAR, "S", 42)
                .init(TableDefinition.of(ColumnDefinition.ofString("S"))));
    }

    @Test
    public void nullIsAlwaysAccepted() {
        final Table t = newTable(stringCol("S", "a", null), intCol("X", 1, NULL_INT));

        assertEquals(1, t.where(new MatchFilter(MatchOptions.REGULAR, "S", (Object) null)).size());
        assertEquals(1, t.where(new MatchFilter(MatchOptions.REGULAR, "X", (Object) null)).size());
    }

    @Test
    public void conversionFailureWithFailoverUsesTheFailover() {
        QueryScope.addParam("coercionVal", 5.7);
        final MatchFilter filter = (MatchFilter) WhereFilterFactory.getExpression("X == coercionVal");
        filter.init(TableDefinition.of(ColumnDefinition.ofInt("X")));

        assertNotNull(filter.getFailoverFilterIfCached());
        assertNull(filter.getValues());
        assertTrue(MatchFilter.extractMatchFilter(filter).isEmpty());
    }
}
