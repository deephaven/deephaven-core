//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.*;

/**
 * This is a catch-all test collection for special cases of QueryTable.where(...), such as NULL / NaN handling and
 * compatibility.
 */
public class QueryTableWhereSpecialCasesTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    // region Table Creation
    private Table getStaticTable() {
        return newTable(
                charCol("charCol",
                        NULL_CHAR,
                        (char) 0,
                        (char) 1,
                        (char) 2,
                        (char) 3,
                        (char) 4,
                        (char) 5,
                        (char) 6),
                byteCol("byteCol",
                        NULL_BYTE, // Byte.MIN_VALUE,
                        (byte) (Byte.MIN_VALUE + 1),
                        (byte) -1,
                        (byte) 0,
                        (byte) 1,
                        (byte) (Byte.MAX_VALUE - 2),
                        (byte) (Byte.MAX_VALUE - 1),
                        Byte.MAX_VALUE),
                shortCol("shortCol",
                        NULL_SHORT,
                        (short) (Short.MIN_VALUE + 1),
                        (short) -1,
                        (short) 0,
                        (short) 1,
                        (short) (Short.MAX_VALUE - 2),
                        (short) (Short.MAX_VALUE - 1),
                        Short.MAX_VALUE),
                intCol("intCol",
                        NULL_INT,
                        Integer.MIN_VALUE + 1,
                        -1,
                        0,
                        1,
                        Integer.MAX_VALUE - 2,
                        Integer.MAX_VALUE - 1,
                        Integer.MAX_VALUE),
                longCol("longCol",
                        NULL_LONG,
                        Long.MIN_VALUE + 1,
                        -1L,
                        0L,
                        1L,
                        Long.MAX_VALUE - 2,
                        Long.MAX_VALUE - 1,
                        Long.MAX_VALUE),
                floatCol("floatCol",
                        NULL_FLOAT,
                        Float.NEGATIVE_INFINITY,
                        -1.0f,
                        -0.0f,
                        0.0f,
                        1.0f,
                        Float.POSITIVE_INFINITY,
                        Float.NaN),
                doubleCol("doubleCol",
                        NULL_DOUBLE,
                        Double.NEGATIVE_INFINITY,
                        -1.0,
                        -0.0,
                        0.0,
                        1.0,
                        Double.POSITIVE_INFINITY,
                        Double.NaN));
    }
    // endregion Table Creation

    // region Predicate Testing
    private interface CharPredicate {
        boolean apply(char value);
    }

    private interface BytePredicate {
        boolean apply(byte value);
    }

    private interface ShortPredicate {
        boolean apply(short value);
    }

    private interface IntPredicate {
        boolean apply(int value);
    }

    private interface LongPredicate {
        boolean apply(long value);
    }

    private interface FloatPredicate {
        boolean apply(float value);
    }

    private interface DoublePredicate {
        boolean apply(double value);
    }


    private void validateCharFilter(
            final Table table,
            final String charColName,
            final Filter charFilter,
            final CharPredicate predicate) {
        // Sorting by the filtered column engages sorted-column pushdown, which answers the filter with a binary
        // search rather than the chunk filter. Both paths must agree with the predicate.
        validateCharFilterOnSource(table, charColName, charFilter, predicate);
        validateCharFilterOnSource(table.sort(charColName), charColName, charFilter, predicate);
        validateCharFilterOnSource(table.sortDescending(charColName), charColName, charFilter, predicate);
    }

    private void validateCharFilterOnSource(
            final Table table,
            final String charColName,
            final Filter charFilter,
            final CharPredicate predicate) {
        final Table result = table.where(charFilter);
        try (final var it = result.characterColumnIterator(charColName)) {
            while (it.hasNext()) {
                final char value = it.nextChar();
                if (!predicate.apply(value)) {
                    throw new AssertionError("Value failed predicate test: " + value);
                }
            }
        }
        final Table resultInvert = table.where(charFilter.invert());
        try (final var it = resultInvert.characterColumnIterator(charColName)) {
            while (it.hasNext()) {
                final char value = it.nextChar();
                if (predicate.apply(value)) {
                    throw new AssertionError("Value failed invert predicate test: " + value);
                }
            }
        }
        final Table resultAll = table.where(Filter.or(charFilter, charFilter.invert()));
        assertTableEquals(resultAll, table);
    }

    private void validateByteFilter(
            final Table table,
            final String byteColName,
            final Filter byteFilter,
            final BytePredicate predicate) {
        // Sorting by the filtered column engages sorted-column pushdown, which answers the filter with a binary
        // search rather than the chunk filter. Both paths must agree with the predicate.
        validateByteFilterOnSource(table, byteColName, byteFilter, predicate);
        validateByteFilterOnSource(table.sort(byteColName), byteColName, byteFilter, predicate);
        validateByteFilterOnSource(table.sortDescending(byteColName), byteColName, byteFilter, predicate);
    }

    private void validateByteFilterOnSource(
            final Table table,
            final String byteColName,
            final Filter byteFilter,
            final BytePredicate predicate) {
        final Table result = table.where(byteFilter);
        try (final var it = result.byteColumnIterator(byteColName)) {
            while (it.hasNext()) {
                final byte value = it.nextByte();
                if (!predicate.apply(value)) {
                    throw new AssertionError("Value failed predicate test: " + value);
                }
            }
        }
        final Table resultInvert = table.where(byteFilter.invert());
        try (final var it = resultInvert.byteColumnIterator(byteColName)) {
            while (it.hasNext()) {
                final byte value = it.nextByte();
                if (predicate.apply(value)) {
                    throw new AssertionError("Value failed invert predicate test: " + value);
                }
            }
        }
        final Table resultAll = table.where(Filter.or(byteFilter, byteFilter.invert()));
        assertTableEquals(resultAll, table);
    }

    private void validateShortFilter(
            final Table table,
            final String shortColName,
            final Filter shortFilter,
            final ShortPredicate predicate) {
        // Sorting by the filtered column engages sorted-column pushdown, which answers the filter with a binary
        // search rather than the chunk filter. Both paths must agree with the predicate.
        validateShortFilterOnSource(table, shortColName, shortFilter, predicate);
        validateShortFilterOnSource(table.sort(shortColName), shortColName, shortFilter, predicate);
        validateShortFilterOnSource(table.sortDescending(shortColName), shortColName, shortFilter, predicate);
    }

    private void validateShortFilterOnSource(
            final Table table,
            final String shortColName,
            final Filter shortFilter,
            final ShortPredicate predicate) {
        final Table result = table.where(shortFilter);
        try (final var it = result.shortColumnIterator(shortColName)) {
            while (it.hasNext()) {
                final short value = it.nextShort();
                if (!predicate.apply(value)) {
                    throw new AssertionError("Value failed predicate test: " + value);
                }
            }
        }
        final Table resultInvert = table.where(shortFilter.invert());
        try (final var it = resultInvert.shortColumnIterator(shortColName)) {
            while (it.hasNext()) {
                final short value = it.nextShort();
                if (predicate.apply(value)) {
                    throw new AssertionError("Value failed invert predicate test: " + value);
                }
            }
        }
        final Table resultAll = table.where(Filter.or(shortFilter, shortFilter.invert()));
        assertTableEquals(resultAll, table);
    }

    private void validateIntFilter(
            final Table table,
            final String intColName,
            final Filter intFilter,
            final IntPredicate predicate) {
        // Sorting by the filtered column engages sorted-column pushdown, which answers the filter with a binary
        // search rather than the chunk filter. Both paths must agree with the predicate.
        validateIntFilterOnSource(table, intColName, intFilter, predicate);
        validateIntFilterOnSource(table.sort(intColName), intColName, intFilter, predicate);
        validateIntFilterOnSource(table.sortDescending(intColName), intColName, intFilter, predicate);
    }

    private void validateIntFilterOnSource(
            final Table table,
            final String intColName,
            final Filter intFilter,
            final IntPredicate predicate) {
        final Table result = table.where(intFilter);
        try (final var it = result.integerColumnIterator(intColName)) {
            while (it.hasNext()) {
                final int value = it.nextInt();
                if (!predicate.apply(value)) {
                    throw new AssertionError("Value failed predicate test: " + value);
                }
            }
        }
        final Table resultInvert = table.where(intFilter.invert());
        try (final var it = resultInvert.integerColumnIterator(intColName)) {
            while (it.hasNext()) {
                final int value = it.nextInt();
                if (predicate.apply(value)) {
                    throw new AssertionError("Value failed invert predicate test: " + value);
                }
            }
        }
        final Table resultAll = table.where(Filter.or(intFilter, intFilter.invert()));
        assertTableEquals(resultAll, table);
    }

    private void validateLongFilter(
            final Table table,
            final String longColName,
            final Filter longFilter,
            final LongPredicate predicate) {
        // Sorting by the filtered column engages sorted-column pushdown, which answers the filter with a binary
        // search rather than the chunk filter. Both paths must agree with the predicate.
        validateLongFilterOnSource(table, longColName, longFilter, predicate);
        validateLongFilterOnSource(table.sort(longColName), longColName, longFilter, predicate);
        validateLongFilterOnSource(table.sortDescending(longColName), longColName, longFilter, predicate);
    }

    private void validateLongFilterOnSource(
            final Table table,
            final String longColName,
            final Filter longFilter,
            final LongPredicate predicate) {
        final Table result = table.where(longFilter);
        try (final var it = result.longColumnIterator(longColName)) {
            while (it.hasNext()) {
                final long value = it.nextLong();
                if (!predicate.apply(value)) {
                    throw new AssertionError("Value failed predicate test: " + value);
                }
            }
        }
        final Table resultInvert = table.where(longFilter.invert());
        try (final var it = resultInvert.longColumnIterator(longColName)) {
            while (it.hasNext()) {
                final long value = it.nextLong();
                if (predicate.apply(value)) {
                    throw new AssertionError("Value failed invert predicate test: " + value);
                }
            }
        }
        final Table resultAll = table.where(Filter.or(longFilter, longFilter.invert()));
        assertTableEquals(resultAll, table);
    }

    private void validateFloatFilter(
            final Table table,
            final String floatColName,
            final Filter floatFilter,
            final FloatPredicate predicate) {
        // Sorting by the filtered column engages sorted-column pushdown, which answers the filter with a binary
        // search rather than the chunk filter. Both paths must agree with the predicate.
        validateFloatFilterOnSource(table, floatColName, floatFilter, predicate);
        validateFloatFilterOnSource(table.sort(floatColName), floatColName, floatFilter, predicate);
        validateFloatFilterOnSource(table.sortDescending(floatColName), floatColName, floatFilter, predicate);
    }

    private void validateFloatFilterOnSource(
            final Table table,
            final String floatColName,
            final Filter floatFilter,
            final FloatPredicate predicate) {
        final Table result = table.where(floatFilter);
        try (final var it = result.floatColumnIterator(floatColName)) {
            while (it.hasNext()) {
                final float value = it.nextFloat();
                if (!predicate.apply(value)) {
                    throw new AssertionError("Value failed predicate test: " + value);
                }
            }
        }
        final Table resultInvert = table.where(floatFilter.invert());
        try (final var it = resultInvert.floatColumnIterator(floatColName)) {
            while (it.hasNext()) {
                final float value = it.nextFloat();
                if (predicate.apply(value)) {
                    throw new AssertionError("Value failed invert predicate test: " + value);
                }
            }
        }
        final Table resultAll = table.where(Filter.or(floatFilter, floatFilter.invert()));
        assertTableEquals(resultAll, table);
    }

    private void validateDoubleFilter(
            final Table table,
            final String doubleColName,
            final Filter doubleFilter,
            final DoublePredicate predicate) {
        // Sorting by the filtered column engages sorted-column pushdown, which answers the filter with a binary
        // search rather than the chunk filter. Both paths must agree with the predicate.
        validateDoubleFilterOnSource(table, doubleColName, doubleFilter, predicate);
        validateDoubleFilterOnSource(table.sort(doubleColName), doubleColName, doubleFilter, predicate);
        validateDoubleFilterOnSource(table.sortDescending(doubleColName), doubleColName, doubleFilter, predicate);
    }

    private void validateDoubleFilterOnSource(
            final Table table,
            final String doubleColName,
            final Filter doubleFilter,
            final DoublePredicate predicate) {
        final Table result = table.where(doubleFilter);
        try (final var it = result.doubleColumnIterator(doubleColName)) {
            while (it.hasNext()) {
                final double value = it.nextDouble();
                if (!predicate.apply(value)) {
                    throw new AssertionError("Value failed predicate test: " + value);
                }
            }
        }
        final Table resultInvert = table.where(doubleFilter.invert());
        try (final var it = resultInvert.doubleColumnIterator(doubleColName)) {
            while (it.hasNext()) {
                final double value = it.nextDouble();
                if (predicate.apply(value)) {
                    throw new AssertionError("Value failed invert predicate test: " + value);
                }
            }
        }
        final Table resultAll = table.where(Filter.or(doubleFilter, doubleFilter.invert()));
        assertTableEquals(resultAll, table);
    }

    // endregion Predicate Testing

    /** A short string column with mixed case and a null, plus the ordering the sorted pushdown would see. */
    private static Table sortedStrings(final boolean descending) {
        final Table source = newTable(stringCol("S", "alpha", "BETA", "Alpha", null, "beta"));
        return descending ? source.sortDescending("S") : source.sort("S");
    }

    /**
     * A BigDecimal column carries values that are ordering-equal but not equal -- 1.0 and 1.00 differ in scale -- so a
     * sort leaves them in one contiguous run, in row order. Matching is decided by equality, so a filter must select
     * only the rows equal to its own value, and must do so whether or not the column is sorted.
     */
    @Test
    public void testMatchOnSortedBigDecimalColumn() {
        for (final String filter : new String[] {"X = 1.0", "X = 1.00", "X = 1", "X = 2.0", "X != 1.0",
                // Several values from the one ordering-equal run, so the run has to answer for all of them.
                "X in 1.0, 1.00", "X not in 1.0, 1.00", "X in 1.000, 1.0", "X in 1.0, 1.000",
                "X in 1.00, 1, 1.000", "X in 1.0, 1.00, 2.0", "X in 1.000, 2.0", "X in 1.0, 1.0"}) {
            final Table unsorted = bigDecimals();
            final Table expected = unsorted.where(filter).sort("X").coalesce();
            assertTableEquals(expected, bigDecimals().sort("X").where(filter).coalesce());

            final Table expectedDescending = unsorted.where(filter).sortDescending("X").coalesce();
            assertTableEquals(expectedDescending, bigDecimals().sortDescending("X").where(filter).coalesce());
        }
    }

    /** Ordering-equal values at several scales, plus one unrelated value. */
    private static Table bigDecimals() {
        return newTable(col("X",
                new BigDecimal("1.0"), new BigDecimal("1.00"), new BigDecimal("1"),
                new BigDecimal("1E+0"), new BigDecimal("2.0")));
    }

    /**
     * Sorted-column pushdown answers a match filter with a binary search that navigates by {@code compareTo}, which is
     * case significant, so it cannot reach the rows a case-insensitive filter matches. Its result is emitted as an
     * exact match with nothing left for a residual pass to repair, so such a filter has to decline the search rather
     * than answer it with the rows the search happens to visit.
     */
    @Test
    public void testCaseInsensitiveMatchOnSortedColumn() {
        for (final boolean descending : new boolean[] {false, true}) {
            for (final String filter : new String[] {"S icase in `alpha`", "S icase not in `alpha`"}) {
                // A fresh table per side, so neither result is served from the other's memoized where().
                final boolean originalSetting = QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION;
                final Table expected;
                QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION = true;
                try {
                    expected = sortedStrings(descending).where(filter).coalesce();
                } finally {
                    QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION = originalSetting;
                }
                assertTableEquals(expected, sortedStrings(descending).where(filter).coalesce());
            }
        }
    }

    @Test
    public void testRangeGTZero() {
        final Table source = getStaticTable();

        // Testing val > 0, must exclude NULL values
        validateCharFilter(
                source,
                "charCol",
                CharRangeFilter.gt("charCol", (char) 0),
                val -> val > 0 && val != NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol > '\u0000'"),
                val -> val > 0 && val != NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                ByteRangeFilter.gt("byteCol", (byte) 0),
                val -> val > 0 && val != NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol > 0"),
                val -> val > 0 && val != NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                ShortRangeFilter.gt("shortCol", (short) 0),
                val -> val > 0 && val != NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol > 0"),
                val -> val > 0 && val != NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                IntRangeFilter.gt("intCol", 0),
                val -> val > 0 && val != NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol > 0"),
                val -> val > 0 && val != NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                LongRangeFilter.gt("longCol", 0L),
                val -> val > 0 && val != NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol > 0"),
                val -> val > 0 && val != NULL_LONG);
        // NaN also excluded for float/double
        validateFloatFilter(
                source,
                "floatCol",
                FloatRangeFilter.gt("floatCol", 0.0f),
                val -> val > 0 && val != NULL_FLOAT && !Float.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol > 0.0f"),
                val -> val > 0 && val != NULL_FLOAT && !Float.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                DoubleRangeFilter.gt("doubleCol", 0.0),
                val -> val > 0 && val != NULL_DOUBLE && !Double.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol > 0.0"),
                val -> val > 0 && val != NULL_DOUBLE && !Double.isNaN(val));
    }

    @Test
    public void testRangeLEQZero() {
        final Table source = getStaticTable();

        // Testing val <= 0, includes NULL because NULL < any value when sorting and comparing
        validateCharFilter(
                source,
                "charCol",
                CharRangeFilter.leq("charCol", (char) 0),
                val -> val <= 0 || val == NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol <= '\u0000'"),
                val -> val <= 0 || val == NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                ByteRangeFilter.leq("byteCol", (byte) 0),
                val -> val <= 0 || val == NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol <= 0"),
                val -> val <= 0 || val == NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                ShortRangeFilter.leq("shortCol", (short) 0),
                val -> val <= 0 || val == NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol <= 0"),
                val -> val <= 0 || val == NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                IntRangeFilter.leq("intCol", 0),
                val -> val <= 0 || val == NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol <= 0"),
                val -> val <= 0 || val == NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                LongRangeFilter.leq("longCol", 0L),
                val -> val <= 0 || val == NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol <= 0"),
                val -> val <= 0 || val == NULL_LONG);
        // NaN also excluded for float/double
        validateFloatFilter(
                source,
                "floatCol",
                FloatRangeFilter.leq("floatCol", 0.0f),
                val -> (val <= 0 || val == NULL_FLOAT) && !Float.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol <= 0.0f"),
                val -> (val <= 0 || val == NULL_FLOAT) && !Float.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                DoubleRangeFilter.leq("doubleCol", 0.0),
                val -> (val <= 0 || val == NULL_DOUBLE) && !Double.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol <= 0.0"),
                val -> (val <= 0 || val == NULL_DOUBLE) && !Double.isNaN(val));
    }

    /**
     * Deephaven ordering sorts NaN above positive infinity, so an inclusive {@code +Inf} upper bound is not an
     * unbounded one: the trailing NaN block has to be located and excluded, which the sorted range pushdown can only do
     * by searching the upper bound as well as the lower. An inclusive NaN upper bound is the unbounded case, and there
     * the upper-bound search can be skipped. Neither shape is produced by {@code geq()}, whose NaN upper bound is
     * exclusive, so both are built directly.
     */
    @Test
    public void testRangeInclusiveInfiniteAndNaNUpperBounds() {
        final Table source = getStaticTable();

        // An inclusive +Inf upper bound admits +Inf and excludes NaN.
        validateDoubleFilter(
                source,
                "doubleCol",
                new DoubleRangeFilter("doubleCol", 0.0, Double.POSITIVE_INFINITY, true, true),
                val -> val >= 0.0 && !Double.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                new FloatRangeFilter("floatCol", 0.0f, Float.POSITIVE_INFINITY, true, true),
                val -> val >= 0.0f && !Float.isNaN(val));

        // An inclusive NaN upper bound is unbounded above, so NaN is admitted along with +Inf.
        validateDoubleFilter(
                source,
                "doubleCol",
                new DoubleRangeFilter("doubleCol", 0.0, Double.NaN, true, true),
                val -> val >= 0.0 || Double.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                new FloatRangeFilter("floatCol", 0.0f, Float.NaN, true, true),
                val -> val >= 0.0f || Float.isNaN(val));

        // An exclusive NaN upper bound is what geq() builds, and must exclude NaN.
        validateDoubleFilter(
                source,
                "doubleCol",
                DoubleRangeFilter.geq("doubleCol", 0.0),
                val -> val >= 0.0 && !Double.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                FloatRangeFilter.geq("floatCol", 0.0f),
                val -> val >= 0.0f && !Float.isNaN(val));
    }

    /**
     * Under IEEE 754 every ordered comparison against NaN is false, NaN itself included, so a NaN bound matches no row.
     * Deephaven ordering instead sorts NaN above every other value, which would have {@code <= NaN} admit everything
     * and {@code >= NaN} admit only NaN; the range operators deliberately do not use that ordering for a NaN bound.
     * Such a bound normally arrives by accident, through a query scope variable, rather than written out.
     */
    @Test
    public void testNaNRangeBoundsMatchNothing() {
        final Table source = getStaticTable();

        for (final String op : new String[] {"<", "<=", ">", ">="}) {
            validateDoubleFilter(
                    source,
                    "doubleCol",
                    RawString.of("doubleCol " + op + " NaN"),
                    val -> false);
            validateFloatFilter(
                    source,
                    "floatCol",
                    RawString.of("floatCol " + op + " NaN"),
                    val -> false);
        }

        // Equality follows the same rules: nothing equals NaN, and everything differs from it, NaN included.
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol == NaN"),
                val -> false);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol != NaN"),
                val -> true);
    }

    @Test
    public void testZeroEquality() {
        final Table source = getStaticTable();

        // Testing val = 0
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol = '\u0000'"),
                val -> val == 0);
        validateCharFilter(
                source,
                "charCol",
                new MatchFilter(MatchOptions.REGULAR, "charCol", (char) 0),
                val -> val == 0);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol = 0"),
                val -> val == 0);
        validateByteFilter(
                source,
                "byteCol",
                new MatchFilter(MatchOptions.REGULAR, "byteCol", (byte) 0),
                val -> val == 0);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol = 0"),
                val -> val == 0);
        validateShortFilter(
                source,
                "shortCol",
                new MatchFilter(MatchOptions.REGULAR, "shortCol", (short) 0),
                val -> val == 0);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol = 0"),
                val -> val == 0);
        validateIntFilter(
                source,
                "intCol",
                new MatchFilter(MatchOptions.REGULAR, "intCol", 0),
                val -> val == 0);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol = 0"),
                val -> val == 0L);
        validateLongFilter(
                source,
                "longCol",
                new MatchFilter(MatchOptions.REGULAR, "longCol", 0L),
                val -> val == 0L);
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol = 0.0f"),
                val -> val == 0.0f);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.REGULAR, "floatCol", 0.0f),
                val -> val == 0.0f);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol = 0.0"),
                val -> val == 0.0);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.REGULAR, "doubleCol", 0.0),
                val -> val == 0.0);
    }

    @Test
    public void testZeroInequality() {
        final Table source = getStaticTable();

        // Testing val != 0, includes NULL values
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol != '\u0000'"),
                val -> val != 0 || val == NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                new MatchFilter(MatchOptions.INVERTED, "charCol", (char) 0),
                val -> val != 0 || val == NULL_CHAR);

        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol != 0"),
                val -> val != 0 || val == NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                new MatchFilter(MatchOptions.INVERTED, "byteCol", (byte) 0),
                val -> val != 0 || val == NULL_BYTE);

        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol != 0"),
                val -> val != 0 || val == NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                new MatchFilter(MatchOptions.INVERTED, "shortCol", (short) 0),
                val -> val != 0 || val == NULL_SHORT);

        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol != 0"),
                val -> val != 0 || val == NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                new MatchFilter(MatchOptions.INVERTED, "intCol", 0),
                val -> val != 0 || val == NULL_INT);

        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol != 0"),
                val -> val != 0L || val == NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                new MatchFilter(MatchOptions.INVERTED, "longCol", 0L),
                val -> val != 0L || val == NULL_LONG);

        // NaN also included for float/double (because NaN != all values)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol != 0.0f"),
                val -> val != 0.0f || val == NULL_FLOAT || Float.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.INVERTED, "floatCol", 0.0f),
                val -> val != 0.0f || val == NULL_FLOAT || Float.isNaN(val));

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol != 0.0"),
                val -> val != 0.0 || val == NULL_DOUBLE || Double.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.INVERTED, "doubleCol", 0.0),
                val -> val != 0.0 || val == NULL_DOUBLE || Double.isNaN(val));
    }

    @Test
    public void testNullEquality() {
        final Table source = getStaticTable();

        // Testing val = NULL
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol = null"),
                val -> val == NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                new MatchFilter(MatchOptions.REGULAR, "charCol", NULL_CHAR),
                val -> val == NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol = null"),
                val -> val == NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                new MatchFilter(MatchOptions.REGULAR, "byteCol", NULL_BYTE),
                val -> val == NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol = null"),
                val -> val == NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                new MatchFilter(MatchOptions.REGULAR, "shortCol", NULL_SHORT),
                val -> val == NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol = null"),
                val -> val == NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                new MatchFilter(MatchOptions.REGULAR, "intCol", NULL_INT),
                val -> val == NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol = null"),
                val -> val == NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                new MatchFilter(MatchOptions.REGULAR, "longCol", NULL_LONG),
                val -> val == NULL_LONG);
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol = null"),
                val -> val == NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.REGULAR, "floatCol", NULL_FLOAT),
                val -> val == NULL_FLOAT);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol = null"),
                val -> val == NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.REGULAR, "doubleCol", NULL_DOUBLE),
                val -> val == NULL_DOUBLE);
    }

    @Test
    public void testNullInequality() {
        final Table source = getStaticTable();

        // Testing val != NULL
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol != null"),
                val -> val != NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                new MatchFilter(MatchOptions.INVERTED, "charCol", NULL_CHAR),
                val -> val != NULL_CHAR);

        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol != null"),
                val -> val != NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                new MatchFilter(MatchOptions.INVERTED, "byteCol", NULL_BYTE),
                val -> val != NULL_BYTE);

        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol != null"),
                val -> val != NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                new MatchFilter(MatchOptions.INVERTED, "shortCol", NULL_SHORT),
                val -> val != NULL_SHORT);

        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol != null"),
                val -> val != NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                new MatchFilter(MatchOptions.INVERTED, "intCol", NULL_INT),
                val -> val != NULL_INT);

        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol != null"),
                val -> val != NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                new MatchFilter(MatchOptions.INVERTED, "longCol", NULL_LONG),
                val -> val != NULL_LONG);

        // NaN also included for float/double
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol != null"),
                val -> val != NULL_FLOAT || Float.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.INVERTED, "floatCol", NULL_FLOAT),
                val -> val != NULL_FLOAT || Float.isNaN(val));

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol != null"),
                val -> val != NULL_DOUBLE || Double.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.INVERTED, "doubleCol", NULL_DOUBLE),
                val -> val != NULL_DOUBLE || Double.isNaN(val));
    }

    @Test
    public void testNanEquality() {
        final Table source = getStaticTable();

        // Testing val = NaN, nothing should match (including NaN)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol = NaN"),
                val -> false);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.REGULAR, "floatCol", Float.NaN),
                val -> false);

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol = NaN"),
                val -> false);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.REGULAR, "doubleCol", Double.NaN),
                val -> false);
    }

    @Test
    public void testNanInequality() {
        final Table source = getStaticTable();

        // Testing val != NaN, everything should match (including NULL / NaN)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol != NaN"),
                val -> true);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.INVERTED, "floatCol", Float.NaN),
                val -> true);

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol != NaN"),
                val -> true);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.INVERTED, "doubleCol", Double.NaN),
                val -> true);
    }

    @Test
    public void testMatchZeroOne() {
        final Table source = getStaticTable();

        // Testing val in [0, 1]
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol in '\u0000', '\u0001'"),
                val -> val == 0 || val == 1);
        validateCharFilter(
                source,
                "charCol",
                new MatchFilter(MatchOptions.REGULAR, "charCol", (char) 0, (char) 1),
                val -> val == 0 || val == 1);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol in 0, 1"),
                val -> val == 0 || val == 1);
        validateByteFilter(
                source,
                "byteCol",
                new MatchFilter(MatchOptions.REGULAR, "byteCol", (byte) 0, (byte) 1),
                val -> val == 0 || val == 1);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol in 0, 1"),
                val -> val == 0 || val == 1);
        validateShortFilter(
                source,
                "shortCol",
                new MatchFilter(MatchOptions.REGULAR, "shortCol", (short) 0, (short) 1),
                val -> val == 0 || val == 1);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol in 0, 1"),
                val -> val == 0 || val == 1);
        validateIntFilter(
                source,
                "intCol",
                new MatchFilter(MatchOptions.REGULAR, "intCol", 0, 1),
                val -> val == 0 || val == 1);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol in 0, 1"),
                val -> val == 0 || val == 1);
        validateLongFilter(
                source,
                "longCol",
                new MatchFilter(MatchOptions.REGULAR, "longCol", 0L, 1L),
                val -> val == 0 || val == 1);

        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol in 0.0f, 1.0f"),
                val -> val == 0.0f || val == 1.0f);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.REGULAR, "floatCol", 0.0f, 1.0f),
                val -> val == 0.0f || val == 1.0f);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.REGULAR, "floatCol", -0.0f, 1.0f),
                val -> val == 0.0f || val == 1.0f);

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol in 0.0, 1.0"),
                val -> val == 0.0 || val == 1.0);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.REGULAR, "doubleCol", 0.0, 1.0),
                val -> val == 0.0 || val == 1.0);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.REGULAR, "doubleCol", -0.0, 1.0),
                val -> val == 0.0 || val == 1.0);
    }

    @Test
    public void testMatchNotZeroOne() {
        final Table source = getStaticTable();

        // Testing val not in [0, 1]
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol not in '\u0000', '\u0001'"),
                val -> val != 0 && val != 1);
        validateCharFilter(
                source,
                "charCol",
                new MatchFilter(MatchOptions.INVERTED, "charCol", (char) 0, (char) 1),
                val -> val != 0 && val != 1);

        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol not in 0, 1"),
                val -> val != 0 && val != 1);
        validateByteFilter(
                source,
                "byteCol",
                new MatchFilter(MatchOptions.INVERTED, "byteCol", (byte) 0, (byte) 1),
                val -> val != 0 && val != 1);

        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol not in 0, 1"),
                val -> val != 0 && val != 1);
        validateShortFilter(
                source,
                "shortCol",
                new MatchFilter(MatchOptions.INVERTED, "shortCol", (short) 0, (short) 1),
                val -> val != 0 && val != 1);

        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol not in 0, 1"),
                val -> val != 0 && val != 1);
        validateIntFilter(
                source,
                "intCol",
                new MatchFilter(MatchOptions.INVERTED, "intCol", 0, 1),
                val -> val != 0 && val != 1);

        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol not in 0, 1"),
                val -> val != 0 && val != 1);
        validateLongFilter(
                source,
                "longCol",
                new MatchFilter(MatchOptions.INVERTED, "longCol", 0L, 1L),
                val -> val != 0 && val != 1);

        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol not in 0.0f, 1.0f"),
                val -> val != 0.0f && val != 1.0f);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.INVERTED, "floatCol", 0.0f, 1.0f),
                val -> val != 0.0f && val != 1.0f);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.INVERTED, "floatCol", -0.0f, 1.0f),
                val -> val != 0.0f && val != 1.0f);

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol not in 0.0, 1.0"),
                val -> val != 0.0 && val != 1.0);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.INVERTED, "doubleCol", 0.0, 1.0),
                val -> val != 0.0 && val != 1.0);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.INVERTED, "doubleCol", -0.0, 1.0),
                val -> val != 0.0 && val != 1.0);
    }

    @Test
    public void testMatchZeroNull() {
        final Table source = getStaticTable();

        // Testing val in [0, null]
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol in '\u0000', null"),
                val -> val == 0 || val == NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                new MatchFilter(MatchOptions.REGULAR, "charCol", (char) 0, NULL_CHAR),
                val -> val == 0 || val == NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol in 0, null"),
                val -> val == 0 || val == NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                new MatchFilter(MatchOptions.REGULAR, "byteCol", (byte) 0, NULL_BYTE),
                val -> val == 0 || val == NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol in 0, null"),
                val -> val == 0 || val == NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                new MatchFilter(MatchOptions.REGULAR, "shortCol", (short) 0, NULL_SHORT),
                val -> val == 0 || val == NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol in 0, null"),
                val -> val == 0 || val == NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                new MatchFilter(MatchOptions.REGULAR, "intCol", 0, NULL_INT),
                val -> val == 0 || val == NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol in 0, null"),
                val -> val == 0L || val == NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                new MatchFilter(MatchOptions.REGULAR, "longCol", 0L, NULL_LONG),
                val -> val == 0L || val == NULL_LONG);

        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol in 0.0f, null"),
                val -> val == 0.0f || val == NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.REGULAR, "floatCol", 0.0f, NULL_FLOAT),
                val -> val == 0.0f || val == NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.REGULAR, "floatCol", -0.0f, NULL_FLOAT),
                val -> val == 0.0f || val == NULL_FLOAT);

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol in 0.0, null"),
                val -> val == 0.0 || val == NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.REGULAR, "doubleCol", 0.0, NULL_DOUBLE),
                val -> val == 0.0 || val == NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.REGULAR, "doubleCol", -0.0, NULL_DOUBLE),
                val -> val == 0.0 || val == NULL_DOUBLE);
    }

    @Test
    public void testMatchNotZeroNull() {
        final Table source = getStaticTable();

        // Testing val not in [0, null]
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol not in '\u0000', null"),
                val -> val != 0 && val != NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                new MatchFilter(MatchOptions.INVERTED, "charCol", (char) 0, NULL_CHAR),
                val -> val != 0 && val != NULL_CHAR);

        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol not in 0, null"),
                val -> val != 0 && val != NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                new MatchFilter(MatchOptions.INVERTED, "byteCol", (byte) 0, NULL_BYTE),
                val -> val != 0 && val != NULL_BYTE);

        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol not in 0, null"),
                val -> val != 0 && val != NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                new MatchFilter(MatchOptions.INVERTED, "shortCol", (short) 0, NULL_SHORT),
                val -> val != 0 && val != NULL_SHORT);

        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol not in 0, null"),
                val -> val != 0 && val != NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                new MatchFilter(MatchOptions.INVERTED, "intCol", 0, NULL_INT),
                val -> val != 0 && val != NULL_INT);

        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol not in 0, null"),
                val -> val != 0L && val != NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                new MatchFilter(MatchOptions.INVERTED, "longCol", 0L, NULL_LONG),
                val -> val != 0L && val != NULL_LONG);

        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol not in 0.0f, null"),
                val -> val != 0.0f && val != NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.INVERTED, "floatCol", 0.0f, NULL_FLOAT),
                val -> val != 0.0f && val != NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.INVERTED, "floatCol", -0.0f, NULL_FLOAT),
                val -> val != 0.0f && val != NULL_FLOAT);

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol not in 0.0, null"),
                val -> val != 0.0 && val != NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.INVERTED, "doubleCol", 0.0, NULL_DOUBLE),
                val -> val != 0.0 && val != NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.INVERTED, "doubleCol", -0.0, NULL_DOUBLE),
                val -> val != 0.0 && val != NULL_DOUBLE);
    }

    @Test
    public void testMatchZeroNan() {
        final Table source = getStaticTable();

        // Testing val in [0, NaN]
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol in 0.0f, NaN"),
                val -> val == 0.0f || Float.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.builder().nanMatch(true).build(), "floatCol", 0.0f, Float.NaN),
                val -> val == 0.0f || Float.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.builder().nanMatch(true).build(), "floatCol", -0.0f, Float.NaN),
                val -> val == 0.0f || Float.isNaN(val));

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol in 0.0, NaN"),
                val -> val == 0.0 || Double.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.builder().nanMatch(true).build(), "doubleCol", 0.0, Double.NaN),
                val -> val == 0.0 || Double.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.builder().nanMatch(true).build(), "doubleCol", -0.0, Double.NaN),
                val -> val == 0.0 || Double.isNaN(val));
    }

    @Test
    public void testMatchNotZeroNan() {
        final Table source = getStaticTable();

        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol not in 0.0f, NaN"),
                val -> val != 0.0f && !Float.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.builder().inverted(true).nanMatch(true).build(), "floatCol", 0.0f,
                        Float.NaN),
                val -> val != 0.0f && !Float.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.builder().inverted(true).nanMatch(true).build(), "floatCol", -0.0f,
                        Float.NaN),
                val -> val != 0.0f && !Float.isNaN(val));

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol not in 0.0, NaN"),
                val -> val != 0.0 && !Double.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.builder().inverted(true).nanMatch(true).build(), "doubleCol", 0.0,
                        Double.NaN),
                val -> val != 0.0 && !Double.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.builder().inverted(true).nanMatch(true).build(), "doubleCol", -0.0,
                        Double.NaN),
                val -> val != 0.0 && !Double.isNaN(val));
    }

    @Test
    public void testConditionalGTZero() {
        final Table source = getStaticTable();

        // Testing val > 0 && true (forces ConditionalFilter)
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol > 0 && true"),
                val -> val > 0 && val != NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol > 0 && true"),
                val -> val > 0 && val != NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol > 0 && true"),
                val -> val > 0 && val != NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol > 0 && true"),
                val -> val > 0 && val != NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol > 0 && true"),
                val -> val > 0 && val != NULL_LONG);

        // NaN should be excluded for float/double
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol > 0.0f && true"),
                val -> val > 0 && val != NULL_FLOAT && !Float.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol > 0.0 && true"),
                val -> val > 0 && val != NULL_DOUBLE && !Double.isNaN(val));
    }

    @Test
    public void testConditionalLEQZero() {
        final Table source = getStaticTable();

        // Testing val <= 0 && true (forces ConditionalFilter)
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol <= 0 && true"),
                val -> val <= 0 || val == NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol <= 0 && true"),
                val -> val <= 0 || val == NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol <= 0 && true"),
                val -> val <= 0 || val == NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol <= 0 && true"),
                val -> val <= 0 || val == NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol <= 0 && true"),
                val -> val <= 0 || val == NULL_LONG);

        // NaN also excluded for float/double
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol <= 0.0f && true"),
                val -> (val <= 0.0f || val == NULL_FLOAT) && !Float.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol <= 0.0 && true"),
                val -> (val <= 0.0 || val == NULL_DOUBLE) && !Double.isNaN(val));
    }

    @Test
    public void testConditionalZeroEquality() {
        final Table source = getStaticTable();

        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol = 0 && true"),
                val -> val == 0 && val != NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol = 0 && true"),
                val -> val == 0 && val != NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol = 0 && true"),
                val -> val == 0 && val != NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol = 0 && true"),
                val -> val == 0 && val != NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol = 0 && true"),
                val -> val == 0L && val != NULL_LONG);

        // Float/Double must exclude NaN
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol = 0.0f && true"),
                val -> val == 0.0f && val != NULL_FLOAT && !Float.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol = 0.0 && true"),
                val -> val == 0.0 && val != NULL_DOUBLE && !Double.isNaN(val));
    }

    @Test
    public void testConditionalZeroInequality() {
        final Table source = getStaticTable();

        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol != 0 && true"),
                val -> val != 0 || val == NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol != 0 && true"),
                val -> val != 0 || val == NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol != 0 && true"),
                val -> val != 0 || val == NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol != 0 && true"),
                val -> val != 0 || val == NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol != 0 && true"),
                val -> val != 0L || val == NULL_LONG);

        // Float/Double must exclude NaN
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol != 0.0f && true"),
                val -> val != 0.0f || val == NULL_FLOAT || Float.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol != 0.0 && true"),
                val -> val != 0.0 || val == NULL_DOUBLE || Double.isNaN(val));
    }

    @Test
    public void testConditionalNullEquality() {
        final Table source = getStaticTable();

        // Testing val = NULL && true (forces ConditionalFilter)
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol = null && true"),
                val -> val == NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol = null && true"),
                val -> val == NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol = null && true"),
                val -> val == NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol = null && true"),
                val -> val == NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol = null && true"),
                val -> val == NULL_LONG);

        // Float/Double must exclude NaN
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol = null && true"),
                val -> val == NULL_FLOAT && !Float.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol = null && true"),
                val -> val == NULL_DOUBLE && !Double.isNaN(val));
    }

    @Test
    public void testConditionalNullInequality() {
        final Table source = getStaticTable();

        // Testing val != NULL && true (forces ConditionalFilter)
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol != null && true"),
                val -> val != NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol != null && true"),
                val -> val != NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol != null && true"),
                val -> val != NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol != null && true"),
                val -> val != NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol != null && true"),
                val -> val != NULL_LONG);

        // Float/Double must include NaN
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol != null && true"),
                val -> val != NULL_FLOAT || Float.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol != null && true"),
                val -> val != NULL_DOUBLE || Double.isNaN(val));
    }

    @Test
    public void testConditionalNanEquality() {
        final Table source = getStaticTable();

        // Testing val = NaN, nothing should match (including NaN)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol = Float.NaN && true"),
                val -> false);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.REGULAR, "floatCol", Float.NaN),
                val -> false);

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol = Double.NaN && true"),
                val -> false);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.REGULAR, "doubleCol", Double.NaN),
                val -> false);
    }

    @Test
    public void testConditionalNanInequality() {
        final Table source = getStaticTable();

        // Testing val != NaN, everything should match (including NULL / NaN)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol != Float.NaN && true"),
                val -> true);
        validateFloatFilter(
                source,
                "floatCol",
                new MatchFilter(MatchOptions.INVERTED, "floatCol", Float.NaN),
                val -> true);

        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol != Double.NaN && true"),
                val -> true);
        validateDoubleFilter(
                source,
                "doubleCol",
                new MatchFilter(MatchOptions.INVERTED, "doubleCol", Double.NaN),
                val -> true);
    }

    @Test
    public void testFilterIsNaN() {
        final Table source = getStaticTable();

        // Testing Filter.isNaN, only NaN should match
        validateFloatFilter(
                source,
                "floatCol",
                Filter.isNaN(ColumnName.of("floatCol")),
                val -> Float.isNaN(val));
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol in NaN"),
                val -> Float.isNaN(val));

        validateDoubleFilter(
                source,
                "doubleCol",
                Filter.isNaN(ColumnName.of("doubleCol")),
                val -> Double.isNaN(val));
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol in NaN"),
                val -> Double.isNaN(val));
    }

    @Test
    public void testFilterIsNotNaN() {
        final Table source = getStaticTable();

        // Testing Filter.isNaN, nothing should match (including NaN)
        validateFloatFilter(
                source,
                "floatCol",
                Filter.isNotNaN(ColumnName.of("floatCol")),
                val -> !Float.isNaN(val));

        validateDoubleFilter(
                source,
                "doubleCol",
                Filter.isNotNaN(ColumnName.of("doubleCol")),
                val -> !Double.isNaN(val));
    }

    @Test
    public void testFilterIsNull() {
        final Table source = getStaticTable();

        validateCharFilter(
                source,
                "charCol",
                Filter.isNull(ColumnName.of("charCol")),
                val -> val == NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol == null"),
                val -> val == NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                RawString.of("isNull(charCol)"),
                val -> val == NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol in null"),
                val -> val == NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                Filter.isNull(ColumnName.of("byteCol")),
                val -> val == NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol == null"),
                val -> val == NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("isNull(byteCol)"),
                val -> val == NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol in null"),
                val -> val == NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                Filter.isNull(ColumnName.of("shortCol")),
                val -> val == NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol == null"),
                val -> val == NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("isNull(shortCol)"),
                val -> val == NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol in null"),
                val -> val == NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                Filter.isNull(ColumnName.of("intCol")),
                val -> val == NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol == null"),
                val -> val == NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("isNull(intCol)"),
                val -> val == NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol in null"),
                val -> val == NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                Filter.isNull(ColumnName.of("longCol")),
                val -> val == NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol == null"),
                val -> val == NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("isNull(longCol)"),
                val -> val == NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol in null"),
                val -> val == NULL_LONG);
        validateFloatFilter(
                source,
                "floatCol",
                Filter.isNull(ColumnName.of("floatCol")),
                val -> val == NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol == null"),
                val -> val == NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("isNull(floatCol)"),
                val -> val == NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol in null"),
                val -> val == NULL_FLOAT);
        validateDoubleFilter(
                source,
                "doubleCol",
                Filter.isNull(ColumnName.of("doubleCol")),
                val -> val == NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol == null"),
                val -> val == NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("isNull(doubleCol)"),
                val -> val == NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol in null"),
                val -> val == NULL_DOUBLE);
    }

    @Test
    public void testFilterIsNotNull() {
        final Table source = getStaticTable();

        validateCharFilter(
                source,
                "charCol",
                Filter.isNotNull(ColumnName.of("charCol")),
                val -> val != NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol != null"),
                val -> val != NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                RawString.of("!isNull(charCol)"),
                val -> val != NULL_CHAR);
        validateCharFilter(
                source,
                "charCol",
                RawString.of("charCol not in null"),
                val -> val != NULL_CHAR);
        validateByteFilter(
                source,
                "byteCol",
                Filter.isNotNull(ColumnName.of("byteCol")),
                val -> val != NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol != null"),
                val -> val != NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("!isNull(byteCol)"),
                val -> val != NULL_BYTE);
        validateByteFilter(
                source,
                "byteCol",
                RawString.of("byteCol not in null"),
                val -> val != NULL_BYTE);
        validateShortFilter(
                source,
                "shortCol",
                Filter.isNotNull(ColumnName.of("shortCol")),
                val -> val != NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol != null"),
                val -> val != NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("!isNull(shortCol)"),
                val -> val != NULL_SHORT);
        validateShortFilter(
                source,
                "shortCol",
                RawString.of("shortCol not in null"),
                val -> val != NULL_SHORT);
        validateIntFilter(
                source,
                "intCol",
                Filter.isNotNull(ColumnName.of("intCol")),
                val -> val != NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol != null"),
                val -> val != NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("!isNull(intCol)"),
                val -> val != NULL_INT);
        validateIntFilter(
                source,
                "intCol",
                RawString.of("intCol not in null"),
                val -> val != NULL_INT);
        validateLongFilter(
                source,
                "longCol",
                Filter.isNotNull(ColumnName.of("longCol")),
                val -> val != NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol != null"),
                val -> val != NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("!isNull(longCol)"),
                val -> val != NULL_LONG);
        validateLongFilter(
                source,
                "longCol",
                RawString.of("longCol not in null"),
                val -> val != NULL_LONG);
        validateFloatFilter(
                source,
                "floatCol",
                Filter.isNotNull(ColumnName.of("floatCol")),
                val -> val != NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol != null"),
                val -> val != NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("!isNull(floatCol)"),
                val -> val != NULL_FLOAT);
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol not in null"),
                val -> val != NULL_FLOAT);
        validateDoubleFilter(
                source,
                "doubleCol",
                Filter.isNotNull(ColumnName.of("doubleCol")),
                val -> val != NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol != null"),
                val -> val != NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("!isNull(doubleCol)"),
                val -> val != NULL_DOUBLE);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol not in null"),
                val -> val != NULL_DOUBLE);
    }

    @Test
    public void testQueryScopeMatching() {
        final Table source = getStaticTable();
        QueryScope.addParam("float_nan", Float.NaN);
        QueryScope.addParam("double_nan", Double.NaN);

        // Testing == scoped_nan, nothing should match (including NaN)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol == float_nan"),
                val -> false);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol == double_nan"),
                val -> false);

        // Testing != scoped_nan, everything should match (including NaN)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol != float_nan"),
                val -> true);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol != double_nan"),
                val -> true);

        // Testing scoped_nan == scoped_nan, nothing should match (including NaN)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("float_nan == float_nan"),
                val -> false);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("double_nan == double_nan"),
                val -> false);

        // Testing scoped_nan != scoped_nan, everything should match (including NaN)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("float_nan != float_nan"),
                val -> true);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("double_nan != double_nan"),
                val -> true);

        // Testing in 0,scoped_nan (NaN will match)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol in 0, float_nan"),
                val -> Float.isNaN(val) || val == 0.0f);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol in 0, double_nan"),
                val -> Double.isNaN(val) || val == 0.0);

        // Testing not in 0,scoped_nan (NaN not match)
        validateFloatFilter(
                source,
                "floatCol",
                RawString.of("floatCol not in 0, float_nan"),
                val -> !Float.isNaN(val) && val != 0.0f);
        validateDoubleFilter(
                source,
                "doubleCol",
                RawString.of("doubleCol not in 0, double_nan"),
                val -> !Double.isNaN(val) && val != 0.0);
    }

    @Test
    public void testColumnComparisons() {
        final Table source = getStaticTable();

        final Table updated = source.update("floatCol2 = floatCol", "doubleCol2 = doubleCol");

        // Direct equality follows IEEE rules, NaN != NaN so this excludes the NaN row
        validateFloatFilter(
                updated,
                "floatCol",
                RawString.of("floatCol == floatCol2"),
                val -> !Float.isNaN(val));
        validateDoubleFilter(
                updated,
                "doubleCol",
                RawString.of("doubleCol == doubleCol2"),
                val -> !Double.isNaN(val));

        // Reverse the columnns
        validateFloatFilter(
                updated,
                "floatCol",
                RawString.of("floatCol2 == floatCol"),
                val -> !Float.isNaN(val));
        validateDoubleFilter(
                updated,
                "doubleCol",
                RawString.of("doubleCol2 == doubleCol"),
                val -> !Double.isNaN(val));

        // NaN != NaN -> true so this only includes the NaN row
        validateFloatFilter(
                updated,
                "floatCol",
                RawString.of("floatCol != floatCol2"),
                val -> Float.isNaN(val));
        validateDoubleFilter(
                updated,
                "doubleCol",
                RawString.of("doubleCol != doubleCol2"),
                val -> Double.isNaN(val));

        // Reverse the columnns
        validateFloatFilter(
                updated,
                "floatCol",
                RawString.of("floatCol2 != floatCol"),
                val -> Float.isNaN(val));
        validateDoubleFilter(
                updated,
                "doubleCol",
                RawString.of("doubleCol2 != doubleCol"),
                val -> Double.isNaN(val));

        // Compare float to double
        validateFloatFilter(
                updated,
                "floatCol",
                RawString.of("floatCol == doubleCol"),
                val -> !Float.isNaN(val));
        validateDoubleFilter(
                updated,
                "doubleCol",
                RawString.of("doubleCol == floatCol"),
                val -> !Double.isNaN(val));
    }
}
