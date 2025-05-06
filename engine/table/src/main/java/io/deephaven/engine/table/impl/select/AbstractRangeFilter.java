//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.util.type.NumericTypeUtils;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A filter that determines if a column value is between an upper and lower bound (which each may either be inclusive or
 * exclusive).
 */
public abstract class AbstractRangeFilter extends WhereFilterImpl implements ExposesChunkFilter {
    private static final Pattern decimalPattern = Pattern.compile("(-)?\\d+(?:\\.((\\d+)0*)?)?");

    protected final String columnName;
    protected final boolean upperInclusive;
    protected final boolean lowerInclusive;

    /**
     * The chunkFilter can be applied to the columns native type.
     * <p>
     * In practice, this is for non-reinterpretable DateTimes.
     */
    ChunkFilter chunkFilter;
    /**
     * If the column can be reinterpreted to a long, then we should prefer to use the longFilter instead.
     * <p>
     * In practice, this is used for reinterpretable DateTimes.
     */
    ChunkFilter longFilter;

    AbstractRangeFilter(String columnName, boolean lowerInclusive, boolean upperInclusive) {
        this.columnName = columnName;
        this.upperInclusive = upperInclusive;
        this.lowerInclusive = lowerInclusive;
    }

    @Override
    public Optional<ChunkFilter> chunkFilter() {
        return Optional.of(chunkFilter);
    }

    public static WhereFilter makeBigDecimalRange(String columnName, String val) {
        final int precision = findPrecision(val);
        final BigDecimal parsed = new BigDecimal(val);
        final BigDecimal offset = BigDecimal.valueOf(1, precision);
        final boolean positiveOrZero = parsed.signum() >= 0;

        return new ComparableRangeFilter(columnName, parsed,
                positiveOrZero ? parsed.add(offset) : parsed.subtract(offset), positiveOrZero, !positiveOrZero);
    }

    static int findPrecision(String val) {
        final Matcher m = decimalPattern.matcher(val);
        if (m.matches()) {
            final String fractionalPart = m.group(2);
            return fractionalPart == null ? 0 : fractionalPart.length();
        }

        throw new NumberFormatException("The value " + val + " is not a double");
    }

    @Override
    public List<String> getColumns() {
        return Collections.singletonList(columnName);
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        final Optional<SortingOrder> orderForColumn = SortedColumnsAttribute.getOrderForColumn(table, columnName);
        if (orderForColumn.isPresent()) {
            // do binary search for value
            return binarySearch(selection, columnSource, usePrev, orderForColumn.get().isDescending());
        }
        if (longFilter != null && columnSource.allowsReinterpret(long.class)) {
            return ChunkFilter.applyChunkFilter(selection, columnSource.reinterpret(long.class), usePrev, longFilter);
        }
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilter);
    }

    abstract WritableRowSet binarySearch(
            @NotNull RowSet selection, @NotNull ColumnSource<?> columnSource, boolean usePrev, boolean reverse);

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}

    public static int compare(Object a, Object b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Arguments cannot be null");
        }

        // Convert Instant to long for comparison.
        if (a instanceof Instant) {
            a = DateTimeUtils.epochNanos((Instant) a);
        }

        if (b instanceof Instant) {
            b = DateTimeUtils.epochNanos((Instant) b);
        }

        if (NumericTypeUtils.isNumericOrChar(a.getClass()) && NumericTypeUtils.isNumericOrChar(b.getClass())) {
            return comparePrimitives(a, b);
        }

        return ObjectComparisons.compare(a, b);
    }

    public static int comparePrimitives(Object a, Object b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Arguments cannot be null");
        }

        // Special case for two longs to avoid double conversion.
        if (a instanceof Long && b instanceof Long) {
            long val1 = (long) a;
            long val2 = (long) b;
            return Long.compare(val1, val2);
        }

        // Convert to double for other comparisons.
        double val1, val2;

        if (a instanceof Character) {
            val1 = (double) (char) a;
        } else if (a instanceof Byte) {
            val1 = (double) (byte) a;
        } else if (a instanceof Short) {
            val1 = (double) (short) a;
        } else if (a instanceof Integer) {
            val1 = (double) (int) a;
        } else if (a instanceof Long) {
            val1 = (double) (long) a;
        } else if (a instanceof Float) {
            val1 = (double) (float) a;
        } else if (a instanceof Double) {
            val1 = (double) a;
        } else {
            throw new IllegalArgumentException("Unsupported type for first argument: " + a.getClass());
        }

        if (b instanceof Character) {
            val2 = (double) (char) b;
        } else if (b instanceof Byte) {
            val2 = (double) (byte) b;
        } else if (b instanceof Short) {
            val2 = (double) (short) b;
        } else if (b instanceof Integer) {
            val2 = (double) (int) b;
        } else if (b instanceof Long) {
            val2 = (double) (long) b;
        } else if (b instanceof Float) {
            val2 = (double) (float) b;
        } else if (b instanceof Double) {
            val2 = (double) b;
        } else {
            throw new IllegalArgumentException("Unsupported type for second argument: " + b.getClass());
        }

        return Double.compare(val1, val2);
    }

    /**
     * Returns true if the range filter overlaps with the given range.
     *
     * @param lower the lower value bound of the range
     * @param upper the upper value bound of the range
     * @param lowerInclusive whether the lower bound is inclusive
     * @param upperInclusive whether the upper bound is inclusive
     * @return {@code true} if the range filter overlaps with the given range, {@code false} otherwise
     */
    public abstract boolean overlaps(
            @NotNull final Object lower,
            @NotNull final Object upper,
            final boolean lowerInclusive,
            final boolean upperInclusive);

    /**
     * Returns true if the range filter overlaps with the given range (assumes the provided min/max values are
     * inclusive)
     * .
     * @param min the minimum value in the given range
     * @param max the maximum value in the given range
     * @return {@code true} if the range filter overlaps with the given range, {@code false} otherwise
     */
    public boolean overlaps(@NotNull final Object min, @NotNull final Object max) {
        return overlaps(min, max, true, true);
    }

    /**
     * Returns true if the given value is found within the range filter.
     *
     * @param value the value to check
     * @return {@code true} if the range filter matches the given value, {@code false} otherwise
     */
    public abstract boolean contains(@NotNull final Object value);
}
