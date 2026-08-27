//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.*;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * One filter, resolved against a column type, ready to be applied to a row group's statistics.
 * <p>
 * A filter is evaluated once per row group, but most of the work of applying one -- unboxing its values into a
 * primitive array, sorting them, encoding them, deciding whether the column type is even supported -- depends only on
 * the filter and not on the row group. Handlers do that work in their {@code maybeCreateEvaluator} and return an
 * instance of this, so it is not repeated for every row group in a file.
 */
@FunctionalInterface
interface StatisticsEvaluator {

    /**
     * Whether the row group described by {@code statistics} may contain a row matching the filter. A {@code false}
     * answer excludes the row group; it must only be given when the statistics prove no row can match.
     */
    boolean maybeOverlaps(@NotNull Statistics<?> statistics);

    /** Used when a filter cannot be answered from statistics at all; decided once, not per row group. */
    StatisticsEvaluator ALWAYS_MAYBE = statistics -> true;

    /**
     * Resolves {@code filter} to the handler for its column type. Call this once per filter and apply the result to
     * each row group's statistics in turn; a filter that no handler serves resolves to {@link #ALWAYS_MAYBE}.
     */
    static StatisticsEvaluator forFilter(@NotNull final WhereFilter filter) {
        final StatisticsEvaluator stringEvaluator = StringPushdownHandler.maybeCreateEvaluator(filter);
        if (stringEvaluator != null) {
            return stringEvaluator;
        }
        if (filter instanceof ByteRangeFilter) {
            final ByteRangeFilter f = (ByteRangeFilter) filter;
            return statistics -> BytePushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof CharRangeFilter) {
            final CharRangeFilter f = (CharRangeFilter) filter;
            return statistics -> CharPushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof ShortRangeFilter) {
            final ShortRangeFilter f = (ShortRangeFilter) filter;
            return statistics -> ShortPushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof IntRangeFilter) {
            final IntRangeFilter f = (IntRangeFilter) filter;
            return statistics -> IntPushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof InstantRangeFilter) {
            final InstantRangeFilter f = (InstantRangeFilter) filter;
            return statistics -> InstantPushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof LongRangeFilter) {
            final LongRangeFilter f = (LongRangeFilter) filter;
            return statistics -> LongPushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof FloatRangeFilter) {
            final FloatRangeFilter f = (FloatRangeFilter) filter;
            return statistics -> FloatPushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof DoubleRangeFilter) {
            final DoubleRangeFilter f = (DoubleRangeFilter) filter;
            return statistics -> DoublePushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof ComparableRangeFilter) {
            final ComparableRangeFilter f = (ComparableRangeFilter) filter;
            return statistics -> ComparablePushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof SingleSidedComparableRangeFilter) {
            final SingleSidedComparableRangeFilter f = (SingleSidedComparableRangeFilter) filter;
            return statistics -> SingleSidedComparableRangePushdownHandler.maybeOverlaps(f, statistics);
        }
        if (filter instanceof MatchFilter) {
            final MatchFilter matchFilter = (MatchFilter) filter;
            final Class<?> dhColumnType = matchFilter.getColumnType();
            if (dhColumnType == null) {
                throw new IllegalStateException("Filter not initialized with a column type: " + filter);
            } else if (dhColumnType == byte.class || dhColumnType == Byte.class) {
                return BytePushdownHandler.maybeCreateEvaluator(matchFilter);
            } else if (dhColumnType == char.class || dhColumnType == Character.class) {
                return CharPushdownHandler.maybeCreateEvaluator(matchFilter);
            } else if (dhColumnType == short.class || dhColumnType == Short.class) {
                return ShortPushdownHandler.maybeCreateEvaluator(matchFilter);
            } else if (dhColumnType == int.class || dhColumnType == Integer.class) {
                return IntPushdownHandler.maybeCreateEvaluator(matchFilter);
            } else if (dhColumnType == long.class || dhColumnType == Long.class) {
                return LongPushdownHandler.maybeCreateEvaluator(matchFilter);
            } else if (dhColumnType == float.class || dhColumnType == Float.class) {
                return FloatPushdownHandler.maybeCreateEvaluator(matchFilter);
            } else if (dhColumnType == double.class || dhColumnType == Double.class) {
                return DoublePushdownHandler.maybeCreateEvaluator(matchFilter);
            } else if (dhColumnType == String.class && matchFilter.getMatchOptions().caseInsensitive()) {
                // Case-insensitive matching is deliberately not pushed down; see the note on
                // StringPushdownHandler.maybeCreateEvaluator. Row group statistics cannot bound it.
                return StatisticsEvaluator.ALWAYS_MAYBE;
            } else if (dhColumnType == Instant.class) {
                return InstantPushdownHandler.maybeCreateEvaluator(matchFilter);
            } else {
                return ComparablePushdownHandler.maybeCreateEvaluator(matchFilter);
            }
        }
        // Unsupported filter type for push down, so assume it overlaps.
        return StatisticsEvaluator.ALWAYS_MAYBE;
    }
}
