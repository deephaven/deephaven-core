//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * Applies an {@link InstantRangeFilter} or an Instant-typed {@link MatchFilter} to one row group's {@code min}/{@code
 * max} statistics, answering whether that row group could hold a matching row.
 * <p>
 * Values are compared as epoch nanoseconds, delegating the interval arithmetic to {@link LongPushdownHandler}.
 * {@link #maybeCreateEvaluator} resolves a match filter once -- converting its values to nanoseconds -- and returns an
 * evaluator to apply to each row group in turn.
 *
 * <h2>Nulls</h2>
 *
 * Of the two sources of a Deephaven null that {@link StatisticsEvaluator} describes, only the <b>stored sentinel</b> is
 * this class's business. Instant behaves like the primitives rather than like the other object types: its null is the
 * {@code NULL_LONG} sentinel in the underlying long, so a stored value equal to it reads back as null while Parquet
 * counts no null at all. That needs no special machinery -- the sentinel is left among the filter's values and tested
 * against {@code min}/{@code max} like any other, a null {@link Instant} converting to exactly that sentinel through
 * {@link DateTimeUtils#epochNanos(Instant)}.
 * <p>
 * <b>These methods are not correct in isolation</b> for a filter that a null row satisfies; reach them through
 * {@code StatisticsEvaluator.makeForFilter}, which accounts for those rows.
 */
final class InstantPushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. This is the
     * entry point {@link StatisticsEvaluator} dispatches through; the typed overloads below skip the column-type test,
     * since choosing this handler already asserts the type.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (filter instanceof final InstantRangeFilter instantRangeFilter) {
            return maybeCreateEvaluator(instantRangeFilter);
        }
        if (filter instanceof final MatchFilter matchFilter) {
            if (matchFilter.getColumnType() == Instant.class) {
                return maybeCreateEvaluator(matchFilter);
            }
        }
        return null;
    }

    /**
     * Reads this row group's extremes as {@code {min, max}} epoch nanoseconds, or returns {@code null} if the
     * statistics cannot be used.
     */
    @Nullable
    private static long[] decodeMinMaxNanos(@NotNull final Statistics<?> statistics) {
        final long[] minMax = new long[2];
        if (!MinMaxFromStatistics.getMinMaxForInstants(statistics,
                v -> minMax[0] = DateTimeUtils.epochNanos(v),
                v -> minMax[1] = DateTimeUtils.epochNanos(v))) {
            // Statistics could not be processed.
            return null;
        }
        return minMax;
    }

    /**
     * Prepares the range filter for evaluation: whether the statistics range intersects the range it defines. Which
     * shape of range it is settled here, once, rather than re-tested for every row group.
     */
    static StatisticsEvaluator maybeCreateEvaluator(final InstantRangeFilter instantRangeFilter) {
        final long dhLower = instantRangeFilter.getLower();
        final long dhUpper = instantRangeFilter.getUpper();
        final boolean lowerInclusive = instantRangeFilter.isLowerInclusive();
        final boolean upperInclusive = instantRangeFilter.isUpperInclusive();
        // A null lower bound needs no handling of its own: NULL_LONG is the bottom of the raw domain, so held
        // inclusively [NULL_LONG, upper] already means "every timestamp up to upper, the stored sentinel included",
        // and held exclusively ({@code X > null}) it means the same less the sentinel.
        return statistics -> {
            final long[] minMax = decodeMinMaxNanos(statistics);
            return minMax == null || LongPushdownHandler.maybeOverlapsRangeImpl(
                    minMax[0], minMax[1],
                    dhLower, lowerInclusive,
                    dhUpper, upperInclusive);
        };
    }

    /**
     * Verifies that the statistics range intersects any point provided in the match filter. Regular and inverted
     * matches are entirely different walks, so which one applies is settled here rather than per row group.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getMatchOptions().inverted();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : StatisticsEvaluator.ALWAYS_NO_OVERLAP;
        }
        // Null deliberately stays among the values; see "Nulls" on this class.
        final long[] instantNanos = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (value != null && !(value instanceof Instant)) {
                // Not an Instant, so the statistics cannot place it.
                return StatisticsEvaluator.ALWAYS_MAYBE;
            }
            instantNanos[i] = DateTimeUtils.epochNanos((Instant) value);
        }
        if (invertMatch) {
            return statistics -> {
                final long[] minMax = decodeMinMaxNanos(statistics);
                return minMax == null || LongPushdownHandler.maybeMatchesInverse(minMax[0], minMax[1], instantNanos);
            };
        }
        return statistics -> {
            final long[] minMax = decodeMinMaxNanos(statistics);
            return minMax == null || LongPushdownHandler.maybeMatches(minMax[0], minMax[1], instantNanos);
        };
    }

}
