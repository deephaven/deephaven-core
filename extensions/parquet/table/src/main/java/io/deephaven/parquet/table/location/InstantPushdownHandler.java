//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Arrays;

/**
 * Applies an {@link InstantRangeFilter} or an Instant-typed {@link MatchFilter} to one row group's {@code min}/{@code
 * max} statistics, answering whether that row group could hold a matching row.
 * <p>
 * Values are compared as epoch nanoseconds, delegating the interval arithmetic to {@link LongPushdownHandler}.
 * {@link #maybeCreateEvaluator} resolves a match filter once -- converting its values, and sorting them for the
 * inverted walk -- and returns an evaluator to apply to each row group in turn.
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
 * {@code StatisticsEvaluator.maybeMakeForFilter}, which gates on those rows.
 */
final class InstantPushdownHandler {

    /**
     * Resolves {@code filter} to an evaluator, or returns {@code null} if this handler does not serve it. This is the
     * entry point {@link StatisticsEvaluator} dispatches through; the typed overloads below skip the column-type test,
     * since choosing this handler already asserts the type.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (filter instanceof InstantRangeFilter) {
            return maybeCreateEvaluator((InstantRangeFilter) filter);
        }
        if (filter instanceof MatchFilter) {
            final MatchFilter matchFilter = (MatchFilter) filter;
            if (matchFilter.getColumnType() == Instant.class) {
                return maybeCreateEvaluator(matchFilter);
            }
        }
        return null;
    }

    /**
     * Prepares the range filter for evaluation: whether the statistics range intersects the range it defines.
     */
    static StatisticsEvaluator maybeCreateEvaluator(final InstantRangeFilter instantRangeFilter) {
        final long dhLower = instantRangeFilter.getLower();
        final long dhUpper = instantRangeFilter.getUpper();
        final boolean unboundedBelow = dhLower == QueryConstants.NULL_LONG;
        final boolean lowerInclusive = instantRangeFilter.isLowerInclusive();
        final boolean upperInclusive = instantRangeFilter.isUpperInclusive();
        return statistics -> {
            final MutableObject<Instant> mutableMin = new MutableObject<>();
            final MutableObject<Instant> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForInstants(statistics, mutableMin::setValue, mutableMax::setValue)) {
                // Statistics could not be processed, so assume that we overlap.
                return true;
            }
            final long min = DateTimeUtils.epochNanos(mutableMin.get());
            final long max = DateTimeUtils.epochNanos(mutableMax.get());
            if (unboundedBelow) {
                if (min <= QueryConstants.NULL_LONG && QueryConstants.NULL_LONG <= max) {
                    // A stored value equal to the sentinel reads back as null and so matches too. Unlike a Parquet
                    // null it is an ordinary value here, covered by min/max rather than by the null gate in
                    // maybeMakeForFilter.
                    return true;
                }
                // Filter is unbounded below; can only match if the upper bound is above this row group's minimum.
                return upperInclusive ? min <= dhUpper : min < dhUpper;
            }
            return LongPushdownHandler.maybeOverlapsRangeImpl(
                    min, max,
                    dhLower, lowerInclusive,
                    dhUpper, upperInclusive);
        };
    }

    /**
     * Verifies that the statistics range intersects any point provided in the match filter.
     */
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getMatchOptions().inverted();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : statistics -> false;
        }
        // Null deliberately stays among the values; see "Nulls" on this class.
        final long[] instantNanos = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (value != null && !(value instanceof Instant)) {
                // Not an Instant, so the statistics cannot place it.
                return StatisticsEvaluator.ALWAYS_MAYBE;
            }
            final long nanos = DateTimeUtils.epochNanos((Instant) value);
            instantNanos[i] = nanos;
        }
        if (invertMatch) {
            // The gap walk requires sorted values; sorted once here rather than per row group.
            Arrays.sort(instantNanos);
        }
        return statistics -> {
            final MutableObject<Instant> mutableMin = new MutableObject<>();
            final MutableObject<Instant> mutableMax = new MutableObject<>();
            if (!MinMaxFromStatistics.getMinMaxForInstants(statistics, mutableMin::setValue, mutableMax::setValue)) {
                // Statistics could not be processed, so assume that we overlap.
                return true;
            }
            final long min = DateTimeUtils.epochNanos(mutableMin.get());
            final long max = DateTimeUtils.epochNanos(mutableMax.get());
            return invertMatch
                    ? LongPushdownHandler.maybeMatchesInverse(min, max, instantNanos)
                    : LongPushdownHandler.maybeMatches(min, max, instantNanos);
        };
    }

}
