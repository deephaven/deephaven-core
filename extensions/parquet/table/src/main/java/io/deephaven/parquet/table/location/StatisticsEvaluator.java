//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPushdownFilterContext;
import io.deephaven.engine.table.impl.select.*;

import java.util.List;
import java.util.function.Function;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * One filter, resolved against a column type, ready to be applied to a row group's statistics.
 * <p>
 * A filter is evaluated once per row group, but most of the work of applying one -- unboxing its values into a
 * primitive array, sorting them, encoding them, deciding whether the column type is even supported -- depends only on
 * the filter and not on the row group. Handlers do that work in their {@code maybeCreateEvaluator} and return an
 * instance of this, so it is not repeated for every row group in a file.
 *
 * <h2>The two sources of a Deephaven null</h2>
 *
 * A row can read back as null in Deephaven for two independent reasons, and an evaluator can exclude a row group only
 * once both are ruled out.
 * <ol>
 * <li>A <b>Parquet null</b>: a row absent from the definition levels and counted in {@code num_nulls}.
 * {@code min}/{@code max} describe non-null values only, so they say nothing whatever about these rows. The null count
 * is the only evidence about them, consulted by the gate in {@link #maybeMakeForFilter}.</li>
 * <li>A <b>stored sentinel</b>: for the types whose Deephaven null is a value rather than an absence -- every
 * primitive, and {@link java.time.Instant} -- a stored value that happens to equal that sentinel reads back as null. To
 * Parquet it is an ordinary value: covered by {@code min}/{@code max}, and counted in {@code num_nulls} not at all.
 * Deephaven's own writer never produces one, converting the sentinel to a Parquet null on the way out, but other
 * writers do.</li>
 * </ol>
 * The handlers own the second and only the second: it is just another value, so long as the sentinel is <b>not
 * removed</b> from a filter's values before they are tested against {@code min}/{@code max}. The first is owned here,
 * by the gate in {@link #maybeMakeForFilter}, which is why a handler called directly is not correct in isolation for a
 * filter that a null row satisfies. The handlers for {@link String} and the other object types have no sentinel
 * encoding and so face only the first route; they simply drop a null from the values.
 */
@FunctionalInterface
interface StatisticsEvaluator {

    /**
     * Whether the row group described by {@code statistics} may contain a row matching the filter. A {@code false}
     * answer excludes the row group; it must only be given when the statistics prove no row can match.
     *
     * @param statistics the row group's {@link ParquetPushdownUtils#areStatisticsUsable usable} statistics for the
     *        filtered column
     */
    boolean maybeOverlaps(@NotNull Statistics<?> statistics);

    /** Used when a filter cannot be answered from statistics at all; decided once, not per row group. */
    StatisticsEvaluator ALWAYS_MAYBE = statistics -> true;

    /**
     * Resolves {@code filter} to the handler for its column type and applies the null gate to it, or returns
     * {@code null} if no row group could ever be excluded -- because no handler serves the filter, or because the one
     * that does cannot bound it. A caller receiving {@code null} should skip the row groups entirely rather than ask
     * about each in turn, since the answer is already known to be "maybe".
     * <p>
     * Call this once per location and apply the result to each row group's statistics in turn.
     * <p>
     * {@code ctx} carries the authoritative answer to whether a null row satisfies the filter:
     * {@link BasePushdownFilterContext#filterNullBehavior()} is measured by running the filter against a null row, and
     * is computed once per filter and cached. It must be the context {@code filter} came from -- pass
     * {@link BasePushdownFilterContext#filterForMetadataFiltering()}.
     * <p>
     * The gate below is the only place Parquet nulls are considered. The handlers answer purely from {@code min}/
     * {@code max}, which describe non-null values, so they can neither see such a row nor rule one out. They remain
     * responsible for Deephaven's <i>other</i> null source -- a stored value equal to the null sentinel -- which is an
     * ordinary value as far as {@code min}/{@code max} are concerned.
     */
    @Nullable
    static StatisticsEvaluator maybeMakeForFilter(
            @NotNull final WhereFilter filter,
            @NotNull final RegionedPushdownFilterContext ctx) {
        final StatisticsEvaluator handler = resolveHandler(filter);
        if (handler == ALWAYS_MAYBE) {
            // Nothing here can bound this filter, so every row group would be evaluated then returned as maybe. Avoid
            // this by returning null.
            return null;
        }
        // A filter that a null row satisfies -- or that throws when it sees one -- may not exclude a row group unless
        // the statistics prove there are no null rows to lose.
        if (ctx.filterNullBehavior() == BasePushdownFilterContext.FilterNullBehavior.EXCLUDES_NULLS) {
            return handler;
        }
        return statistics -> !ParquetPushdownUtils.isProvenFreeOfNulls(statistics)
                || handler.maybeOverlaps(statistics);
    }

    /**
     * Every handler, in the order they are offered the filter. Each returns {@code null} for a filter it does not
     * serve.
     * <p>
     * Order is load-bearing wherever one handler's filter type is a subtype of another's, since a handler claims a
     * filter by {@code instanceof} and the first to claim it wins. {@link InstantRangeFilter} extends
     * {@link LongRangeFilter}, so {@code InstantPushdownHandler} must precede {@code LongPushdownHandler}: the long
     * handler would otherwise claim every Instant range filter and compare its epoch-nanosecond bounds against
     * statistics left in the file's own timestamp unit, excluding every row group of a millisecond- or
     * microsecond-stamped file. {@code StringPushdownHandler} must likewise precede {@code ComparablePushdownHandler},
     * which is last because it claims whatever the others did not.
     */
    List<Function<WhereFilter, StatisticsEvaluator>> HANDLERS = List.of(
            StringPushdownHandler::maybeCreateEvaluator,
            BytePushdownHandler::maybeCreateEvaluator,
            CharPushdownHandler::maybeCreateEvaluator,
            ShortPushdownHandler::maybeCreateEvaluator,
            IntPushdownHandler::maybeCreateEvaluator,
            InstantPushdownHandler::maybeCreateEvaluator,
            LongPushdownHandler::maybeCreateEvaluator,
            FloatPushdownHandler::maybeCreateEvaluator,
            DoublePushdownHandler::maybeCreateEvaluator,
            SingleSidedComparableRangePushdownHandler::maybeCreateEvaluator,
            ComparablePushdownHandler::maybeCreateEvaluator);

    /**
     * Offers {@code filter} to each handler in {@link #HANDLERS} order and returns the first evaluator claimed, or
     * {@link #ALWAYS_MAYBE} if none was. Visible to the package so tests can pin the resolution itself, which the
     * ordering above depends on; callers outside this file want {@link #maybeMakeForFilter}, which also applies the
     * null gate.
     */
    static StatisticsEvaluator resolveHandler(@NotNull final WhereFilter filter) {
        if (filter instanceof MatchFilter && ((MatchFilter) filter).getColumnType() == null) {
            throw new IllegalStateException("Filter not initialized with a column type: " + filter);
        }
        for (final Function<WhereFilter, StatisticsEvaluator> handler : HANDLERS) {
            final StatisticsEvaluator evaluator = handler.apply(filter);
            if (evaluator != null) {
                return evaluator;
            }
        }
        // No handler serves this filter, so nothing about it can be bounded by statistics.
        return ALWAYS_MAYBE;
    }
}
