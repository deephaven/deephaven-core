//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPushdownFilterContext;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

/**
 * Applies null-checking before delegating to another {@link StatisticsEvaluator}. Parquet {@code min}/{@code max}
 * summarize non-null values only, so a row group's nulls are invisible to the delegate; this keeps such a row group
 * unless the null count proves there are none. See {@link StatisticsEvaluator} for the division of labour around nulls.
 */
final class NullAwareEvaluator implements StatisticsEvaluator {

    /**
     * Wraps {@code handler}, unless {@code ctx} shows the filter can never match a null row and so needs no check.
     * {@code ctx} must be the context the filter came from.
     */
    static StatisticsEvaluator maybeWrap(
            @NotNull final StatisticsEvaluator handler,
            @NotNull final RegionedPushdownFilterContext ctx) {
        if (ctx.filterNullBehavior() == BasePushdownFilterContext.FilterNullBehavior.EXCLUDES_NULLS) {
            // No null row can match, so none can be lost by excluding a row group.
            return handler;
        }
        return new NullAwareEvaluator(handler);
    }

    private final StatisticsEvaluator handler;

    NullAwareEvaluator(@NotNull final StatisticsEvaluator handler) {
        this.handler = handler;
    }

    @Override
    public boolean maybeOverlaps(@NotNull final Statistics<?> statistics) {
        // Absent proof that there are no nulls, one may be there and may match, whatever min/max say.
        return !ParquetPushdownUtils.isProvenFreeOfNulls(statistics) || handler.maybeOverlaps(statistics);
    }

    @Override
    public String toString() {
        return "NullAwareEvaluator{" + handler + "}";
    }
}
