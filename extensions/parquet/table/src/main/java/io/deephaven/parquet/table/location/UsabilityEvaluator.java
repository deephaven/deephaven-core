//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

/**
 * Applies the statistics-usability check before delegating to another {@link StatisticsEvaluator}. Statistics that are
 * not {@link ParquetPushdownUtils#areStatisticsUsable usable} -- an all-null row group, absent extremes, a column order
 * this code does not interpret -- prove nothing about the row group, so it has to be kept.
 * <p>
 * This sits <b>outside</b> {@link NullAwareEvaluator}, so nothing downstream is handed statistics it may not read. That
 * is what lets each handler's {@code maybeOverlaps} take usable statistics as a precondition rather than re-checking,
 * and it keeps the null count -- readable even when {@code min}/{@code max} are not -- from being consulted for a row
 * group whose statistics were never approved.
 */
final class UsabilityEvaluator implements StatisticsEvaluator {

    /**
     * Wraps {@code handler}, unless it answers without reading statistics at all.
     * <p>
     * {@link StatisticsEvaluator#ALWAYS_MAYBE} and {@link StatisticsEvaluator#ALWAYS_NO_OVERLAP} are
     * statistics-independent by contract, so there is no precondition to enforce for either, and handing the singleton
     * straight back keeps it comparable by identity -- which is how {@code ParquetTableLocation} recognizes an answer
     * it can give without walking the row groups.
     */
    static StatisticsEvaluator maybeWrap(@NotNull final StatisticsEvaluator handler) {
        if (handler == StatisticsEvaluator.ALWAYS_MAYBE || handler == StatisticsEvaluator.ALWAYS_NO_OVERLAP) {
            return handler;
        }
        return new UsabilityEvaluator(handler);
    }

    private final StatisticsEvaluator handler;

    UsabilityEvaluator(@NotNull final StatisticsEvaluator handler) {
        this.handler = handler;
    }

    @Override
    public boolean maybeOverlaps(@NotNull final Statistics<?> statistics) {
        // Unusable statistics are no evidence at all, so the row group stays.
        return !ParquetPushdownUtils.areStatisticsUsable(statistics) || handler.maybeOverlaps(statistics);
    }

    @Override
    public String toString() {
        return "UsabilityEvaluator{" + handler + "}";
    }
}
