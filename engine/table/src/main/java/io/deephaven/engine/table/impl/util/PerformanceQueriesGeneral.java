/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.Table;
import io.deephaven.util.QueryConstants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static io.deephaven.api.agg.Aggregation.AggFirst;
import static io.deephaven.api.agg.Aggregation.AggMax;
import static io.deephaven.api.agg.Aggregation.AggPct;
import static io.deephaven.api.agg.Aggregation.AggSum;

/**
 * Generalizes {@link PerformanceQueries} to accept table parameters and make evaluation number parameter optional.
 */
public class PerformanceQueriesGeneral {
    private static boolean formatPctColumns = true;

    public static Table queryPerformance(Table queryPerformanceLog, final long evaluationNumber) {

        if (evaluationNumber != QueryConstants.NULL_LONG) {
            queryPerformanceLog = queryPerformanceLog.where(whereConditionForEvaluationNumber(evaluationNumber));
        }

        final long workerHeapSizeBytes = getWorkerHeapSizeBytes();
        queryPerformanceLog = queryPerformanceLog
                .updateView(
                        "WorkerHeapSize = " + workerHeapSizeBytes + "L",
                        "TimeSecs = nanosToMillis(EndTime - StartTime) / 1000d", // How long this query ran for, in
                                                                                 // seconds
                        "NetMemoryChange = FreeMemoryChange - TotalMemoryChange",
                        "QueryMemUsed = TotalMemory - FreeMemory", // Memory in use by the query. (Only
                                                                   // includes active heap memory.)
                        "QueryMemUsedPct = QueryMemUsed / WorkerHeapSize", // Memory usage as a percenage of max heap
                                                                           // size (-Xmx)
                        "QueryMemFree = WorkerHeapSize - QueryMemUsed" // Remaining memory until the query runs into the
                                                                       // max heap size
                )
                .moveColumnsUp(
                        "ProcessUniqueId", "EvaluationNumber",
                        "QueryMemUsed", "QueryMemFree", "QueryMemUsedPct",
                        "EndTime", "TimeSecs", "NetMemoryChange");
        if (formatPctColumns) {
            queryPerformanceLog = formatColumnsAsPct(queryPerformanceLog, "QueryMemUsedPct");
        }
        return queryPerformanceLog;
    }

    public static Table queryPerformance(Table queryPerformanceLog) {
        return queryPerformance(queryPerformanceLog, QueryConstants.NULL_LONG);
    }

    public static Table queryOperationPerformance(Table queryOps, final long evaluationNumber) {
        if (evaluationNumber != QueryConstants.NULL_LONG) {
            queryOps = queryOps.where(whereConditionForEvaluationNumber(evaluationNumber));
        }

        return queryOps
                .updateView(
                        "TimeSecs = nanosToMillis(EndTime - StartTime) / 1000d",
                        "NetMemoryChange = FreeMemoryChange - TotalMemoryChange" // Change in memory usage delta while
                                                                                 // this query was executing
                )
                .moveColumnsUp(
                        "ProcessUniqueId", "EvaluationNumber", "OperationNumber",
                        "EndTime", "TimeSecs", "NetMemoryChange");
    }

    public static Table queryOperationPerformance(final Table queryOps) {
        return queryOperationPerformance(queryOps, QueryConstants.NULL_LONG);
    }

    public static String processInfo(Table processInfo, final String processInfoId, final String type, final String key) {
        processInfo = processInfo
                .where("Id = `" + processInfoId + "`", "Type = `" + type + "`", "Key = `" + key + "`")
                .select("Value");
        try {
            return (String) processInfo.getColumn(0).get(0);
        } catch (Exception e) {
            return null;
        }
    }

    public static Table queryUpdatePerformance(Table queryUpdatePerformance, final long evaluationNumber, boolean formatPctColumnsLocal) {
        if (evaluationNumber != QueryConstants.NULL_LONG) {
            queryUpdatePerformance = queryUpdatePerformance.where(whereConditionForEvaluationNumber(evaluationNumber));
        }

        final long workerHeapSizeBytes = getWorkerHeapSizeBytes();
        queryUpdatePerformance = queryUpdatePerformance
                .updateView(
                        "WorkerHeapSize = " + workerHeapSizeBytes + "L",
                        "Ratio = EntryIntervalUsage / IntervalDurationNanos", // % of time during this interval that the
                                                                              // operation was using CPU
                        "QueryMemUsed = MaxTotalMemory - MinFreeMemory", // Memory in use by the query. (Only
                                                                         // includes active heap memory.)
                        "QueryMemUsedPct = QueryMemUsed / WorkerHeapSize", // Memory usage as a percenage of the max
                                                                           // heap size (-Xmx)
                        "QueryMemFree = WorkerHeapSize - QueryMemUsed", // Remaining memory until the query runs into
                                                                        // the max heap size
                        "NRows = EntryIntervalAdded + EntryIntervalRemoved + EntryIntervalModified", // Total number of
                                                                                                     // changed rows
                        "RowsPerSec = round(NRows / IntervalDurationNanos * 1.0e9)", // Average rate data is ticking at
                        "RowsPerCPUSec = round(NRows / EntryIntervalUsage * 1.0e9)" // Approximation of how fast CPU
                                                                                    // handles row changes
                )
                .moveColumnsUp(
                        "ProcessUniqueId", "EvaluationNumber", "OperationNumber",
                        "Ratio", "QueryMemUsed", "QueryMemUsedPct", "IntervalEndTime",
                        "RowsPerSec", "RowsPerCPUSec", "EntryDescription");
        if (formatPctColumnsLocal && formatPctColumns) {
            queryUpdatePerformance = formatColumnsAsPct(queryUpdatePerformance, "Ratio", "QueryMemUsedPct");
        }
        return queryUpdatePerformance;
    }

    public static Table queryUpdatePerformance(Table queryUpdatePerformance) {
        return queryUpdatePerformance(queryUpdatePerformance, QueryConstants.NULL_LONG, true);
    }

    public static Map<String, Table> queryUpdatePerformanceMap(final Table queryUpdatePerformance, final long evaluationNumber) {
        final Map<String, Table> resultMap = new HashMap<>();
        final Table qup = queryUpdatePerformance(queryUpdatePerformance, evaluationNumber, false);
        resultMap.put("QueryUpdatePerformance", qup);

        final Table worstInterval = qup
                .groupBy("IntervalStartTime", "IntervalDurationNanos")
                .sort("IntervalDurationNanos")
                .tail(1)
                .ungroup()
                .view("IntervalStartTime",
                        "IntervalEndTime",
                        "EntryId",
                        "EntryDescription",
                        "IntervalDurationNanos",
                        "Ratio",
                        "EntryIntervalUsage",
                        "EntryIntervalAdded",
                        "EntryIntervalRemoved",
                        "EntryIntervalModified",
                        "NRows");

        resultMap.put("WorstInterval", worstInterval);

        // Create a table showing the 'worst' updates, i.e. the operations with the greatest 'Ratio'
        final Table updateWorst = qup.sortDescending("Ratio");
        resultMap.put("UpdateWorst", updateWorst);

        // Create a table with updates from the most recent performance recording. interval at the top. (Within each
        // interval, operations are still sorted with the greatest Ratio at the top.)

        final Table updateMostRecent = updateWorst.sortDescending("IntervalEndTime").moveColumnsUp("IntervalEndTime");
        resultMap.put("UpdateMostRecent", updateMostRecent);


        // Create a table that summarizes the update performance data within each interval
        Table updateAggregate = qup.aggBy(
                Arrays.asList(
                        AggSum("NRows", "EntryIntervalUsage"),
                        AggFirst("QueryMemUsed", "WorkerHeapSize", "QueryMemUsedPct", "IntervalDurationNanos")),
                "IntervalStartTime", "IntervalEndTime", "ProcessUniqueId")
                .updateView("Ratio = EntryIntervalUsage / IntervalDurationNanos")
                .moveColumnsUp("IntervalStartTime", "IntervalEndTime", "Ratio");
        if (formatPctColumns) {
            updateAggregate = formatColumnsAsPct(updateAggregate, "Ratio", "QueryMemUsedPct");
        }
        resultMap.put("UpdateAggregate", updateAggregate);


        final Table updateSummaryStats = updateAggregate.aggBy(Arrays.asList(
                AggPct(0.99, "Ratio_99_Percentile = Ratio", "QueryMemUsedPct_99_Percentile = QueryMemUsedPct"),
                AggPct(0.90, "Ratio_90_Percentile = Ratio", "QueryMemUsedPct_90_Percentile = QueryMemUsedPct"),
                AggPct(0.75, "Ratio_75_Percentile = Ratio", "QueryMemUsedPct_75_Percentile = QueryMemUsedPct"),
                AggPct(0.50, "Ratio_50_Percentile = Ratio", "QueryMemUsedPct_50_Percentile = QueryMemUsedPct"),
                AggMax("Ratio_Max = Ratio", "QueryMemUsedPct_Max = QueryMemUsedPct")));

        resultMap.put("UpdateSummaryStats", updateSummaryStats);
        return resultMap;
    }

    public static Map<String, Table> queryUpdatePerformanceMap(final Table queryUpdatePerformance) {
        return queryUpdatePerformanceMap(queryUpdatePerformance, QueryConstants.NULL_LONG);
    }

    public static float approxRatio(final long v0, final long v1) {
        if (v1 == 0 || v0 == QueryConstants.NULL_LONG || v1 == QueryConstants.NULL_LONG) {
            return QueryConstants.NULL_FLOAT;
        }
        final float pct = v0 / (float) v1;
        // The samples are not perfect; let's not confuse our users.
        return Math.min(pct, 1.0F);
    }

    public static Table serverState(Table pml) {
        final long maxMemoryBytes = RuntimeMemory.getInstance().maxMemory();
        final int maxMemoryMiB = (int) Math.ceil(maxMemoryBytes / (1024 * 1024.0));
        pml = pml.updateView("MaxMemMiB = " + maxMemoryMiB);
        Table pm = pml.view(
                "IntervalStart = IntervalStartTime",
                "IntervalSecs = IntervalDurationMicros / (1000 * 1000.0)",
                "UsedMemMiB = TotalMemoryMiB - FreeMemoryMiB",
                "AvailMemMiB = MaxMemMiB - TotalMemoryMiB + FreeMemoryMiB",
                "MaxMemMiB",
                "AvailMemRatio = AvailMemMiB/MaxMemMiB",
                "GcTimeRatio = io.deephaven.engine.table.impl.util.PerformanceQueriesGeneral.approxRatio(IntervalCollectionTimeMicros, IntervalDurationMicros)",
                "UGPCycles = count(IntervalUGPCyclesTimeMicros)",
                "UGPOnBudgetRatio = io.deephaven.engine.table.impl.util.PerformanceQueriesGeneral.approxRatio(IntervalUGPCyclesOnBudget, IntervalUGPCyclesTimeMicros.length)",
                "UGPCycleMaxSecs = max(IntervalUGPCyclesTimeMicros) / (1000 * 1000.0)",
                "UGPCycleMedianSecs = median(IntervalUGPCyclesTimeMicros) / (1000 * 1000.0)",
                "UGPCycleMeanSecs = avg(IntervalUGPCyclesTimeMicros) / (1000 * 1000.0)",
                "UGPCycleP90Secs = percentile(0.9, IntervalUGPCyclesTimeMicros) / (1000 * 1000.0)",
                "UGPTimeRatio = io.deephaven.engine.table.impl.util.PerformanceQueriesGeneral.approxRatio(sum(IntervalUGPCyclesTimeMicros), IntervalDurationMicros)",
                "UGPSafePointTimeRatio = io.deephaven.engine.table.impl.util.PerformanceQueriesGeneral.approxRatio(IntervalUGPCyclesSafePointTimeMicros, IntervalDurationMicros)");
        pm = pm.formatColumns(
                "AvailMemRatio=Decimal(`#0.0%`)",
                "AvailMemRatio=(AvailMemRatio < 0.05) ? PALE_RED : " +
                        "((AvailMemRatio < 0.10) ? PALE_REDPURPLE : " +
                        "((AvailMemRatio < 0.20) ? PALE_PURPLE : NO_FORMATTING))",
                "UGPOnBudgetRatio=Decimal(`#0.0%`)",
                "UGPTimeRatio=Decimal(`#0.0%`)",
                "UGPSafePointTimeRatio=Decimal(`#0.0%`)",
                "GcTimeRatio=Decimal(`#0.0%`)",
                "GcTimeRatio=(GcTimeRatio >= 0.75) ? PALE_RED : " +
                        "((GcTimeRatio >= 0.50) ? PALE_REDPURPLE : " +
                        "((GcTimeRatio > 0.05) ? PALE_PURPLE : NO_FORMATTING))",
                "UGPOnBudgetRatio=(UGPOnBudgetRatio < 0.05) ? PALE_RED : " +
                        "((UGPOnBudgetRatio < 0.10) ? PALE_REDPURPLE : " +
                        "((UGPOnBudgetRatio < 0.20) ? PALE_PURPLE : NO_FORMATTING))",
                "UGPTimeRatio=(UGPTimeRatio >= 0.99) ? PALE_RED : " +
                        "((UGPTimeRatio >= 0.95) ? PALE_REDPURPLE : " +
                        "((UGPTimeRatio > 0.90) ? PALE_PURPLE : NO_FORMATTING))",
                "UGPSafePointTimeRatio=(UGPSafePointTimeRatio >= 0.75) ? PALE_RED : " +
                        "((UGPSafePointTimeRatio >= 0.50) ? PALE_REDPURPLE : " +
                        "((UGPSafePointTimeRatio > 0.05) ? PALE_PURPLE : NO_FORMATTING))",
                "IntervalSecs=Decimal(`#0.000`)");
        return pm;
    }

    private static Table formatColumnsAsPct(final Table t, final String... cols) {
        final String[] formats = new String[cols.length];
        for (int i = 0; i < cols.length; ++i) {
            formats[i] = cols[i] + "=Decimal(`#0.0%`)";
        }
        return t.formatColumns(formats);
    }

    private static long getWorkerHeapSizeBytes() {
        final OptionalLong opt = EngineMetrics.getProcessInfo().getMemoryInfo().heap().max();
        return opt.orElse(0);
    }

    private static String whereConditionForEvaluationNumber(final long evaluationNumber) {
        return "EvaluationNumber = " + evaluationNumber + "";
    }
}
