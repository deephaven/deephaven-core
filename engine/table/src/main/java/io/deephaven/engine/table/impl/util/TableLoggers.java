package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.util.annotations.ScriptApi;

/**
 * Accessors for Deephaven tables of instrumentation logs. These tables include query logs and performance logs
 */
public class TableLoggers {
    /**
     * Return a table with update performance data.
     *
     * @return A table with update performance data.
     */
    @ScriptApi
    public static QueryTable updatePerformanceLog() {
        return UpdatePerformanceTracker.getInstance().getQueryTable();
    }

    /**
     * Return a table with query performance data. Individual sub-operations in the query are referenced in
     * QueryOperationPerformanceLog.
     *
     * @return A table with query performance data.
     */
    @ScriptApi
    public static QueryTable queryPerformanceLog() {
        return MemoryTableLoggers.getInstance().getQplLoggerQueryTable();
    }

    /**
     * Return a table with individual subquery performance data.
     *
     * @return A table with subquery performance data.
     */
    @ScriptApi
    public static QueryTable queryOperationPerformanceLog() {
        return MemoryTableLoggers.getInstance().getQoplLoggerQueryTable();
    }

    /**
     * Return a table with metrics collected for the current engine process.
     *
     * @return A table with metrics fopr the current engine process.
     */
    @ScriptApi
    public static QueryTable processMetricsLog() {
        return MemoryTableLoggers.getInstance().getProcessMetricsQueryTable();
    }

    /**
     * Return a static table with process information for the current engine process.
     *
     * @return A table with process information for the current engine process.
     */
    @ScriptApi
    public static QueryTable processInfoLog() {
        return MemoryTableLoggers.getInstance().getProcessInfoQueryTable();
    }

    /**
     * Return a table with process memory utilization and garbage collection data.
     *
     * @return A table with memory and GC data.
     */
    @ScriptApi
    public static QueryTable processMemoryLog() {
        return ProcessMemoryTracker.getInstance().getQueryTable();
    }
}
