package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.QueryTable;
import io.deephaven.util.annotations.ScriptApi;

/**
 * Tools to obtain internal, Deephaven logs as tables. These tables include query logs and performance logs.
 */
public class TableLoggers {
    /**
     * Return a table with update performance data.
     *
     * Note this table will only tick if/since startUpdatePerformanceLog is called.
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
     * Start collecting data for query update performance.
     */
    @ScriptApi
    public static void startUpdatePerformanceLog() {
        UpdatePerformanceTracker.start();
    }
}
