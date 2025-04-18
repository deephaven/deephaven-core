//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
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
        return UpdatePerformanceTracker.getQueryTable();
    }

    /**
     * Return a table with update performance ancestor data.
     *
     * @return A table with update performance ancestor data.
     */
    @ScriptApi
    public static QueryTable updatePerformanceAncestorsLog() {
        return UpdatePerformanceTracker.getAncestorTable();
    }

    /**
     * Return a table with query performance data. Individual sub-operations in the query are referenced in
     * QueryOperationPerformanceLog.
     *
     * @return A table with query performance data.
     */
    @ScriptApi
    public static QueryTable queryPerformanceLog() {
        return EngineMetrics.getInstance().getQplLoggerQueryTable();
    }

    /**
     * Return a table with individual subquery performance data.
     *
     * @return A table with subquery performance data.
     */
    @ScriptApi
    public static QueryTable queryOperationPerformanceLog() {
        return EngineMetrics.getInstance().getQoplLoggerQueryTable();
    }

    /**
     * Return a table with metrics collected for the current engine process.
     *
     * @return A table with metrics for the current engine process.
     */
    @ScriptApi
    public static QueryTable processMetricsLog() {
        return EngineMetrics.getInstance().getProcessMetricsQueryTable();
    }

    /**
     * Return a static table with process information for the current engine process.
     *
     * @return A table with process information for the current engine process.
     */
    @ScriptApi
    public static QueryTable processInfoLog() {
        return EngineMetrics.getInstance().getProcessInfoQueryTable();
    }

    /**
     * Return a table with process memory utilization and garbage collection data.
     *
     * @return A table with memory and GC data.
     */
    @ScriptApi
    public static QueryTable serverStateLog() {
        return ServerStateTracker.getInstance().getQueryTable();
    }
}
