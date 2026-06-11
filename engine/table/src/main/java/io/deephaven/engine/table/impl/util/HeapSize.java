//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.util.QueryConstants;

/**
 * Get this process's maximum heap size for performance logs.
 */
public class HeapSize {
    /**
     * Get this process's maximum heap size in bytes, or {@link QueryConstants#NULL_LONG}.
     * 
     * @return this processes maximum heap size in bytes, or {@link QueryConstants#NULL_LONG}.
     */
    public static long getMaximumHeapSizeBytes() {
        return EngineMetrics.getProcessInfo().getMemoryInfo().heap().max().orElse(QueryConstants.NULL_LONG);
    }
}
