//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.util.SafeCloseable;

/**
 * Helper class to manage filters and their associated execution cost.
 */
public interface PushdownFilterContext extends SafeCloseable {
    PushdownFilterContext NO_PUSHDOWN_CONTEXT = new PushdownFilterContext() {
        @Override
        public long executedFilterCost() {
            return Long.MAX_VALUE;
        }

        @Override
        public void updateExecutedFilterCost(long executedFilterCost) {
            // No-op
        }

        @Override
        public void close() {
            // No-op
        }
    };

    /**
     * Returns the maximum cost of the pushdown filter operation that has already been executed for this context.
     */
    long executedFilterCost();

    /**
     * Update the cost of the pushdown filter operations that have already been executed for this context.
     */
    void updateExecutedFilterCost(long executedFilterCost);
}
