//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

/**
 * Base class for {@link PushdownFilterContext} to help with execution cost tracking.
 */
public abstract class BasePushdownFilterContext implements PushdownFilterContext {
    protected long executedFilterCost;

    protected BasePushdownFilterContext() {
        executedFilterCost = 0;
    }

    @Override
    public long executedFilterCost() {
        return executedFilterCost;
    }

    @Override
    public void updateExecutedFilterCost(long executedFilterCost) {
        this.executedFilterCost = executedFilterCost;
    }

    @Override
    public void close() {
        // No-op
    }
}
