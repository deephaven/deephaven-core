//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import java.util.Map;

/**
 * Base class for {@link PushdownFilterContext} to help with execution cost tracking.
 */
public abstract class BasePushdownFilterContext implements PushdownFilterContext {
    protected long executedFilterCost;

    public BasePushdownFilterContext() {
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

    /**
     * The mapping from filter column names to column names from the table definition if they differ. User should use
     * this mapping as {@code renameMap().getOrDefault(colNameFromFilter, colNameFromFilter)}
     */
    public abstract Map<String, String> renameMap();

    @Override
    public void close() {
        // No-op
    }
}
