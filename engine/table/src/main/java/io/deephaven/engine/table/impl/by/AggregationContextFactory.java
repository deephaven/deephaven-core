/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

/**
 * Produces an AggregationContext for aggregations given a table and the names of the group by columns.
 */
@FunctionalInterface
public interface AggregationContextFactory {

    /**
     * Make an {@link AggregationContext} for this aggregation.
     *
     * @param table The source {@link Table} to aggregate
     * @param requireStateChangeRecorder Whether the resulting context is required to have an operator that extends
     *        {@link StateChangeRecorder}
     * @param groupByColumns The key column names
     * @return A new or safely reusable {@link AggregationContext}
     */
    AggregationContext makeAggregationContext(
            @NotNull Table table, boolean requireStateChangeRecorder, @NotNull String... groupByColumns);
}
