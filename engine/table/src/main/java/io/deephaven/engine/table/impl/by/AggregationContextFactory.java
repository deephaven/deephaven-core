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
     * @param groupByColumns The key column names
     * @return A new or safely reusable {@link AggregationContext}
     */
    AggregationContext makeAggregationContext(@NotNull Table table, @NotNull String... groupByColumns);
}
