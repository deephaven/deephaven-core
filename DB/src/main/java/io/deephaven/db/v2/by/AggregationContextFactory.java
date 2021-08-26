package io.deephaven.db.v2.by;

import io.deephaven.db.tables.Table;
import org.jetbrains.annotations.NotNull;

/**
 * Produces an AggregationContext for aggregations given a table and the names of the group by columns.
 */
public interface AggregationContextFactory {

    /**
     * Should we allow substitution with a {@link KeyOnlyAggregationFactory} (e.g. selectDistinct) when there are only
     * key columns? Instances whose operators could have side effects or are already {@link KeyOnlyAggregationFactory}
     * should return false.
     *
     * @return Whether to allow a {@link KeyOnlyAggregationFactory} to be substituted for this when there are only key
     *         columns
     */
    default boolean allowKeyOnlySubstitution() {
        return false;
    }

    /**
     * Make an {@link AggregationContext} for this aggregation.
     *
     * @param table The source {@link Table} to aggregate
     * @param groupByColumns The key column names
     * @return A new or safely reusable {@link AggregationContext}
     */
    AggregationContext makeAggregationContext(@NotNull Table table, @NotNull String... groupByColumns);
}
