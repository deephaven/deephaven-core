/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Parameter;

/**
 * An {@link Aggregation} that provides a single output column with sub-tables of the input table for each aggregation
 * group in the result.
 * <p>
 * If the input table is refreshing, the sub-tables will be also. Note that the output column will never report
 * modifications, only additions or removals; users should listen to the sub-tables individually or as a merged result
 * in order to be notified of sub-table updates. This allows for fine-grained, parallelizable incremental updates, as
 * distinct from the result of a {@link io.deephaven.api.agg.spec.AggSpecGroup grouping} {@link ColumnAggregation column
 * aggregation}.
 */
@Value.Immutable
@BuildableStyle
public abstract class Partition implements Aggregation {

    public static Partition of(ColumnName name) {
        return ImmutablePartition.builder().column(name).build();
    }

    public static Partition of(String x) {
        return of(ColumnName.of(x));
    }

    public static Partition of(ColumnName name, boolean includeGroupByColumns) {
        return ImmutablePartition.builder().column(name).includeGroupByColumns(includeGroupByColumns).build();
    }

    public static Partition of(String x, boolean includeGroupByColumns) {
        return of(ColumnName.of(x), includeGroupByColumns);
    }

    @Parameter
    public abstract ColumnName column();

    /**
     * Whether group-by columns (sometimes referred to as "key" columns) should be included in the output sub-tables.
     *
     * @return Whether to include group-by columns in the output sub-tables
     */
    @Default
    public boolean includeGroupByColumns() {
        return true;
    }

    @Override
    public final <V extends Aggregation.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
