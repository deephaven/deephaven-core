package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value;

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
@SimpleStyle
public abstract class Partition implements Aggregation {

    public static Partition of(ColumnName name) {
        return ImmutablePartition.of(name);
    }

    public static Partition of(String x) {
        return of(ColumnName.of(x));
    }

    @Value.Parameter
    public abstract ColumnName column();

    @Override
    public final <V extends Aggregation.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
