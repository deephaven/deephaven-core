/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import org.immutables.value.Value.Immutable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Immutable
@NodeStyle
public abstract class AggregateAllByTable extends ByTableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableAggregateAllByTable.builder();
    }

    /**
     * Computes the single-aggregation from the agg-all implied by the {@code spec} and {@code groupByColumns} by
     * removing the {@code groupByColumns} and any extra columns implied by the {@code spec}.
     *
     * @param spec the spec
     * @param groupByColumns the group by columns
     * @param tableColumns the table columns
     * @return the aggregation, if non-empty
     */
    public static Optional<Aggregation> singleAggregation(
            AggSpec spec, Collection<? extends ColumnName> groupByColumns,
            Collection<? extends ColumnName> tableColumns) {
        Set<ColumnName> exclusions = AggAllByExclusions.of(spec, groupByColumns);
        List<ColumnName> columnsToAgg = new ArrayList<>(tableColumns.size());
        for (ColumnName column : tableColumns) {
            if (exclusions.contains(column)) {
                continue;
            }
            columnsToAgg.add(column);
        }
        return columnsToAgg.isEmpty() ? Optional.empty() : Optional.of(spec.aggregation(columnsToAgg));
    }

    public abstract AggSpec spec();

    /**
     * Transform {@code this} agg-all-by table into an {@link AggregationTable} by constructing the necessary
     * {@link Aggregation} from the {@link #spec()} and {@code tableColumns}.
     *
     * @param tableColumns the table columns
     * @return the aggregation table
     * @see #singleAggregation(AggSpec, Collection, Collection)
     */
    public final AggregationTable asAggregation(Collection<? extends ColumnName> tableColumns) {
        AggregationTable.Builder builder = AggregationTable.builder()
                .parent(parent())
                .addAllGroupByColumns(groupByColumns());
        singleAggregation(spec(), groupByColumns(), tableColumns).ifPresent(builder::addAggregations);
        return builder.build();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends ByTableBase.Builder<AggregateAllByTable, Builder> {
        Builder spec(AggSpec spec);
    }
}
