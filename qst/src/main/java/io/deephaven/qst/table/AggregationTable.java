/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.agg.Aggregation;
import org.immutables.value.Value;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * @see io.deephaven.api.TableOperations#aggBy(Collection, Collection)
 */
@Immutable
@NodeStyle
public abstract class AggregationTable extends ByTableBase {

    public static Builder builder() {
        return ImmutableAggregationTable.builder();
    }

    public abstract List<Aggregation> aggregations();

    @Value.Default
    public boolean preserveEmpty() {
        return false;
    }

    public abstract Optional<TableSpec> initialGroups();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkNumAggs() {
        if (aggregations().isEmpty()) {
            throw new IllegalArgumentException("Aggregations must not be empty");
        }
    }

    @Check
    final void checkInitialGroups() {
        if (groupByColumns().isEmpty() && initialGroups().isPresent()) {
            throw new IllegalArgumentException("InitialGroups must not be set if GroupByColumns is empty");
        }
    }


    public interface Builder extends ByTableBase.Builder<AggregationTable, Builder> {
        Builder addAggregations(Aggregation element);

        Builder addAggregations(Aggregation... elements);

        Builder addAllAggregations(Iterable<? extends Aggregation> elements);

        Builder preserveEmpty(boolean preserveEmpty);

        Builder initialGroups(TableSpec initialGroups);

        Builder initialGroups(
                @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<? extends TableSpec> initialGroups);
    }
}
