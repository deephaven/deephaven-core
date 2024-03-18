//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.agg.Aggregation;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * @see io.deephaven.api.TableOperations#aggBy(Collection, Collection)
 */
@Immutable
@NodeStyle
public abstract class AggregateTable extends ByTableBase {

    public static Builder builder() {
        return ImmutableAggregateTable.builder();
    }

    public abstract List<Aggregation> aggregations();

    @Default
    public boolean preserveEmpty() {
        return AGG_BY_PRESERVE_EMPTY_DEFAULT;
    }

    public abstract Optional<TableSpec> initialGroups();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
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

    public interface Builder extends ByTableBase.Builder<AggregateTable, Builder> {
        Builder addAggregations(Aggregation element);

        Builder addAggregations(Aggregation... elements);

        Builder addAllAggregations(Iterable<? extends Aggregation> elements);

        Builder preserveEmpty(boolean preserveEmpty);

        Builder initialGroups(TableSpec initialGroups);

        Builder initialGroups(
                @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<? extends TableSpec> initialGroups);
    }
}
