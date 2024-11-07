//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.rangejoin;

import io.deephaven.api.Strings;
import io.deephaven.api.agg.*;
import io.deephaven.api.agg.spec.*;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Tool for validating aggregation inputs to {@link Table#rangeJoin range join}.
 */
public class SupportedRangeJoinAggregations implements Aggregation.Visitor {

    /**
     * Validate {@code aggregations} for support by {@link Table#rangeJoin range join}.
     * 
     * @param description A description of the range join operation
     * @param aggregations The {@link Aggregation aggregations} to validate
     * @throws UnsupportedOperationException if any of the {@code aggregations} is unsupported by {@link Table#rangeJoin
     *         range join}
     */
    public static void validate(
            @NotNull final String description,
            @NotNull final Collection<? extends Aggregation> aggregations) {
        final SupportedRangeJoinAggregations visitor = new SupportedRangeJoinAggregations();
        final Collection<? extends Aggregation> unsupportedAggregations = aggregations.stream()
                .filter((final Aggregation agg) -> !visitor.isSupported(agg))
                .collect(Collectors.toList());
        if (!unsupportedAggregations.isEmpty()) {
            throw new UnsupportedOperationException(String.format(
                    "%s: rangeJoin only supports the \"group\" aggregation at this time - unsupported aggregations were requested: %s",
                    description,
                    Strings.ofAggregations(unsupportedAggregations)));
        }
    }

    private boolean hasUnsupportedArgs;

    private boolean isSupported(@NotNull final Aggregation aggregation) {
        aggregation.walk(this);
        return !hasUnsupportedArgs;
    }

    @Override
    public void visit(@NotNull final Aggregations aggregations) {
        aggregations.aggregations().forEach((final Aggregation agg) -> agg.walk(this));
    }

    @Override
    public void visit(@NotNull final ColumnAggregation columnAgg) {
        hasUnsupportedArgs |= !(columnAgg.spec() instanceof AggSpecGroup);
    }

    @Override
    public void visit(@NotNull final ColumnAggregations columnAggs) {
        hasUnsupportedArgs |= !(columnAggs.spec() instanceof AggSpecGroup);
    }

    @Override
    public void visit(@NotNull final Count count) {
        hasUnsupportedArgs = true;
    }

    @Override
    public void visit(@NotNull final FirstRowKey firstRowKey) {
        hasUnsupportedArgs = true;
    }

    @Override
    public void visit(@NotNull final LastRowKey lastRowKey) {
        hasUnsupportedArgs = true;
    }

    @Override
    public void visit(@NotNull final Partition partition) {
        hasUnsupportedArgs = true;
    }

    @Override
    public void visit(@NotNull final Formula formula) {
        hasUnsupportedArgs = true;
    }
}
