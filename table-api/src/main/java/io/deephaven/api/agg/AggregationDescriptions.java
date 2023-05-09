/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A visitor to describe the input and aggregation {@link Pair column name pairs} for {@link Aggregation aggregations}.
 */
public final class AggregationDescriptions implements Aggregation.Visitor {

    public static Map<String, String> of(Aggregation aggregation) {
        return aggregation.walk(new AggregationDescriptions()).getOut();
    }

    public static Map<String, String> of(Collection<? extends Aggregation> aggregations) {
        final AggregationDescriptions descriptions = new AggregationDescriptions();
        aggregations.forEach(a -> a.walk(descriptions));
        return descriptions.getOut();
    }

    public static Map<String, String> of(Stream<? extends Aggregation> aggregations) {
        final AggregationDescriptions descriptions = new AggregationDescriptions();
        aggregations.forEach(a -> a.walk(descriptions));
        return descriptions.getOut();
    }

    private final Map<String, String> out = new LinkedHashMap<>();

    private AggregationDescriptions() {}

    private Map<String, String> getOut() {
        return out;
    }

    @Override
    public void visit(Aggregations aggregations) {
        aggregations.aggregations().forEach(a -> a.walk(this));
    }

    @Override
    public void visit(ColumnAggregation columnAgg) {
        visitColumnAgg(columnAgg.pair(), columnAgg.spec().description());
    }

    @Override
    public void visit(ColumnAggregations columnAggs) {
        final String specDescription = columnAggs.spec().description();
        columnAggs.pairs().forEach(p -> visitColumnAgg(p, specDescription));
    }

    private void visitColumnAgg(Pair pair, String specDescription) {
        out.put(pair.output().name(), pair.input().name() + " aggregated with " + specDescription);
    }

    @Override
    public void visit(Count count) {
        out.put(count.column().name(), "count");
    }

    @Override
    public void visit(FirstRowKey firstRowKey) {
        out.put(firstRowKey.column().name(), "first row key");
    }

    @Override
    public void visit(LastRowKey lastRowKey) {
        out.put(lastRowKey.column().name(), "last row key");
    }

    @Override
    public void visit(Partition partition) {
        out.put(partition.column().name(), "partition sub-table"
                + (partition.includeGroupByColumns() ? " (including group-by columns)" : ""));
    }
}
