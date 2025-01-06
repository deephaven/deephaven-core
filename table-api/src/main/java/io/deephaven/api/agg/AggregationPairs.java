//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.api.Pair;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A visitor to get the ordered input/output {@link Pair column name pairs} for {@link Aggregation aggregations}.
 * Aggregations with no inputs columns do not emit pairs.
 */
public class AggregationPairs implements Aggregation.Visitor {

    public static Stream<Pair> of(Aggregation aggregation) {
        return aggregation.walk(new AggregationPairs()).getOut();
    }

    public static Stream<Pair> of(Collection<? extends Aggregation> aggregations) {
        return aggregations.stream().flatMap(AggregationPairs::of);
    }

    protected Stream<Pair> out;

    protected Stream<Pair> getOut() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(Aggregations aggregations) {
        out = aggregations.aggregations().stream().flatMap(AggregationPairs::of);
    }

    @Override
    public void visit(ColumnAggregation columnAgg) {
        out = Stream.of(columnAgg.pair());
    }

    @Override
    public void visit(ColumnAggregations columnAggs) {
        out = columnAggs.pairs().stream();
    }

    @Override
    public void visit(Count count) {
        out = Stream.empty();
    }

    @Override
    public void visit(CountWhere countWhere) {
        out = Stream.empty();
    }

    @Override
    public void visit(FirstRowKey firstRowKey) {
        out = Stream.empty();
    }

    @Override
    public void visit(LastRowKey lastRowKey) {
        out = Stream.empty();
    }

    @Override
    public void visit(Partition partition) {
        out = Stream.empty();
    }

    @Override
    public void visit(Formula formula) {
        out = Stream.empty();
    }

}
