//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A visitor to get the ordered output {@link ColumnName column names} for {@link Aggregation aggregations}.
 */
public class AggregationOutputs implements Aggregation.Visitor {

    public static Stream<ColumnName> of(Aggregation aggregation) {
        return aggregation.walk(new AggregationOutputs()).getOut();
    }

    public static Stream<ColumnName> of(Collection<? extends Aggregation> aggregations) {
        return aggregations.stream().flatMap(AggregationOutputs::of);
    }

    protected Stream<ColumnName> out;

    protected Stream<ColumnName> getOut() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(Aggregations aggregations) {
        out = aggregations.aggregations().stream().flatMap(AggregationOutputs::of);
    }

    @Override
    public void visit(ColumnAggregation columnAgg) {
        out = Stream.of(columnAgg.pair().output());
    }

    @Override
    public void visit(ColumnAggregations columnAggs) {
        out = columnAggs.pairs().stream().map(Pair::output);
    }

    @Override
    public void visit(Count count) {
        out = Stream.of(count.column());
    }

    @Override
    public void visit(CountWhere countWhere) {
        out = Stream.of(countWhere.column());
    }

    @Override
    public void visit(FirstRowKey firstRowKey) {
        out = Stream.of(firstRowKey.column());
    }

    @Override
    public void visit(LastRowKey lastRowKey) {
        out = Stream.of(lastRowKey.column());
    }

    @Override
    public void visit(Partition partition) {
        out = Stream.of(partition.column());
    }


    @Override
    public void visit(Formula formula) {
        out = Stream.of(formula.column());
    }
}
