package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A visitor to get the ordered input/output {@link Pair column name pairs} for {@link Aggregation aggregations}.
 */
public class AggregationPairs implements Aggregation.Visitor {

    public static Stream<Pair> of(Aggregation aggregation) {
        return aggregation.walk(new AggregationPairs()).getOut();
    }

    public static Stream<Pair> of(Collection<? extends Aggregation> aggregations) {
        return aggregations.stream().flatMap(AggregationPairs::of);
    }

    public static Stream<ColumnName> outputsOf(Aggregation aggregation) {
        return of(aggregation).map(Pair::output);
    }

    public static Stream<ColumnName> outputsOf(Collection<? extends Aggregation> aggregations) {
        return of(aggregations).map(Pair::output);
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
        out = Stream.of(count.column());
    }

    @Override
    public void visit(FirstRowKey firstRowKey) {
        out = Stream.of(firstRowKey.column());
    }

    @Override
    public void visit(LastRowKey lastRowKey) {
        out = Stream.of(lastRowKey.column());
    }
}
