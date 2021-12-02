package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;

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

    private Stream<ColumnName> out;

    Stream<ColumnName> getOut() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(Count count) {
        out = Stream.of(count.column());
    }

    @Override
    public void visit(NormalAggregation normalAgg) {
        out = Stream.of(normalAgg.pair().output());
    }

    @Override
    public void visit(NormalAggregations normalAggs) {
        out = normalAggs.pairs().stream().map(Pair::output);
    }
}
