package io.deephaven.engine.table.impl.by.rollup;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.api.agg.Pair;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * A visitor to get the ordered input/output {@link Pair column name pairs} for {@link Aggregation aggregations},
 * including {@link RollupAggregation rollup aggregations}.
 */
public class RollupAggregationPairs extends AggregationPairs implements RollupAggregation.Visitor {

    public static Stream<Pair> of(Aggregation aggregation) {
        return aggregation.walk(new RollupAggregationPairs()).getOut();
    }

    public static Stream<Pair> of(Collection<? extends Aggregation> aggregations) {
        return aggregations.stream().flatMap(RollupAggregationPairs::of);
    }

    public static Stream<ColumnName> outputsOf(Aggregation aggregation) {
        return of(aggregation).map(Pair::output);
    }

    public static Stream<ColumnName> outputsOf(Collection<? extends Aggregation> aggregations) {
        return of(aggregations).map(Pair::output);
    }

    @Override
    public void visit(NullColumns nullColumns) {
        out = nullColumns.resultColumns().keySet().stream().map(ColumnName::of);
    }

    @Override
    public void visit(Partition partition) {
        out = Stream.empty();
    }
}
