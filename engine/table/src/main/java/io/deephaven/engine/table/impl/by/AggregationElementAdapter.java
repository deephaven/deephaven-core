package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.KeyedAggregation;
import io.deephaven.api.agg.KeyedAggregations;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.by.AggregationFactory.AggregationElement;
import io.deephaven.engine.table.impl.by.AggregationFactory.AggregationElementImpl;

import java.util.Objects;

/**
 * Utility for converting an {@link Aggregation} to an {@link AggregationElement}.
 */
class AggregationElementAdapter implements Aggregation.Visitor {

    public static AggregationElement of(Aggregation aggregation, BaseTable parent) {
        return aggregation.walk(new AggregationElementAdapter(parent)).out();
    }

    private final BaseTable parent;
    private AggregationElement out;

    public AggregationElementAdapter(BaseTable parent) {
        this.parent = Objects.requireNonNull(parent);
    }

    public AggregationElement out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(Count count) {
        out = new AggregationFactory.CountAggregationElement(count.column().name());
    }

    @Override
    public void visit(KeyedAggregation keyedAgg) {
        final AggregationSpec spec = AggregationSpecAdapter.of(keyedAgg.key(), parent);
        final MatchPair pair = MatchPair.of(keyedAgg.pair());
        out = new AggregationElementImpl(spec, pair);
    }

    @Override
    public void visit(KeyedAggregations keyedAggs) {
        final AggregationSpec spec = AggregationSpecAdapter.of(keyedAggs.key(), parent);
        final MatchPair[] pairs = MatchPair.fromPairs(keyedAggs.pairs());
        out = new AggregationElementImpl(spec, pairs);
    }
}
