package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.NormalAggregation;
import io.deephaven.api.agg.NormalAggregations;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.by.AggregationFactory.AggregationElement;
import io.deephaven.engine.table.impl.by.AggregationFactory.AggregationElementImpl;

import java.util.Objects;

/**
 * Utility for converting an {@link Aggregation} to an {@link AggregationElement}.
 */
class AggregationElementAdapter implements Aggregation.Visitor {

    public static AggregationElement of(Aggregation aggregation) {
        return aggregation.walk(new AggregationElementAdapter()).out();
    }

    private AggregationElement out;

    public AggregationElement out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(Count count) {
        out = new AggregationFactory.CountAggregationElement(count.column().name());
    }

    @Override
    public void visit(NormalAggregation normalAgg) {
        final AggregationSpec spec = AggregationSpecAdapter.of(normalAgg.spec());
        final MatchPair pair = MatchPair.of(normalAgg.pair());
        out = new AggregationElementImpl(spec, pair);
    }

    @Override
    public void visit(NormalAggregations normalAggs) {
        final AggregationSpec spec = AggregationSpecAdapter.of(normalAggs.spec());
        final MatchPair[] pairs = MatchPair.fromPairs(normalAggs.pairs());
        out = new AggregationElementImpl(spec, pairs);
    }
}
