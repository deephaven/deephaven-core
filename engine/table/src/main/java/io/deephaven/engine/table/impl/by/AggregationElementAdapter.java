package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.*;
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
    public void visit(FirstRowKey firstRowKey) {
        out = new AggregationElementImpl(
                new KeyOnlyFirstOrLastBySpec(firstRowKey.column().name(), AggType.First),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    @Override
    public void visit(LastRowKey lastRowKey) {
        out = new AggregationElementImpl(
                new KeyOnlyFirstOrLastBySpec(lastRowKey.column().name(), AggType.Last),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    @Override
    public void visit(ColumnAggregation columnAgg) {
        final AggregationSpec spec = AggregationSpecAdapter.of(columnAgg.spec());
        final MatchPair pair = MatchPair.of(columnAgg.pair());
        out = new AggregationElementImpl(spec, pair);
    }

    @Override
    public void visit(ColumnAggregations columnAggs) {
        final AggregationSpec spec = AggregationSpecAdapter.of(columnAggs.spec());
        final MatchPair[] pairs = MatchPair.fromPairs(columnAggs.pairs());
        out = new AggregationElementImpl(spec, pairs);
    }
}
