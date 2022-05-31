package io.deephaven.api.agg.spec;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.ColumnAggregation;
import io.deephaven.api.agg.ColumnAggregations;
import io.deephaven.api.agg.Pair;

import java.util.Collection;

public abstract class AggSpecBase implements AggSpec {

    @Override
    public final ColumnAggregation aggregation(Pair pair) {
        return ColumnAggregation.of(this, pair);
    }

    @Override
    public final Aggregation aggregation(Pair... pairs) {
        if (pairs.length == 1) {
            return aggregation(pairs[0]);
        }
        return ColumnAggregations.builder().spec(this).addPairs(pairs).build();
    }

    @Override
    public final Aggregation aggregation(Collection<? extends Pair> pairs) {
        if (pairs.size() == 1) {
            return aggregation(pairs.iterator().next());
        }
        return ColumnAggregations.builder().spec(this).addAllPairs(pairs).build();
    }
}
