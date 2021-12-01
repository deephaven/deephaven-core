package io.deephaven.api.agg.key;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.KeyedAggregation;
import io.deephaven.api.agg.KeyedAggregations;
import io.deephaven.api.agg.Pair;

import java.util.Collection;

public abstract class KeyBase implements Key {

    @Override
    public final KeyedAggregation aggregation(Pair pair) {
        return KeyedAggregation.of(this, pair);
    }

    @Override
    public final Aggregation aggregation(Pair... pairs) {
        if (pairs.length == 1) {
            return aggregation(pairs[0]);
        }
        return KeyedAggregations.builder().key(this).addPairs(pairs).build();
    }

    @Override
    public final Aggregation aggregation(Collection<? extends Pair> pairs) {
        if (pairs.size() == 1) {
            return aggregation(pairs.iterator().next());
        }
        return KeyedAggregations.builder().key(this).addAllPairs(pairs).build();
    }
}
