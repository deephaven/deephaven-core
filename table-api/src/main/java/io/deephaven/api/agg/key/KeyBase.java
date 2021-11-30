package io.deephaven.api.agg.key;

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
    public final KeyedAggregations aggregation(Pair... pairs) {
        return KeyedAggregations.builder().key(this).addPairs(pairs).build();
    }

    @Override
    public final KeyedAggregations aggregation(Collection<? extends Pair> pairs) {
        return KeyedAggregations.builder().key(this).addAllPairs(pairs).build();
    }
}
