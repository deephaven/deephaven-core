package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.key.Key;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Optimizes a collection of {@link Aggregation aggregations} by grouping like-keyed aggregations together.
 */
public final class AggregationOptimizer implements Aggregation.Visitor {
    private static final Object COUNT_OBJ = new Object();

    /**
     * Optimizes a collection of {@link Aggregation aggregations} by grouping like-keyed aggregations together. The
     * input order will be preserved based on the key-encountered order.
     *
     * @param aggregations the aggregations
     * @return the optimized aggregations
     */
    public static List<Aggregation> of(Collection<? extends Aggregation> aggregations) {
        final AggregationOptimizer optimizer = new AggregationOptimizer();
        for (Aggregation aggregation : aggregations) {
            aggregation.walk(optimizer);
        }
        return optimizer.build();
    }

    private final LinkedHashMap<Object, List<Pair>> visitOrder = new LinkedHashMap<>();

    public List<Aggregation> build() {
        List<Aggregation> out = new ArrayList<>();
        for (Entry<Object, List<Pair>> e : visitOrder.entrySet()) {
            if (e.getKey() == COUNT_OBJ) {
                for (Pair pair : e.getValue()) {
                    out.add(Count.of((ColumnName) pair));
                }
            } else if (e.getValue().size() == 1) {
                out.add(KeyedAggregation.of((Key) e.getKey(), e.getValue().get(0)));
            } else {
                out.add(KeyedAggregations.builder().key((Key) e.getKey()).addAllPairs(e.getValue()).build());
            }
        }
        return out;
    }

    @Override
    public void visit(Count count) {
        visitOrder.computeIfAbsent(COUNT_OBJ, k -> new ArrayList<>()).add(count.column());
    }

    @Override
    public void visit(KeyedAggregation keyedAgg) {
        visitOrder.computeIfAbsent(keyedAgg.key(), k -> new ArrayList<>()).add(keyedAgg.pair());
    }

    @Override
    public void visit(KeyedAggregations keyedAggs) {
        visitOrder.computeIfAbsent(keyedAggs.key(), k -> new ArrayList<>())
                .addAll(keyedAggs.pairs());
    }
}
