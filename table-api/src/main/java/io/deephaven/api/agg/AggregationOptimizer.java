package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.spec.AggSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Optimizes a collection of {@link Aggregation aggregations} by grouping like-speccedd aggregations together.
 */
public final class AggregationOptimizer implements Aggregation.Visitor {
    private static final Object COUNT_OBJ = new Object();

    /**
     * Optimizes a collection of {@link Aggregation aggregations} by grouping like-specced aggregations together. The
     * input order will be preserved based on the spec-encountered order.
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
                out.add(NormalAggregation.of((AggSpec) e.getKey(), e.getValue().get(0)));
            } else {
                out.add(NormalAggregations.builder().spec((AggSpec) e.getKey()).addAllPairs(e.getValue()).build());
            }
        }
        return out;
    }

    @Override
    public void visit(Count count) {
        visitOrder.computeIfAbsent(COUNT_OBJ, k -> new ArrayList<>()).add(count.column());
    }

    @Override
    public void visit(NormalAggregation normalAgg) {
        visitOrder.computeIfAbsent(normalAgg.spec(), k -> new ArrayList<>()).add(normalAgg.pair());
    }

    @Override
    public void visit(NormalAggregations normalAggs) {
        visitOrder.computeIfAbsent(normalAggs.spec(), k -> new ArrayList<>())
                .addAll(normalAggs.pairs());
    }
}
