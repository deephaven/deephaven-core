//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.api.Pair;
import io.deephaven.api.agg.spec.AggSpec;

import java.util.*;
import java.util.Map.Entry;

/**
 * Optimizes a collection of {@link Aggregation aggregations} by grouping like-specced aggregations together.
 */
public final class AggregationOptimizer implements Aggregation.Visitor {

    private static final Object COUNT_OBJ = new Object();
    private static final Object FIRST_ROW_KEY_OBJ = new Object();
    private static final Object LAST_ROW_KEY_OBJ = new Object();
    private static final Object PARTITION_KEEPING_OBJ = new Object();
    private static final Object PARTITION_DROPPING_OBJ = new Object();

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

    private final LinkedHashMap<Object, List<Aggregation>> visitOrder = new LinkedHashMap<>();

    public List<Aggregation> build() {
        List<Aggregation> out = new ArrayList<>();
        for (Entry<Object, List<Aggregation>> e : visitOrder.entrySet()) {
            if (e.getKey() == COUNT_OBJ
                    || e.getKey() == FIRST_ROW_KEY_OBJ
                    || e.getKey() == LAST_ROW_KEY_OBJ
                    || e.getKey() == PARTITION_KEEPING_OBJ
                    || e.getKey() == PARTITION_DROPPING_OBJ) {
                // These aggregations are now grouped together, output them together
                out.addAll(e.getValue());
            } else {
                // Assert that the key is an AggSpec
                if (!(e.getKey() instanceof AggSpec)) {
                    throw new IllegalStateException("Unexpected key type: " + e.getKey());
                }

                // Group all the aggregations with the same spec together
                final AggSpec aggSpec = (AggSpec) e.getKey();
                final List<Pair> pairs = new ArrayList<>();

                for (Aggregation inputAgg : e.getValue()) {
                    if (inputAgg instanceof ColumnAggregations) {
                        final ColumnAggregations agg = (ColumnAggregations) inputAgg;
                        pairs.addAll(agg.pairs());
                    } else {
                        final ColumnAggregation agg = (ColumnAggregation) inputAgg;
                        pairs.add(agg.pair());
                    }
                }
                if (pairs.size() == 1) {
                    out.add(ColumnAggregation.of(aggSpec, pairs.get(0)));
                } else {
                    out.add(ColumnAggregations.builder().spec(aggSpec).addAllPairs(pairs).build());
                }
            }
        }
        return out;
    }

    @Override
    public void visit(Aggregations aggregations) {
        aggregations.aggregations().forEach(a -> a.walk(this));
    }

    @Override
    public void visit(ColumnAggregation columnAgg) {
        visitOrder.computeIfAbsent(columnAgg.spec(), k -> new ArrayList<>()).add(columnAgg);
    }

    @Override
    public void visit(ColumnAggregations columnAggs) {
        visitOrder.computeIfAbsent(columnAggs.spec(), k -> new ArrayList<>()).add(columnAggs);
    }

    @Override
    public void visit(Count count) {
        visitOrder.computeIfAbsent(COUNT_OBJ, k -> new ArrayList<>()).add(count);
    }

    @Override
    public void visit(FirstRowKey firstRowKey) {
        visitOrder.computeIfAbsent(FIRST_ROW_KEY_OBJ, k -> new ArrayList<>()).add(firstRowKey);
    }

    @Override
    public void visit(LastRowKey lastRowKey) {
        visitOrder.computeIfAbsent(LAST_ROW_KEY_OBJ, k -> new ArrayList<>()).add(lastRowKey);
    }

    @Override
    public void visit(Partition partition) {
        visitOrder.computeIfAbsent(partition.includeGroupByColumns() ? PARTITION_KEEPING_OBJ : PARTITION_DROPPING_OBJ,
                k -> new ArrayList<>()).add(partition);
    }
}
