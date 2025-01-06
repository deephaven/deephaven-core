//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;
import io.deephaven.api.agg.spec.AggSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
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

    private final LinkedHashMap<Object, List<Pair>> visitOrder = new LinkedHashMap<>();

    public List<Aggregation> build() {
        List<Aggregation> out = new ArrayList<>();
        for (Entry<Object, List<Pair>> e : visitOrder.entrySet()) {
            if (e.getKey() == COUNT_OBJ) {
                for (Pair pair : e.getValue()) {
                    out.add(Count.of((ColumnName) pair));
                }
            } else if (e.getKey() == FIRST_ROW_KEY_OBJ) {
                for (Pair pair : e.getValue()) {
                    out.add(FirstRowKey.of((ColumnName) pair));
                }
            } else if (e.getKey() == LAST_ROW_KEY_OBJ) {
                for (Pair pair : e.getValue()) {
                    out.add(LastRowKey.of((ColumnName) pair));
                }
            } else if (e.getKey() == PARTITION_KEEPING_OBJ) {
                for (Pair pair : e.getValue()) {
                    out.add(Partition.of((ColumnName) pair));
                }
            } else if (e.getKey() == PARTITION_DROPPING_OBJ) {
                for (Pair pair : e.getValue()) {
                    out.add(Partition.of((ColumnName) pair, false));
                }
            } else if (e.getValue() == null) {
                out.add((Aggregation) e.getKey());
            } else if (e.getValue().size() == 1) {
                out.add(ColumnAggregation.of((AggSpec) e.getKey(), e.getValue().get(0)));
            } else {
                out.add(ColumnAggregations.builder().spec((AggSpec) e.getKey()).addAllPairs(e.getValue()).build());
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
        visitOrder.computeIfAbsent(columnAgg.spec(), k -> new ArrayList<>()).add(columnAgg.pair());
    }

    @Override
    public void visit(ColumnAggregations columnAggs) {
        visitOrder.computeIfAbsent(columnAggs.spec(), k -> new ArrayList<>())
                .addAll(columnAggs.pairs());
    }

    @Override
    public void visit(Count count) {
        visitOrder.computeIfAbsent(COUNT_OBJ, k -> new ArrayList<>()).add(count.column());
    }

    @Override
    public void visit(CountWhere countWhere) {
        // Supplying a `null` entry value indicates that the key is already an aggregation.
        visitOrder.putIfAbsent(countWhere, null);
    }

    @Override
    public void visit(FirstRowKey firstRowKey) {
        visitOrder.computeIfAbsent(FIRST_ROW_KEY_OBJ, k -> new ArrayList<>()).add(firstRowKey.column());
    }

    @Override
    public void visit(LastRowKey lastRowKey) {
        visitOrder.computeIfAbsent(LAST_ROW_KEY_OBJ, k -> new ArrayList<>()).add(lastRowKey.column());
    }

    @Override
    public void visit(Partition partition) {
        visitOrder.computeIfAbsent(partition.includeGroupByColumns() ? PARTITION_KEEPING_OBJ : PARTITION_DROPPING_OBJ,
                k -> new ArrayList<>()).add(partition.column());
    }

    @Override
    public void visit(Formula formula) {
        // Supplying a `null` entry value indicates that the key is already an aggregation.
        visitOrder.putIfAbsent(formula, null);
    }
}
