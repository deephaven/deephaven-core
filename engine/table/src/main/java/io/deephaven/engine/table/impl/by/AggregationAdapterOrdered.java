package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.*;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.by.AggregationFactory.AggregationElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility for converting collections of {@link Aggregation aggregations} to collections of {@link AggregationElement}
 * in strict order with no coalescing/optimization.
 */
class AggregationAdapterOrdered implements Aggregation.Visitor {

    private final List<AggregationElement> aggs = new ArrayList<>();

    List<AggregationElement> build() {
        return aggs;
    }

    @Override
    public void visit(AbsSum absSum) {
        aggs.add(AggregationFactory.Agg(AggType.AbsSum, MatchPair.of(absSum.pair())));
    }

    @Override
    public void visit(Group group) {
        aggs.add(AggregationFactory.Agg(AggType.Group, MatchPair.of(group.pair())));
    }

    @Override
    public void visit(Avg avg) {
        aggs.add(AggregationFactory.Agg(AggType.Avg, MatchPair.of(avg.pair())));
    }

    @Override
    public void visit(Count count) {
        aggs.add(new AggregationFactory.CountAggregationElement(count.column().name()));
    }

    @Override
    public void visit(CountDistinct countDistinct) {
        aggs.add(AggregationFactory.Agg(new CountDistinctSpec(countDistinct.countNulls()),
                MatchPair.of(countDistinct.pair())));
    }

    @Override
    public void visit(Distinct distinct) {
        aggs.add(AggregationFactory.Agg(new DistinctSpec(distinct.includeNulls()), MatchPair.of(distinct.pair())));
    }

    @Override
    public void visit(First first) {
        aggs.add(AggregationFactory.Agg(AggType.First, MatchPair.of(first.pair())));
    }

    @Override
    public void visit(Last last) {
        aggs.add(AggregationFactory.Agg(AggType.Last, MatchPair.of(last.pair())));
    }

    @Override
    public void visit(Max max) {
        aggs.add(AggregationFactory.Agg(AggType.Max, MatchPair.of(max.pair())));
    }

    @Override
    public void visit(Med med) {
        aggs.add(
                AggregationFactory.Agg(new PercentileBySpecImpl(0.50d, med.averageMedian()), MatchPair.of(med.pair())));
    }

    @Override
    public void visit(Min min) {
        aggs.add(AggregationFactory.Agg(AggType.Min, MatchPair.of(min.pair())));
    }

    @Override
    public void visit(Multi<?> multi) {
        final List<AggregationElement> forMulti = AggregationFactory.AggregationElement.convert(multi);
        if (forMulti.size() > 1) {
            throw new IllegalArgumentException(
                    "Expected all elements in a Multi to map to a single operator, encountered aggregations: "
                            + multi.aggregations());
        }
        aggs.add(forMulti.get(0));
    }

    @Override
    public void visit(Pct pct) {
        aggs.add(AggregationFactory.Agg(
                new PercentileBySpecImpl(pct.percentile(), pct.averageMedian()), MatchPair.of(pct.pair())));
    }

    @Override
    public void visit(SortedFirst sortedFirst) {
        // TODO(deephaven-core#821): SortedFirst / SortedLast aggregations with sort direction
        final String[] columns =
                sortedFirst.columns().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new);
        aggs.add(AggregationFactory.Agg(new SortedFirstBy(columns), MatchPair.of(sortedFirst.pair())));
    }

    @Override
    public void visit(SortedLast sortedLast) {
        // TODO(deephaven-core#821): SortedFirst / SortedLast aggregations with sort direction
        final String[] columns =
                sortedLast.columns().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new);
        aggs.add(AggregationFactory.Agg(new SortedFirstBy(columns), MatchPair.of(sortedLast.pair())));
    }

    @Override
    public void visit(Std std) {
        aggs.add(AggregationFactory.Agg(AggType.Std, MatchPair.of(std.pair())));
    }

    @Override
    public void visit(Sum sum) {
        aggs.add(AggregationFactory.Agg(AggType.Sum, MatchPair.of(sum.pair())));
    }

    @Override
    public void visit(Unique unique) {
        aggs.add(AggregationFactory.Agg(new UniqueSpec(unique.includeNulls()), MatchPair.of(unique.pair())));
    }

    @Override
    public void visit(Var var) {
        aggs.add(AggregationFactory.Agg(AggType.Var, MatchPair.of(var.pair())));
    }

    @Override
    public void visit(WAvg wAvg) {
        aggs.add(AggregationFactory.Agg(new WeightedAverageSpecImpl(wAvg.weight().name()), MatchPair.of(wAvg.pair())));
    }

    @Override
    public void visit(WSum wSum) {
        aggs.add(AggregationFactory.Agg(new WeightedSumSpecImpl(wSum.weight().name()), MatchPair.of(wSum.pair())));
    }
}
