package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.*;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.by.AggregationFactory.AggregationElement;
import io.deephaven.tuple.generated.ByteDoubleTuple;

import java.util.*;

/**
 * Utility for converting collections of {@link Aggregation aggregations} to collections of {@link AggregationElement},
 * grouping compatible aggregations in order to allow optimizations at execution time.
 */
class AggregationAdapterOptimized implements Aggregation.Visitor {

    private final List<Pair> absSums = new ArrayList<>();
    private final List<Pair> arrays = new ArrayList<>();
    private final List<Pair> avgs = new ArrayList<>();
    private final List<ColumnName> counts = new ArrayList<>();
    private final Map<Boolean, List<Pair>> countDistincts = new HashMap<>();
    private final Map<Boolean, List<Pair>> distincts = new HashMap<>();
    private final List<Pair> firsts = new ArrayList<>();
    private final List<Pair> lasts = new ArrayList<>();
    private final List<Pair> maxs = new ArrayList<>();
    private final Map<Boolean, List<Pair>> medians = new HashMap<>();
    private final List<Pair> mins = new ArrayList<>();
    private final Map<ByteDoubleTuple, List<Pair>> pcts = new HashMap<>();
    private final Map<List<SortColumn>, List<Pair>> sortedFirsts = new HashMap<>();
    private final Map<List<SortColumn>, List<Pair>> sortedLasts = new HashMap<>();
    private final List<Pair> stds = new ArrayList<>();
    private final List<Pair> sums = new ArrayList<>();
    private final Map<Boolean, List<Pair>> uniques = new HashMap<>();
    private final List<Pair> vars = new ArrayList<>();
    private final Map<ColumnName, List<Pair>> wAvgs = new HashMap<>();
    private final Map<ColumnName, List<Pair>> wSums = new HashMap<>();

    /**
     * We'll do our best to maintain the original aggregation ordering. This will maintain the user-specified order as
     * long as the user aggregation types were all next to each other.
     * <p>
     * ie:
     * <p>
     * {@code aggBy([ Sum.of(A), Sum.of(B), Avg.of(C), Avg.of(D) ], ...)} will not need to be re-ordered
     * <p>
     * {@code aggBy([ Sum.of(A), Avg.of(C), Avg.of(D), Sum.of(B) ], ...)} will need to be re-ordered
     */
    private final LinkedHashSet<BuildLogic> buildOrder = new LinkedHashSet<>();

    @FunctionalInterface
    private interface BuildLogic {
        void appendTo(List<AggregationElement> outs);
    }

    // Unfortunately, it doesn't look like we can add ad-hoc lambdas to buildOrder, they don't appear to be equal
    // across multiple constructions.
    private final BuildLogic buildAbsSums = this::buildAbsSums;
    private final BuildLogic buildArrays = this::buildArrays;
    private final BuildLogic buildAvgs = this::buildAvgs;
    private final BuildLogic buildCounts = this::buildCounts;
    private final BuildLogic buildCountDistincts = this::buildCountDistincts;
    private final BuildLogic buildDistincts = this::buildDistincts;
    private final BuildLogic buildFirsts = this::buildFirsts;
    private final BuildLogic buildLasts = this::buildLasts;
    private final BuildLogic buildMaxes = this::buildMaxes;
    private final BuildLogic buildMedians = this::buildMedians;
    private final BuildLogic buildMins = this::buildMins;
    private final BuildLogic buildPcts = this::buildPcts;
    private final BuildLogic buildSortedFirsts = this::buildSortedFirsts;
    private final BuildLogic buildSortedLasts = this::buildSortedLasts;
    private final BuildLogic buildStds = this::buildStds;
    private final BuildLogic buildSums = this::buildSums;
    private final BuildLogic buildUniques = this::buildUniques;
    private final BuildLogic buildVars = this::buildVars;
    private final BuildLogic buildWAvgs = this::buildWAvgs;
    private final BuildLogic buildWSums = this::buildWSums;


    List<AggregationElement> build() {
        List<AggregationElement> aggs = new ArrayList<>();
        for (BuildLogic buildLogic : buildOrder) {
            buildLogic.appendTo(aggs);
        }
        return aggs;
    }

    private void buildWSums(List<AggregationElement> aggs) {
        for (Map.Entry<ColumnName, List<Pair>> e : wSums.entrySet()) {
            aggs.add(AggregationFactory.Agg(new WeightedSumSpecImpl(e.getKey().name()),
                    MatchPair.fromPairs(e.getValue())));
        }
    }

    private void buildWAvgs(List<AggregationElement> aggs) {
        for (Map.Entry<ColumnName, List<Pair>> e : wAvgs.entrySet()) {
            aggs.add(AggregationFactory.Agg(new WeightedAverageSpecImpl(e.getKey().name()),
                    MatchPair.fromPairs(e.getValue())));
        }
    }

    private void buildVars(List<AggregationElement> aggs) {
        if (!vars.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.Var, MatchPair.fromPairs(vars)));
        }
    }

    private void buildUniques(List<AggregationElement> aggs) {
        for (Map.Entry<Boolean, List<Pair>> e : uniques.entrySet()) {
            aggs.add(AggregationFactory.Agg(new UniqueSpec(e.getKey()), MatchPair.fromPairs(e.getValue())));
        }
    }

    private void buildSums(List<AggregationElement> aggs) {
        if (!sums.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.Sum, MatchPair.fromPairs(sums)));
        }
    }

    private void buildStds(List<AggregationElement> aggs) {
        if (!stds.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.Std, MatchPair.fromPairs(stds)));
        }
    }

    private void buildSortedLasts(List<AggregationElement> aggs) {
        for (Map.Entry<List<SortColumn>, List<Pair>> e : sortedLasts.entrySet()) {
            // TODO(deephaven-core#821): SortedFirst / SortedLast aggregations with sort direction
            String[] columns =
                    e.getKey().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new);
            aggs.add(AggregationFactory.Agg(new SortedLastBy(columns), MatchPair.fromPairs(e.getValue())));
        }
    }

    private void buildSortedFirsts(List<AggregationElement> aggs) {
        for (Map.Entry<List<SortColumn>, List<Pair>> e : sortedFirsts.entrySet()) {
            // TODO(deephaven-core#821): SortedFirst / SortedLast aggregations with sort direction
            String[] columns =
                    e.getKey().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new);
            aggs.add(AggregationFactory.Agg(new SortedFirstBy(columns), MatchPair.fromPairs(e.getValue())));
        }
    }

    private void buildPcts(List<AggregationElement> aggs) {
        for (Map.Entry<ByteDoubleTuple, List<Pair>> e : pcts.entrySet()) {
            aggs.add(AggregationFactory.Agg(new PercentileBySpecImpl(e.getKey().getSecondElement(),
                    e.getKey().getFirstElement() != 0), MatchPair.fromPairs(e.getValue())));
        }
    }

    private void buildMins(List<AggregationElement> aggs) {
        if (!mins.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.Min, MatchPair.fromPairs(mins)));
        }
    }

    private void buildMedians(List<AggregationElement> aggs) {
        for (Map.Entry<Boolean, List<Pair>> e : medians.entrySet()) {
            aggs.add(AggregationFactory.Agg(new PercentileBySpecImpl(0.50d, e.getKey()),
                    MatchPair.fromPairs(e.getValue())));
        }
    }

    private void buildMaxes(List<AggregationElement> aggs) {
        if (!maxs.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.Max, MatchPair.fromPairs(maxs)));
        }
    }

    private void buildLasts(List<AggregationElement> aggs) {
        if (!lasts.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.Last, MatchPair.fromPairs(lasts)));
        }
    }

    private void buildFirsts(List<AggregationElement> aggs) {
        if (!firsts.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.First, MatchPair.fromPairs(firsts)));
        }
    }

    private void buildDistincts(List<AggregationElement> aggs) {
        for (Map.Entry<Boolean, List<Pair>> e : distincts.entrySet()) {
            aggs.add(AggregationFactory.Agg(new DistinctSpec(e.getKey()), MatchPair.fromPairs(e.getValue())));
        }
    }

    private void buildCountDistincts(List<AggregationElement> aggs) {
        for (Map.Entry<Boolean, List<Pair>> e : countDistincts.entrySet()) {
            aggs.add(AggregationFactory.Agg(new CountDistinctSpec(e.getKey()), MatchPair.fromPairs(e.getValue())));
        }
    }

    private void buildCounts(List<AggregationElement> aggs) {
        for (ColumnName count : counts) {
            aggs.add(new AggregationFactory.CountAggregationElement(count.name()));
        }
    }

    private void buildAvgs(List<AggregationElement> aggs) {
        if (!avgs.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.Avg, MatchPair.fromPairs(avgs)));
        }
    }

    private void buildArrays(List<AggregationElement> aggs) {
        if (!arrays.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.Group, MatchPair.fromPairs(arrays)));
        }
    }

    private void buildAbsSums(List<AggregationElement> aggs) {
        if (!absSums.isEmpty()) {
            aggs.add(AggregationFactory.Agg(AggType.AbsSum, MatchPair.fromPairs(absSums)));
        }
    }

    @Override
    public void visit(AbsSum absSum) {
        absSums.add(absSum.pair());
        buildOrder.add(buildAbsSums);
    }

    @Override
    public void visit(Group group) {
        arrays.add(group.pair());
        buildOrder.add(buildArrays);
    }

    @Override
    public void visit(Avg avg) {
        avgs.add(avg.pair());
        buildOrder.add(buildAvgs);
    }

    @Override
    public void visit(Count count) {
        counts.add(count.column());
        buildOrder.add(buildCounts);
    }

    @Override
    public void visit(CountDistinct countDistinct) {
        countDistincts.computeIfAbsent(countDistinct.countNulls(), b -> new ArrayList<>())
                .add(countDistinct.pair());
        buildOrder.add(buildCountDistincts);
    }

    @Override
    public void visit(Distinct distinct) {
        distincts.computeIfAbsent(distinct.includeNulls(), b -> new ArrayList<>()).add(distinct.pair());
        buildOrder.add(buildDistincts);
    }

    @Override
    public void visit(First first) {
        firsts.add(first.pair());
        buildOrder.add(buildFirsts);
    }

    @Override
    public void visit(Last last) {
        lasts.add(last.pair());
        buildOrder.add(buildLasts);
    }

    @Override
    public void visit(Max max) {
        maxs.add(max.pair());
        buildOrder.add(buildMaxes);
    }

    @Override
    public void visit(Med med) {
        medians.computeIfAbsent(med.averageMedian(), b -> new ArrayList<>()).add(med.pair());
        buildOrder.add(buildMedians);
    }

    @Override
    public void visit(Min min) {
        mins.add(min.pair());
        buildOrder.add(buildMins);
    }

    @Override
    public void visit(Multi<?> multi) {
        for (Aggregation aggregation : multi.aggregations()) {
            aggregation.walk(this);
        }
    }

    @Override
    public void visit(Pct pct) {
        pcts.computeIfAbsent(new ByteDoubleTuple(pct.averageMedian() ? (byte) 1 : (byte) 0, pct.percentile()),
                b -> new ArrayList<>()).add(pct.pair());
        buildOrder.add(buildPcts);
    }

    @Override
    public void visit(SortedFirst sortedFirst) {
        sortedFirsts.computeIfAbsent(sortedFirst.columns(), b -> new ArrayList<>()).add(sortedFirst.pair());
        buildOrder.add(buildSortedFirsts);
    }

    @Override
    public void visit(SortedLast sortedLast) {
        sortedLasts.computeIfAbsent(sortedLast.columns(), b -> new ArrayList<>()).add(sortedLast.pair());
        buildOrder.add(buildSortedLasts);
    }

    @Override
    public void visit(Std std) {
        stds.add(std.pair());
        buildOrder.add(buildStds);
    }

    @Override
    public void visit(Sum sum) {
        sums.add(sum.pair());
        buildOrder.add(buildSums);
    }

    @Override
    public void visit(Unique unique) {
        uniques.computeIfAbsent(unique.includeNulls(), b -> new ArrayList<>()).add(unique.pair());
        buildOrder.add(buildUniques);
    }

    @Override
    public void visit(Var var) {
        vars.add(var.pair());
        buildOrder.add(buildVars);
    }

    @Override
    public void visit(WAvg wAvg) {
        wAvgs.computeIfAbsent(wAvg.weight(), b -> new ArrayList<>()).add(wAvg.pair());
        buildOrder.add(buildWAvgs);
    }

    @Override
    public void visit(WSum wSum) {
        wSums.computeIfAbsent(wSum.weight(), b -> new ArrayList<>()).add(wSum.pair());
        buildOrder.add(buildWSums);
    }
}
