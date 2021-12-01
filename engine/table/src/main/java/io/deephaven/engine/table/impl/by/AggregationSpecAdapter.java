package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.key.Key;
import io.deephaven.api.agg.key.KeyAbsSum;
import io.deephaven.api.agg.key.KeyAvg;
import io.deephaven.api.agg.key.KeyCountDistinct;
import io.deephaven.api.agg.key.KeyDistinct;
import io.deephaven.api.agg.key.KeyFirst;
import io.deephaven.api.agg.key.KeyGroup;
import io.deephaven.api.agg.key.KeyLast;
import io.deephaven.api.agg.key.KeyMax;
import io.deephaven.api.agg.key.KeyMedian;
import io.deephaven.api.agg.key.KeyMin;
import io.deephaven.api.agg.key.KeyPct;
import io.deephaven.api.agg.key.KeySortedFirst;
import io.deephaven.api.agg.key.KeySortedLast;
import io.deephaven.api.agg.key.KeyStd;
import io.deephaven.api.agg.key.KeySum;
import io.deephaven.api.agg.key.KeyUnique;
import io.deephaven.api.agg.key.KeyVar;
import io.deephaven.api.agg.key.KeyWAvg;
import io.deephaven.api.agg.key.KeyWSum;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class AggregationSpecAdapter implements Key.Visitor {

    public static AggregationSpec of(Key key) {
        return key.walk(new AggregationSpecAdapter()).out();
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyAbsSum key) {
        return new AbsSumSpec();
    }

    public static AggregationSpec of(KeyCountDistinct countDistinct) {
        return new CountDistinctSpec(countDistinct.countNulls());
    }

    public static AggregationSpec of(KeyDistinct distinct) {
        return new DistinctSpec(distinct.includeNulls());
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyGroup group) {
        return new AggregationGroupSpec();
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyAvg avg) {
        return new AvgSpec();
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyFirst first) {
        return new FirstBySpecImpl();
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyLast last) {
        return new LastBySpecImpl();
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyMax max) {
        return new MinMaxBySpecImpl(false);
    }

    public static AggregationSpec of(KeyMedian median) {
        return new PercentileBySpecImpl(0.50d, median.averageMedian());
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyMin min) {
        return new MinMaxBySpecImpl(true);
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyPct pct) {
        return new PercentileBySpecImpl(pct.percentile(), pct.averageMedian());
    }

    public static AggregationSpec of(KeySortedFirst sortedFirst) {
        return new SortedFirstBy(
                sortedFirst.columns().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new));
    }

    public static AggregationSpec of(KeySortedLast sortedLast) {
        return new SortedLastBy(
                sortedLast.columns().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new));
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyStd std) {
        return new StdSpec();
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeySum sum) {
        return new SumSpec();
    }

    public static AggregationSpec of(KeyUnique unique) {
        return new UniqueSpec(unique.includeNulls());
    }

    public static AggregationSpec of(KeyWAvg wAvg) {
        return new WeightedAverageSpecImpl(wAvg.weight().name());
    }

    public static AggregationSpec of(KeyWSum wSum) {
        return new WeightedSumSpecImpl(wSum.weight().name());
    }

    public static AggregationSpec of(@SuppressWarnings("unused") KeyVar var) {
        return new VarSpec();
    }

    private AggregationSpec out;

    public AggregationSpec out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(KeyAbsSum absSum) {
        out = of(absSum);
    }

    @Override
    public void visit(KeyCountDistinct countDistinct) {
        out = of(countDistinct);
    }

    @Override
    public void visit(KeyDistinct distinct) {
        out = of(distinct);
    }

    @Override
    public void visit(KeyGroup group) {
        out = of(group);
    }

    @Override
    public void visit(KeyAvg avg) {
        out = of(avg);
    }

    @Override
    public void visit(KeyFirst first) {
        out = of(first);
    }

    @Override
    public void visit(KeyLast last) {
        out = of(last);
    }

    @Override
    public void visit(KeyMax max) {
        out = of(max);
    }

    @Override
    public void visit(KeyMedian median) {
        out = of(median);
    }

    @Override
    public void visit(KeyMin min) {
        out = of(min);
    }

    @Override
    public void visit(KeyPct pct) {
        out = of(pct);
    }

    @Override
    public void visit(KeySortedFirst sortedFirst) {
        out = of(sortedFirst);
    }

    @Override
    public void visit(KeySortedLast sortedLast) {
        out = of(sortedLast);
    }

    @Override
    public void visit(KeyStd std) {
        out = of(std);
    }

    @Override
    public void visit(KeySum sum) {
        out = of(sum);
    }

    @Override
    public void visit(KeyUnique unique) {
        out = of(unique);
    }

    @Override
    public void visit(KeyWAvg wAvg) {
        out = of(wAvg);
    }

    @Override
    public void visit(KeyWSum wSum) {
        out = of(wSum);
    }

    @Override
    public void visit(KeyVar var) {
        out = of(var);
    }
}
