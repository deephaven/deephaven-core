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

    private AggregationSpec out;

    public AggregationSpec out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(KeyAbsSum absSum) {
        out = new AbsSumSpec();
    }

    @Override
    public void visit(KeyCountDistinct countDistinct) {
        out = new CountDistinctSpec(countDistinct.countNulls());
    }

    @Override
    public void visit(KeyDistinct distinct) {
        out = new DistinctSpec(distinct.includeNulls());
    }

    @Override
    public void visit(KeyGroup group) {
        out = new AggregationGroupSpec();
    }

    @Override
    public void visit(KeyAvg avg) {
        out = new AvgSpec();
    }

    @Override
    public void visit(KeyFirst first) {
        out = new FirstBySpecImpl();
    }

    @Override
    public void visit(KeyLast last) {
        out = new LastBySpecImpl();
    }

    @Override
    public void visit(KeyMax max) {
        out = new MinMaxBySpecImpl(false);
    }

    @Override
    public void visit(KeyMedian median) {
        out = new PercentileBySpecImpl(0.50d, median.averageMedian());
    }

    @Override
    public void visit(KeyMin min) {
        out = new MinMaxBySpecImpl(true);
    }

    @Override
    public void visit(KeyPct pct) {
        out = new PercentileBySpecImpl(pct.percentile(), pct.averageMedian());
    }

    @Override
    public void visit(KeySortedFirst sortedFirst) {
        out = new SortedFirstBy(
                sortedFirst.columns().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new));
    }

    @Override
    public void visit(KeySortedLast sortedLast) {
        out = new SortedLastBy(
                sortedLast.columns().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new));
    }

    @Override
    public void visit(KeyStd std) {
        out = new StdSpec();
    }

    @Override
    public void visit(KeySum sum) {
        out = new SumSpec();
    }

    @Override
    public void visit(KeyUnique unique) {
        out = new UniqueSpec(unique.includeNulls());
    }

    @Override
    public void visit(KeyWAvg wAvg) {
        out = new WeightedAverageSpecImpl(wAvg.weight().name());
    }

    @Override
    public void visit(KeyWSum wSum) {
        out = new WeightedSumSpecImpl(wSum.weight().name());
    }

    @Override
    public void visit(KeyVar var) {
        out = new VarSpec();
    }
}
