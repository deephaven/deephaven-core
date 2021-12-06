package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpecAbsSum;
import io.deephaven.api.agg.spec.AggSpecAvg;
import io.deephaven.api.agg.spec.AggSpecCountDistinct;
import io.deephaven.api.agg.spec.AggSpecDistinct;
import io.deephaven.api.agg.spec.AggSpecFirst;
import io.deephaven.api.agg.spec.AggSpecFormula;
import io.deephaven.api.agg.spec.AggSpecGroup;
import io.deephaven.api.agg.spec.AggSpecLast;
import io.deephaven.api.agg.spec.AggSpecMax;
import io.deephaven.api.agg.spec.AggSpecMedian;
import io.deephaven.api.agg.spec.AggSpecMin;
import io.deephaven.api.agg.spec.AggSpecPercentile;
import io.deephaven.api.agg.spec.AggSpecSortedFirst;
import io.deephaven.api.agg.spec.AggSpecSortedLast;
import io.deephaven.api.agg.spec.AggSpecStd;
import io.deephaven.api.agg.spec.AggSpecSum;
import io.deephaven.api.agg.spec.AggSpecUnique;
import io.deephaven.api.agg.spec.AggSpecVar;
import io.deephaven.api.agg.spec.AggSpecWAvg;
import io.deephaven.api.agg.spec.AggSpecWSum;

import java.util.Objects;

import static io.deephaven.engine.table.impl.QueryTable.TRACKED_FIRST_BY;
import static io.deephaven.engine.table.impl.QueryTable.TRACKED_LAST_BY;

public class AggregationSpecAdapter implements AggSpec.Visitor {

    public static AggregationSpec of(AggSpec spec) {
        return spec.walk(new AggregationSpecAdapter()).out();
    }

    private AggregationSpec out;


    public AggregationSpec out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(AggSpecAbsSum absSum) {
        out = new AbsSumSpec();
    }

    @Override
    public void visit(AggSpecCountDistinct countDistinct) {
        out = new CountDistinctSpec(countDistinct.countNulls());
    }

    @Override
    public void visit(AggSpecDistinct distinct) {
        out = new DistinctSpec(distinct.includeNulls());
    }

    @Override
    public void visit(AggSpecGroup group) {
        out = new AggregationGroupSpec();
    }

    @Override
    public void visit(AggSpecAvg avg) {
        out = new AvgSpec();
    }

    @Override
    public void visit(AggSpecFirst first) {
        out = TRACKED_FIRST_BY ? new TrackingFirstBySpecImpl() : new FirstBySpecImpl();
    }

    @Override
    public void visit(AggSpecFormula formula) {
        out = new AggregationFormulaSpec(formula.formula(), formula.formulaParam());
    }

    @Override
    public void visit(AggSpecLast last) {
        out = TRACKED_LAST_BY ? new TrackingLastBySpecImpl() : new LastBySpecImpl();
    }

    @Override
    public void visit(AggSpecMax max) {
        out = new MinMaxBySpecImpl(false);
    }

    @Override
    public void visit(AggSpecMedian median) {
        out = new PercentileBySpecImpl(0.50d, median.averageMedian());
    }

    @Override
    public void visit(AggSpecMin min) {
        out = new MinMaxBySpecImpl(true);
    }

    @Override
    public void visit(AggSpecPercentile pct) {
        out = new PercentileBySpecImpl(pct.percentile(), pct.averageMedian());
    }

    @Override
    public void visit(AggSpecSortedFirst sortedFirst) {
        out = new SortedFirstBy(
                sortedFirst.columns().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new));
    }

    @Override
    public void visit(AggSpecSortedLast sortedLast) {
        out = new SortedLastBy(
                sortedLast.columns().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new));
    }

    @Override
    public void visit(AggSpecStd std) {
        out = new StdSpec();
    }

    @Override
    public void visit(AggSpecSum sum) {
        out = new SumSpec();
    }

    @Override
    public void visit(AggSpecUnique unique) {
        out = new UniqueSpec(unique.includeNulls());
    }

    @Override
    public void visit(AggSpecWAvg wAvg) {
        out = new WeightedAverageSpecImpl(wAvg.weight().name());
    }

    @Override
    public void visit(AggSpecWSum wSum) {
        out = new WeightedSumSpecImpl(wSum.weight().name());
    }

    @Override
    public void visit(AggSpecVar var) {
        out = new VarSpec();
    }
}
