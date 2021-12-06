package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpec.Visitor;
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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Computes the columns to exclude from aggregation output
 */
final class AggAllByExclusions implements Visitor {

    public static Set<ColumnName> of(AggSpec spec, Collection<? extends Selectable> groupByColumns) {
        final Set<ColumnName> exclusions =
                groupByColumns.stream().map(Selectable::newColumn).collect(Collectors.toSet());
        final Set<ColumnName> otherExclusions = spec.walk(new AggAllByExclusions()).out();
        exclusions.addAll(otherExclusions);
        return exclusions;
    }

    private Set<ColumnName> out;

    public Set<ColumnName> out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(AggSpecAbsSum absSum) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecCountDistinct countDistinct) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecDistinct distinct) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecGroup group) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecAvg avg) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecFirst first) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecFormula formula) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecLast last) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecMax max) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecMedian median) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecMin min) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecPercentile pct) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecSortedFirst sortedFirst) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecSortedLast sortedLast) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecStd std) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecSum sum) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecUnique unique) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(AggSpecWAvg wAvg) {
        out = Collections.singleton(wAvg.weight());
    }

    @Override
    public void visit(AggSpecWSum wSum) {
        out = Collections.singleton(wSum.weight());
    }

    @Override
    public void visit(AggSpecVar var) {
        out = Collections.emptySet();
    }
}
