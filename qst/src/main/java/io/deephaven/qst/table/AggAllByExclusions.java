package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.agg.key.Key;
import io.deephaven.api.agg.key.Key.Visitor;
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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Computes the columns to exclude from aggregation output
 */
final class AggAllByExclusions implements Visitor {

    public static Set<ColumnName> of(Key key, Collection<? extends Selectable> groupByColumns) {
        final Set<ColumnName> exclusions =
                groupByColumns.stream().map(Selectable::newColumn).collect(Collectors.toSet());
        final Set<ColumnName> otherExclusions = key.walk(new AggAllByExclusions()).out();
        exclusions.addAll(otherExclusions);
        return exclusions;
    }

    private Set<ColumnName> out;

    public Set<ColumnName> out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(KeyAbsSum absSum) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyCountDistinct countDistinct) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyDistinct distinct) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyGroup group) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyAvg avg) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyFirst first) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyLast last) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyMax max) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyMedian median) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyMin min) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyPct pct) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeySortedFirst sortedFirst) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeySortedLast sortedLast) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyStd std) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeySum sum) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyUnique unique) {
        out = Collections.emptySet();
    }

    @Override
    public void visit(KeyWAvg wAvg) {
        out = Collections.singleton(wAvg.weight());
    }

    @Override
    public void visit(KeyWSum wSum) {
        out = Collections.singleton(wSum.weight());
    }

    @Override
    public void visit(KeyVar var) {
        out = Collections.emptySet();
    }
}
