package io.deephaven.api.agg.key;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class KeyColumnReferences implements Key.Visitor {

    public static Set<ColumnName> of(Key key) {
        return key.walk(new KeyColumnReferences()).out();
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
        out = sortedFirst.columns().stream().map(SortColumn::column).collect(Collectors.toSet());
    }

    @Override
    public void visit(KeySortedLast sortedLast) {
        out = sortedLast.columns().stream().map(SortColumn::column).collect(Collectors.toSet());
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
