package io.deephaven.engine.table.impl;

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
import io.deephaven.engine.table.Table;

import java.util.Objects;

class AggAllByUseTable implements AggSpec.Visitor {

    public static Table of(Table parent, AggSpec spec) {
        return spec.walk(new AggAllByUseTable(parent)).out();
    }

    private final Table parent;
    private Table out;

    public AggAllByUseTable(Table parent) {
        this.parent = Objects.requireNonNull(parent);
    }

    public Table out() {
        return Objects.requireNonNull(out);
    }

    private void keep() {
        out = parent;
    }

    private void drop() {
        out = parent.dropColumnFormats();
    }

    @Override
    public void visit(AggSpecAbsSum absSum) {
        drop();
    }

    @Override
    public void visit(AggSpecCountDistinct countDistinct) {
        drop();
    }

    @Override
    public void visit(AggSpecDistinct distinct) {
        keep();
    }

    @Override
    public void visit(AggSpecGroup group) {
        drop();
    }

    @Override
    public void visit(AggSpecAvg avg) {
        drop();
    }

    @Override
    public void visit(AggSpecFirst first) {
        keep();
    }

    @Override
    public void visit(AggSpecFormula formula) {
        drop();
    }

    @Override
    public void visit(AggSpecLast last) {
        keep();
    }

    @Override
    public void visit(AggSpecMax max) {
        keep();
    }

    @Override
    public void visit(AggSpecMedian median) {
        drop();
    }

    @Override
    public void visit(AggSpecMin min) {
        keep();
    }

    @Override
    public void visit(AggSpecPercentile pct) {
        drop();
    }

    @Override
    public void visit(AggSpecSortedFirst sortedFirst) {
        keep();
    }

    @Override
    public void visit(AggSpecSortedLast sortedLast) {
        keep();
    }

    @Override
    public void visit(AggSpecStd std) {
        drop();
    }

    @Override
    public void visit(AggSpecSum sum) {
        drop();
    }

    @Override
    public void visit(AggSpecUnique unique) {
        keep();
    }

    @Override
    public void visit(AggSpecWAvg wAvg) {
        drop();
    }

    @Override
    public void visit(AggSpecWSum wSum) {
        drop();
    }

    @Override
    public void visit(AggSpecVar var) {
        drop();
    }
}
