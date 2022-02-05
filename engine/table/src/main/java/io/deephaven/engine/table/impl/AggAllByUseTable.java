package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.spec.*;
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
    public void visit(AggSpecApproximatePercentile approxPct) {
        drop();
    }

    @Override
    public void visit(AggSpecAvg avg) {
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
    public void visit(AggSpecFirst first) {
        keep();
    }

    @Override
    public void visit(AggSpecFormula formula) {
        drop();
    }

    @Override
    public void visit(AggSpecFreeze freeze) {
        keep();
    }

    @Override
    public void visit(AggSpecGroup group) {
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
    public void visit(AggSpecTDigest tDigest) {
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
