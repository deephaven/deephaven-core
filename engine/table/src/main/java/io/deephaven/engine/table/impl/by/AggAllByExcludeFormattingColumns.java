//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.spec.*;

class AggAllByExcludeFormattingColumns implements AggSpec.Visitor {

    public static boolean of(AggSpec spec) {
        return spec.walk(new AggAllByExcludeFormattingColumns()).out();
    }

    /**
     * {@code true} if we should drop formatting columns from the output, {@code false} otherwise, decided based on
     * aggregation operation. For example, {@link AggSpecDistinct} should keep formatting columns but {@link AggSpecSum}
     * should drop them because it doesn't make sense to apply formatting to sum but does make sense to apply the same
     * formatting to distinct values of the same column.
     */
    private boolean out;

    private AggAllByExcludeFormattingColumns() {}

    public boolean out() {
        return out;
    }

    private void keep() {
        out = false;
    }

    private void drop() {
        out = true;
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
