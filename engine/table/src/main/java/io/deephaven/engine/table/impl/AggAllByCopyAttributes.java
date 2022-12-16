/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.spec.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable.CopyAttributeOperation;

import java.util.Objects;

class AggAllByCopyAttributes implements AggSpec.Visitor {

    private final BaseTable<?> parent;
    private final BaseTable<?> result;

    public AggAllByCopyAttributes(BaseTable<?> parent, BaseTable<?> result) {
        this.parent = Objects.requireNonNull(parent);
        this.result = Objects.requireNonNull(result);
    }

    @Override
    public void visit(AggSpecAbsSum absSum) {}

    @Override
    public void visit(AggSpecApproximatePercentile approxPct) {}

    @Override
    public void visit(AggSpecAvg avg) {}

    @Override
    public void visit(AggSpecCountDistinct countDistinct) {}

    @Override
    public void visit(AggSpecDistinct distinct) {}

    @Override
    public void visit(AggSpecFirst first) {
        parent.copyAttributes(result, CopyAttributeOperation.FirstBy);
    }

    @Override
    public void visit(AggSpecFormula formula) {}

    @Override
    public void visit(AggSpecFreeze freeze) {}

    @Override
    public void visit(AggSpecGroup group) {}

    @Override
    public void visit(AggSpecLast last) {
        parent.copyAttributes(result, CopyAttributeOperation.LastBy);
    }

    @Override
    public void visit(AggSpecMax max) {}

    @Override
    public void visit(AggSpecMedian median) {}

    @Override
    public void visit(AggSpecMin min) {}

    @Override
    public void visit(AggSpecPercentile pct) {}

    @Override
    public void visit(AggSpecSortedFirst sortedFirst) {}

    @Override
    public void visit(AggSpecSortedLast sortedLast) {}

    @Override
    public void visit(AggSpecStd std) {}

    @Override
    public void visit(AggSpecSum sum) {}

    @Override
    public void visit(AggSpecTDigest tDigest) {}

    @Override
    public void visit(AggSpecUnique unique) {}

    @Override
    public void visit(AggSpecWAvg wAvg) {}

    @Override
    public void visit(AggSpecWSum wSum) {}

    @Override
    public void visit(AggSpecVar var) {}
}
