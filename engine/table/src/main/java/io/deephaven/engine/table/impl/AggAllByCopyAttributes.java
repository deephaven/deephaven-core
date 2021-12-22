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
import io.deephaven.engine.table.impl.BaseTable.CopyAttributeOperation;

import java.util.Objects;

class AggAllByCopyAttributes implements AggSpec.Visitor {

    private final BaseTable parent;
    private final Table result;

    public AggAllByCopyAttributes(BaseTable parent, Table result) {
        this.parent = Objects.requireNonNull(parent);
        this.result = Objects.requireNonNull(result);
    }

    @Override
    public void visit(AggSpecAbsSum absSum) {}

    @Override
    public void visit(AggSpecCountDistinct countDistinct) {}

    @Override
    public void visit(AggSpecDistinct distinct) {}

    @Override
    public void visit(AggSpecGroup group) {}

    @Override
    public void visit(AggSpecAvg avg) {}

    @Override
    public void visit(AggSpecFirst first) {
        parent.copyAttributes(result, CopyAttributeOperation.FirstBy);
    }

    @Override
    public void visit(AggSpecFormula formula) {}

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
    public void visit(AggSpecUnique unique) {}

    @Override
    public void visit(AggSpecWAvg wAvg) {}

    @Override
    public void visit(AggSpecWSum wSum) {}

    @Override
    public void visit(AggSpecVar var) {}
}
