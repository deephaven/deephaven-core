package io.deephaven.engine.table.impl;

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
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable.CopyAttributeOperation;

import java.util.Objects;

class AggAllByCopyAttributes implements Key.Visitor {

    private final BaseTable parent;
    private final Table result;

    public AggAllByCopyAttributes(BaseTable parent, Table result) {
        this.parent = Objects.requireNonNull(parent);
        this.result = Objects.requireNonNull(result);
    }

    @Override
    public void visit(KeyAbsSum absSum) {}

    @Override
    public void visit(KeyCountDistinct countDistinct) {}

    @Override
    public void visit(KeyDistinct distinct) {}

    @Override
    public void visit(KeyGroup group) {}

    @Override
    public void visit(KeyAvg avg) {}

    @Override
    public void visit(KeyFirst first) {
        parent.copyAttributes(result, CopyAttributeOperation.FirstBy);
    }

    @Override
    public void visit(KeyLast last) {
        parent.copyAttributes(result, CopyAttributeOperation.LastBy);
    }

    @Override
    public void visit(KeyMax max) {}

    @Override
    public void visit(KeyMedian median) {}

    @Override
    public void visit(KeyMin min) {}

    @Override
    public void visit(KeyPct pct) {}

    @Override
    public void visit(KeySortedFirst sortedFirst) {}

    @Override
    public void visit(KeySortedLast sortedLast) {}

    @Override
    public void visit(KeyStd std) {}

    @Override
    public void visit(KeySum sum) {}

    @Override
    public void visit(KeyUnique unique) {}

    @Override
    public void visit(KeyWAvg wAvg) {}

    @Override
    public void visit(KeyWSum wSum) {}

    @Override
    public void visit(KeyVar var) {}
}
