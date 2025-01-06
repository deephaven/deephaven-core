//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.vector.Vector;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ColumnSource;

import java.util.*;

public abstract class DependencyLayerBase extends SelectAndViewAnalyzer.Layer {
    final String name;
    final SelectColumn selectColumn;
    final boolean selectColumnHoldsVector;
    final ColumnSource<?> columnSource;
    final ModifiedColumnSet myModifiedColumnSet;
    final BitSet myLayerDependencySet;

    DependencyLayerBase(
            final SelectAndViewAnalyzer.AnalyzerContext context,
            final SelectColumn selectColumn,
            final ColumnSource<?> columnSource,
            final String[] dependencies,
            final ModifiedColumnSet mcsBuilder) {
        super(context.getNextLayerIndex());
        this.name = selectColumn.getName();
        this.selectColumn = selectColumn;
        selectColumnHoldsVector = Vector.class.isAssignableFrom(selectColumn.getReturnedType());
        this.columnSource = columnSource;
        context.populateParentDependenciesMCS(mcsBuilder, dependencies);
        if (selectColumn.recomputeOnModifiedRow()) {
            mcsBuilder.setAll(ModifiedColumnSet.ALL);
        }
        this.myModifiedColumnSet = mcsBuilder;
        this.myLayerDependencySet = new BitSet();
        context.populateLayerDependencySet(myLayerDependencySet, dependencies);
    }

    @Override
    Set<String> getLayerColumnNames() {
        return Set.of(name);
    }

    @Override
    public ModifiedColumnSet getModifiedColumnSet() {
        return myModifiedColumnSet;
    }
}
