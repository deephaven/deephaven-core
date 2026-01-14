//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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

    DependencyLayerBase(
            final SelectAndViewAnalyzer.AnalyzerContext context,
            final SelectColumn selectColumn,
            final ColumnSource<?> columnSource,
            final String[] recomputeDependencies,
            final ModifiedColumnSet mcsBuilder) {
        super(context.getNextLayerIndex());
        this.name = selectColumn.getName();
        this.selectColumn = selectColumn;
        selectColumnHoldsVector = Vector.class.isAssignableFrom(selectColumn.getReturnedType());
        this.columnSource = columnSource;
        context.populateParentDependenciesMCS(mcsBuilder, recomputeDependencies);
        if (selectColumn.recomputeOnModifiedRow()) {
            mcsBuilder.setAll(ModifiedColumnSet.ALL);
        }
        this.myModifiedColumnSet = mcsBuilder;
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
