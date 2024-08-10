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
    // probably don't need this any more
    private final String[] dependencies;
    final ModifiedColumnSet myModifiedColumnSet;

    DependencyLayerBase(
            final SelectAndViewAnalyzer analyzer,
            final String name,
            final SelectColumn selectColumn,
            final ColumnSource<?> columnSource,
            final String[] dependencies,
            final ModifiedColumnSet mcsBuilder) {
        super(analyzer.getNextLayerIndex());
        this.name = name;
        this.selectColumn = selectColumn;
        selectColumnHoldsVector = Vector.class.isAssignableFrom(selectColumn.getReturnedType());
        this.columnSource = columnSource;
        this.dependencies = dependencies;
        final Set<String> remainingDepsToSatisfy = new HashSet<>(Arrays.asList(dependencies));
        analyzer.populateModifiedColumnSet(mcsBuilder, remainingDepsToSatisfy);
        this.myModifiedColumnSet = mcsBuilder;
    }

    @Override
    Set<String> getLayerColumnNames() {
        return Set.of(name);
    }

    @Override
    void populateModifiedColumnSetInReverse(
            final ModifiedColumnSet mcsBuilder,
            final Set<String> remainingDepsToSatisfy) {
        // Later-defined columns override earlier-defined columns. So we satisfy column dependencies "on the way
        // down" the recursion.
        if (remainingDepsToSatisfy.remove(name)) {
            // Caller had a dependency on us, so caller gets our dependencies
            mcsBuilder.setAll(myModifiedColumnSet);
        }
    }

    @Override
    void calcDependsOn(
            final Map<String, Set<String>> result,
            final boolean forcePublishAllSources) {

        final Set<String> thisResult = new HashSet<>();
        for (final String dep : dependencies) {
            final Set<String> innerDependencies = result.get(dep);
            if (innerDependencies == null) {
                // There are no further expansions of 'dep', so add it as a dependency.
                thisResult.add(dep);
            } else {
                // Instead of adding 'dep', add what 'dep' expands to.
                thisResult.addAll(innerDependencies);
            }
        }

        result.put(name, thisResult);
    }
}
