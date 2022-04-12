package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.ColumnSource;

import java.util.*;

public abstract class DependencyLayerBase extends SelectAndViewAnalyzer {
    final SelectAndViewAnalyzer inner;
    final String name;
    final SelectColumn selectColumn;
    final boolean selectColumnHoldsVector;
    final ColumnSource<?> columnSource;
    // probably don't need this any more
    private final String[] dependencies;
    final ModifiedColumnSet myModifiedColumnSet;

    DependencyLayerBase(SelectAndViewAnalyzer inner, String name, SelectColumn selectColumn,
            ColumnSource<?> columnSource,
            String[] dependencies, ModifiedColumnSet mcsBuilder) {
        super(inner.getLayerIndex() + 1);
        this.inner = inner;
        this.name = name;
        this.selectColumn = selectColumn;
        selectColumnHoldsVector = Vector.class.isAssignableFrom(selectColumn.getReturnedType());
        this.columnSource = columnSource;
        this.dependencies = dependencies;
        final Set<String> remainingDepsToSatisfy = new HashSet<>(Arrays.asList(dependencies));
        inner.populateModifiedColumnSetRecurse(mcsBuilder, remainingDepsToSatisfy);
        this.myModifiedColumnSet = mcsBuilder;
    }


    @Override
    public void updateColumnDefinitionsFromTopLayer(Map<String, ColumnDefinition<?>> columnDefinitions) {
        final ColumnDefinition<?> cd =
                ColumnDefinition.fromGenericType(name, columnSource.getType(), columnSource.getComponentType());
        columnDefinitions.put(name, cd);
    }

    @Override
    void populateModifiedColumnSetRecurse(ModifiedColumnSet mcsBuilder, Set<String> remainingDepsToSatisfy) {
        // Later-defined columns override earlier-defined columns. So we satisfy column dependencies "on the way
        // down" the recursion.
        if (remainingDepsToSatisfy.remove(name)) {
            // Caller had a depenency on us, so caller gets our dependencies
            mcsBuilder.setAll(myModifiedColumnSet);
        }
        inner.populateModifiedColumnSetRecurse(mcsBuilder, remainingDepsToSatisfy);
    }

    @Override
    final Map<String, Set<String>> calcDependsOnRecurse() {
        final Map<String, Set<String>> result = inner.calcDependsOnRecurse();
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
        return result;
    }

    @Override
    public SelectAndViewAnalyzer getInner() {
        return inner;
    }

    @Override
    int getLayerIndexFor(String column) {
        if (name.equals(column)) {
            return getLayerIndex();
        }
        return inner.getLayerIndexFor(column);
    }

    @Override
    void setBaseBits(BitSet bitset) {
        inner.setBaseBits(bitset);
    }
}
