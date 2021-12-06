package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;

import java.util.*;

public class BaseLayer extends SelectAndViewAnalyzer {
    private final Map<String, ColumnSource<?>> sources;
    private final boolean publishTheseSources;

    BaseLayer(Map<String, ColumnSource<?>> sources, boolean publishTheseSources) {
        this.sources = sources;
        this.publishTheseSources = publishTheseSources;
    }

    @Override
    void populateModifiedColumnSetRecurse(ModifiedColumnSet mcsBuilder, Set<String> remainingDepsToSatisfy) {
        mcsBuilder.setAll(remainingDepsToSatisfy.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    @Override
    final Map<String, ColumnSource<?>> getColumnSourcesRecurse(GetMode mode) {
        // We specifically return a LinkedHashMap so the columns get populated in order
        final Map<String, ColumnSource<?>> result = new LinkedHashMap<>();
        if (mode == GetMode.All || (mode == GetMode.Published && publishTheseSources)) {
            result.putAll(sources);
        }
        return result;
    }

    @Override
    public void updateColumnDefinitionsFromTopLayer(Map<String, ColumnDefinition<?>> columnDefinitions) {
        for (Map.Entry<String, ColumnSource<?>> entry : sources.entrySet()) {
            final String name = entry.getKey();
            final ColumnSource<?> cs = entry.getValue();
            final ColumnDefinition<?> cd = ColumnDefinition.fromGenericType(name, cs.getType(), cs.getComponentType());
            columnDefinitions.put(name, cd);
        }
    }

    @Override
    public void applyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper) {
        // nothing to do at the base layer
    }

    @Override
    final Map<String, Set<String>> calcDependsOnRecurse() {
        final Map<String, Set<String>> result = new HashMap<>();
        if (publishTheseSources) {
            for (final String col : sources.keySet()) {
                result.computeIfAbsent(col, dummy -> new HashSet<>()).add(col);
            }
        }
        return result;
    }

    @Override
    public SelectAndViewAnalyzer getInner() {
        return null;
    }

    @Override
    public void startTrackingPrev() {
        // nothing to do
    }
}
