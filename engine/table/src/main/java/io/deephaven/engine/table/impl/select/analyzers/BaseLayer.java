package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
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
        super(BASE_LAYER_INDEX);
        this.sources = sources;
        this.publishTheseSources = publishTheseSources;
    }

    @Override
    int getLayerIndexFor(String column) {
        if (sources.containsKey(column)) {
            return BASE_LAYER_INDEX;
        }
        throw new IllegalArgumentException("Unknown column: " + column);
    }

    @Override
    void setBaseBits(BitSet bitset) {
        bitset.set(BASE_LAYER_INDEX);
    }

    @Override
    public void setAllNewColumns(BitSet bitset) {
        bitset.set(BASE_LAYER_INDEX);
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
    public void applyUpdate(TableUpdate upstream, RowSet toClear, UpdateHelper helper, JobScheduler jobScheduler,
            SelectLayerCompletionHandler onCompletion) {
        // nothing to do at the base layer
        onCompletion.onLayerCompleted(BASE_LAYER_INDEX);
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

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{BaseLayer").append(", layerIndex=").append(getLayerIndex()).append("}");
    }

    @Override
    public boolean allowCrossColumnParallelization() {
        return true;
    }
}
