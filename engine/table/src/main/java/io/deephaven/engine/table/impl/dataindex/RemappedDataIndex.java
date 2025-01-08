//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link AbstractDataIndex} that remaps the key columns of another {@link AbstractDataIndex}. Used to implement
 * {@link io.deephaven.engine.table.DataIndex#remapKeyColumns(Map)}.
 */
public class RemappedDataIndex extends AbstractDataIndex implements DataIndexer.RetainableDataIndex {

    private final AbstractDataIndex sourceIndex;
    private final Map<ColumnSource<?>, ColumnSource<?>> oldToNewColumnMap;

    private final Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn;

    public static DataIndex from(
            @NotNull final AbstractDataIndex sourceIndex,
            @NotNull final Map<ColumnSource<?>, ColumnSource<?>> oldToNewColumnMap) {
        if (sourceIndex instanceof RemappedDataIndex) {
            // We should chain the remappings and delegate to the original source index.
            final RemappedDataIndex sourceAsRemapped = (RemappedDataIndex) sourceIndex;
            final Map<ColumnSource<?>, ColumnSource<?>> sourceOldToNewColumnMap = sourceAsRemapped.oldToNewColumnMap;
            final Map<ColumnSource<?>, ColumnSource<?>> chainedOldToNewColumnMap =
                    sourceAsRemapped.sourceIndex.keyColumnNamesByIndexedColumn().keySet().stream()
                            .filter((final ColumnSource<?> originalColumnSource) -> sourceOldToNewColumnMap
                                    .containsKey(originalColumnSource)
                                    || oldToNewColumnMap.containsKey(originalColumnSource))
                            .collect(Collectors.toMap(
                                    Function.identity(),
                                    (final ColumnSource<?> originalColumnSource) -> {
                                        final ColumnSource<?> sourceReplacement =
                                                sourceOldToNewColumnMap.get(originalColumnSource);
                                        if (sourceReplacement != null) {
                                            final ColumnSource<?> chainedReplacement =
                                                    oldToNewColumnMap.get(sourceReplacement);
                                            return chainedReplacement == null ? sourceReplacement : chainedReplacement;
                                        }
                                        return Objects.requireNonNull(oldToNewColumnMap.get(originalColumnSource));
                                    }));
            return new RemappedDataIndex(sourceAsRemapped.sourceIndex, chainedOldToNewColumnMap);
        }
        return new RemappedDataIndex(sourceIndex, oldToNewColumnMap);
    }

    private RemappedDataIndex(
            @NotNull final AbstractDataIndex sourceIndex,
            @NotNull final Map<ColumnSource<?>, ColumnSource<?>> oldToNewColumnMap) {
        this.sourceIndex = sourceIndex;
        this.oldToNewColumnMap = oldToNewColumnMap;
        // Build a new map of column sources to index table key column names using either the original column
        // sources or the remapped column sources.
        keyColumnNamesByIndexedColumn = sourceIndex.keyColumnNamesByIndexedColumn().entrySet().stream()
                .collect(Collectors.toMap(
                        (final Map.Entry<ColumnSource<?>, String> entry) -> {
                            // Use the remapped column source (or the original source if not remapped) as the key
                            final ColumnSource<?> oldColumnSource = entry.getKey();
                            final ColumnSource<?> newColumnSource = oldToNewColumnMap.get(oldColumnSource);
                            return newColumnSource != null ? newColumnSource : oldColumnSource;
                        },
                        Map.Entry::getValue,
                        Assert::neverInvoked,
                        LinkedHashMap::new));
        if (sourceIndex.isRefreshing()) {
            manage(sourceIndex);
        }
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
        return keyColumnNamesByIndexedColumn;
    }

    @Override
    @NotNull
    public List<String> keyColumnNames() {
        return sourceIndex.keyColumnNames();
    }

    @Override
    @NotNull
    public Table table() {
        return sourceIndex.table();
    }

    @Override
    @NotNull
    public RowKeyLookup rowKeyLookup() {
        return sourceIndex.rowKeyLookup();
    }

    @Override
    public boolean isRefreshing() {
        return sourceIndex.isRefreshing();
    }

    @Override
    public boolean isValid() {
        return sourceIndex.isValid();
    }

    @Override
    public boolean shouldRetain() {
        return DataIndexer.RetainableDataIndex.shouldRetain(sourceIndex);
    }
}
