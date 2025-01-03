//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ForkJoinPoolOperationInitializer;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.select.FunctionalColumnLong;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TransformedDataIndex extends LivenessArtifact implements BasicDataIndex {

    @NotNull
    private final DataIndex parentIndex;
    private final boolean isRefreshing;

    private DataIndexTransformer transformer;

    private volatile Table indexTable;

    public static TransformedDataIndex from(
            @NotNull final DataIndex index,
            @NotNull final DataIndexTransformer transformer) {
        return new TransformedDataIndex(index, transformer);
    }

    private TransformedDataIndex(
            @NotNull final DataIndex parentIndex,
            @NotNull final DataIndexTransformer transformer) {
        this.parentIndex = parentIndex;
        isRefreshing = !transformer.snapshotResult() && parentIndex.isRefreshing();
        this.transformer = transformer;
    }

    @Override
    @NotNull
    public List<String> keyColumnNames() {
        return parentIndex.keyColumnNames();
    }

    @Override
    @NotNull
    public Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
        return parentIndex.keyColumnNamesByIndexedColumn();
    }

    @Override
    @NotNull
    public String rowSetColumnName() {
        return parentIndex.rowSetColumnName();
    }

    @Override
    @NotNull
    public Table table(final DataIndexOptions unused) {
        Table localIndexTable;
        if ((localIndexTable = indexTable) != null) {
            return localIndexTable;
        }
        synchronized (this) {
            if ((localIndexTable = indexTable) != null) {
                return localIndexTable;
            }
            indexTable = localIndexTable = QueryPerformanceRecorder.withNugget("Transform Data Index",
                    ForkJoinPoolOperationInitializer.ensureParallelizable(this::buildTable));
            // Don't hold onto the transformer after the index table is computed, we don't need to maintain
            // reachability for its RowSets anymore.
            transformer = null;
            return localIndexTable;
        }
    }

    private Table buildTable() {
        try (final SafeCloseable ignored = parentIndex.isRefreshing() ? LivenessScopeStack.open() : null) {
            Table localIndexTable = parentIndex.table();
            localIndexTable = maybeIntersectAndInvert(localIndexTable);
            localIndexTable = maybeSortByFirstKey(localIndexTable);
            localIndexTable = localIndexTable.isRefreshing() && transformer.snapshotResult()
                    ? localIndexTable.snapshot()
                    : localIndexTable;

            if (localIndexTable.isRefreshing()) {
                manage(localIndexTable);
            }
            return localIndexTable;
        }
    }

    @Override
    public boolean isRefreshing() {
        return isRefreshing;
    }

    // region DataIndex materialization operations

    private static Function<RowSet, RowSet> getMutator(
            @Nullable final RowSet intersectRowSet,
            @Nullable final RowSet invertRowSet) {
        final Function<RowSet, RowSet> mutator;
        if (invertRowSet == null) {
            assert intersectRowSet != null;
            // Only intersect
            mutator = (final RowSet rowSet) -> {
                final WritableRowSet intersected = rowSet.intersect(intersectRowSet);
                if (intersected.isEmpty()) {
                    intersected.close();
                    return null;
                }
                return intersected;
            };
        } else if (intersectRowSet == null) {
            // Only invert
            mutator = invertRowSet::invert;
        } else {
            // Intersect and invert
            mutator = (final RowSet rowSet) -> {
                try (final WritableRowSet intersected = rowSet.intersect(intersectRowSet)) {
                    if (intersected.isEmpty()) {
                        return null;
                    }
                    return invertRowSet.invert(intersected);
                }
            };
        }
        return mutator;
    }

    /**
     * Apply the intersect and invert transformations to the input index table, if specified.
     *
     * @param indexTable The input index table
     * @return The table with intersections and inversions applied.
     */
    private Table maybeIntersectAndInvert(@NotNull final Table indexTable) {
        Assert.neqNull(transformer, "transformer");
        if (transformer.intersectRowSet().isEmpty() && transformer.invertRowSet().isEmpty()) {
            return indexTable;
        }
        final Function<RowSet, RowSet> mutator =
                getMutator(transformer.intersectRowSet().orElse(null), transformer.invertRowSet().orElse(null));
        final Table mutated = indexTable
                .update(List.of(SelectColumn.ofStateless(new FunctionalColumn<>(
                        parentIndex.rowSetColumnName(), RowSet.class,
                        parentIndex.rowSetColumnName(), RowSet.class,
                        mutator))));
        if (transformer.intersectRowSet().isPresent()) {
            return mutated.where(Filter.isNotNull(ColumnName.of(parentIndex.rowSetColumnName())));
        }
        return mutated;
    }

    /**
     * Sort the input index table by the first row key of its RowSet column, if specified.
     *
     * @param indexTable The input index table
     * @return The table sorted by first row key, if requested, else the input index table
     */
    protected Table maybeSortByFirstKey(final @NotNull Table indexTable) {
        Assert.neqNull(transformer, "transformer");
        if (!transformer.sortByFirstRowKey()) {
            return indexTable;
        }
        return indexTable
                .updateView(List.of(new FunctionalColumnLong<>(
                        parentIndex.rowSetColumnName(), RowSet.class,
                        "__FRK__", RowSet::firstRowKey)))
                .sort("__FRK__")
                .dropColumns("__FRK__");
    }

    // endregion DataIndex materialization operations
}
