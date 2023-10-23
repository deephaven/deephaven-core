package io.deephaven.engine.table.impl.dataindex;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ImmutableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.WrappedRowSetRowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class DerivedDataIndex extends AbstractDataIndex {
    private static final int CHUNK_SIZE = 2048;
    @NotNull
    final DataIndex parentIndex;
    @Nullable
    final RowSet intersectRowSet;
    @Nullable
    final RowSet invertRowSet;
    final boolean sortByFirstRowKey;
    final boolean immutableResult;

    final Map<ColumnSource<?>, String> columnNameMap;

    private SoftReference<Table> cachedTable = new SoftReference<>(null);
    private long cachedTableStep = -1;

    private SoftReference<Table> cachedPrevTable = new SoftReference<>(null);
    private long cachedPrevTableStep = -1;

    private SoftReference<PositionLookup> cachedPositionLookup = new SoftReference<>(null);
    private long cachedPositionLookupStep = -1;

    public static DerivedDataIndex from(@NotNull final DataIndex index,
            @Nullable final RowSet intersectRowSet,
            @Nullable final RowSet invertRowSet,
            final boolean sortByFirstRowKey,
            @Nullable final Map<ColumnSource<?>, ColumnSource<?>> oldToNewMap,
            final boolean immutableResult) {
        return new DerivedDataIndex(index, intersectRowSet, invertRowSet, sortByFirstRowKey, oldToNewMap,
                immutableResult);
    }

    private DerivedDataIndex(@NotNull final DataIndex parentIndex,
            @Nullable final RowSet intersectRowSet,
            @Nullable final RowSet invertRowSet,
            final boolean sortByFirstRowKey,
            @Nullable final Map<ColumnSource<?>, ColumnSource<?>> oldToNewMap,
            final boolean immutableResult) {
        this.parentIndex = parentIndex;
        this.intersectRowSet = intersectRowSet;
        this.invertRowSet = invertRowSet;
        this.sortByFirstRowKey = sortByFirstRowKey;
        this.immutableResult = immutableResult;

        // Handle the column source remapping if needed.
        if (oldToNewMap != null && !oldToNewMap.isEmpty()) {
            // Build a new map of column sources to index table key column names using either the original column
            // sources or the remapped column sources.
            columnNameMap = new LinkedHashMap<>();

            for (Map.Entry<ColumnSource<?>, String> entry : parentIndex.keyColumnMap().entrySet()) {
                final ColumnSource<?> originalColumnSource = entry.getKey();
                // Use the remapped column source (or the original source if not remapped) as the key.
                columnNameMap.put(
                        oldToNewMap.getOrDefault(originalColumnSource, originalColumnSource),
                        entry.getValue());
            }
        } else {
            columnNameMap = null;
        }
    }

    @Override
    public String[] keyColumnNames() {
        return parentIndex.keyColumnNames();
    }

    @Override
    public Map<ColumnSource<?>, String> keyColumnMap() {
        return columnNameMap != null ? columnNameMap : parentIndex.keyColumnMap();
    }

    @Override
    public String rowSetColumnName() {
        return parentIndex.rowSetColumnName();
    }

    @Override
    public @Nullable Table table() {
        // Return a valid cached table if possible. If the index is static or was computed on this cycle, it remains
        // valid. Otherwise, we need to recompute the index from its parent.
        final Table cached = cachedTable.get();
        if (cached != null
                && (!isRefreshing() || cached.getUpdateGraph().clock().currentStep() == cachedTableStep)) {
            return cached;
        }

        // Get the parent index table and resolve our operations.
        Table resultTable = parentIndex.table();

        resultTable = maybeIntersectAndInvert(resultTable);
        resultTable = maybeSortByFirsKey(resultTable);
        resultTable = maybeMakeImmutable(resultTable);

        // Cache the result table.
        cachedTable = new SoftReference<>(resultTable);
        cachedTableStep = resultTable.getUpdateGraph().clock().currentStep();

        return resultTable;
    }

    @Override
    public @Nullable Table prevTable() {
        if (!isRefreshing()) {
            // This index is static, so prev==current
            return table();
        }

        // Return a valid cached table if possible. If the index is static or was computed on this cycle, it remains
        // valid. Otherwise, we need to recompute the index from its parent.
        final Table cached = cachedPrevTable.get();
        if (cached != null
                && (cached.getUpdateGraph().clock().currentStep() == cachedPrevTableStep)) {
            return cached;
        }

        // Get the parent index table and resolve our operations.
        Table resultTable = parentIndex.prevTable();

        resultTable = maybeIntersectAndInvert(resultTable);
        resultTable = maybeSortByFirsKey(resultTable);
        resultTable = maybeMakeImmutable(resultTable);

        // Cache the result table.
        cachedPrevTable = new SoftReference<>(resultTable);
        cachedPrevTableStep = resultTable.getUpdateGraph().clock().currentStep();

        return resultTable;
    }

    @Override
    public @Nullable RowSetLookup rowSetLookup() {
        // Assuming the parent lookup function is fast and efficient, we will leverage the parent's function
        // and apply the mutator to the retrieved result.
        final RowSetLookup lookup = parentIndex.rowSetLookup();
        if (intersectRowSet == null && invertRowSet == null) {
            // No need to mutate retrieved row set.
            return lookup;
        }

        final Function<RowSet, RowSet> mutator = getMutator(intersectRowSet, invertRowSet);
        return (Object o) -> {
            final RowSet rowSet = lookup.apply(o);
            return rowSet == null ? null : mutator.apply(rowSet);
        };
    }

    @Override
    public @NotNull PositionLookup positionLookup() {
        if (!mayModifyParentIndexRowSet()) {
            // We can use the parent lookup function directly because the operations being applied will not change
            // the index table row set and the key vs. position will be correct.
            return parentIndex.positionLookup();
        }

        // We need to build a lookup function from the table, either using a successive binary search function or
        // storing the keys in a hashmap.

        // Make sure we have a valid table on hand.
        final Table indexTable = table();

        // Return a valid cached lookup function if possible.
        final PositionLookup positionLookup = cachedPositionLookup.get();
        if (positionLookup != null
                && (!isRefreshing() || indexTable.getUpdateGraph().clock().currentStep() == cachedPositionLookupStep)) {
            return positionLookup;
        }

        // Decide whether to create a map or use a binary search strategy
        final PositionLookup newLookup;
        if (indexTable.size() >= BIN_SEARCH_THRESHOLD) {
            // Use a binary search strategy rather than consume memory for the hashmap.
            newLookup = buildPositionLookup(indexTable, keyColumnNames());
        } else {
            // Build a key to position hashmap from the table.
            TObjectIntHashMap<Object> lookupMap = buildPositionMap(indexTable, keyColumnNames());
            newLookup = lookupMap::get;
        }
        cachedPositionLookup = new SoftReference<>(newLookup);
        cachedPositionLookupStep = indexTable.getUpdateGraph().clock().currentStep();

        return newLookup;
    }

    @Override
    public boolean isRefreshing() {
        return parentIndex.isRefreshing();
    }

    @Override
    public DataIndex apply(@Nullable final RowSet intersectRowSet,
            @Nullable final RowSet invertRowSet,
            final boolean sortByFirstRowKey,
            @Nullable final Map<ColumnSource<?>, ColumnSource<?>> keyColumnRemap,
            final boolean immutableResult) {
        return DerivedDataIndex.from(this, intersectRowSet, invertRowSet, sortByFirstRowKey, keyColumnRemap,
                immutableResult);
    }

    /** Return true if the set of operations may modify the parent index table row set. **/
    private boolean mayModifyParentIndexRowSet() {
        return intersectRowSet != null || invertRowSet != null;
    }


    // region DataIndex materialization operations
    private static Function<RowSet, RowSet> getMutator(@Nullable final RowSet intersectRowSet,
            @Nullable final RowSet invertRowSet) {
        final Function<RowSet, RowSet> mutator;
        if (invertRowSet == null) {
            // Only intersect.
            mutator = rs -> rs.intersect(intersectRowSet);
        } else if (intersectRowSet == null) {
            // Only invert.
            mutator = index -> invertRowSet.invert(index);
        } else {
            // Intersect and invert.
            mutator = index -> {
                try (final WritableRowSet intersected = index.intersect(intersectRowSet)) {
                    return invertRowSet.invert(intersected);
                }
            };
        }
        return mutator;
    }

    /**
     * Apply strict intersection and invert operations as required by the deferred index.
     *
     * @param indexTable the table to apply the operations to.
     * @return the table with intersections and inversions applied.
     */
    private Table maybeIntersectAndInvert(@NotNull final Table indexTable) {
        if (intersectRowSet == null && invertRowSet == null) {
            return indexTable;
        }

        final Function<RowSet, RowSet> mutator = getMutator(intersectRowSet, invertRowSet);

        // Build a new table with redirected column sources for the key column(s) and an in-memory column for the
        // mutated output row sets.

        // noinspection unchecked
        final ColumnSource<RowSet> indexSource = indexTable.getColumnSource(INDEX_COL_NAME);

        final RowSetBuilderSequential redirectionBuilder = RowSetFactory.builderSequential();
        final ObjectArraySource<RowSet> resultIndexSource =
                (ObjectArraySource<RowSet>) ArrayBackedColumnSource.getMemoryColumnSource(RowSet.class, null);

        final int chunkSize = Math.min(CHUNK_SIZE, indexTable.intSize());
        try (final RowSequence.Iterator rsIt = indexTable.getRowSet().getRowSequenceIterator();
                final ChunkSource.GetContext rowSetCtx = indexSource.makeGetContext(chunkSize)) {

            long outputPosition = 0;
            while (rsIt.hasMore()) {
                final RowSequence rs = rsIt.getNextRowSequenceWithLength(chunkSize);
                resultIndexSource.ensureCapacity(outputPosition + rs.size());

                final LongChunk<OrderedRowKeys> rsChunk = rs.asRowKeyChunk();
                final ObjectChunk<RowSet, ? extends Values> indexChunk =
                        indexSource.getChunk(rowSetCtx, rs).asObjectChunk();

                for (int ii = 0; ii < rs.size(); ii++) {
                    final RowSet permutedRowSet = mutator.apply(indexChunk.get(ii));
                    if (permutedRowSet != null && permutedRowSet.isNonempty()) {
                        // We'll include this row set in the mutated result table.
                        resultIndexSource.set(outputPosition++, permutedRowSet);
                        redirectionBuilder.appendKey(rsChunk.get(ii));
                    }
                }
            }

            // Build the redirection row set, note that we may not actually use it.
            final WritableRowSet redirRowSet = redirectionBuilder.build();
            final Map<String, ColumnSource<?>> csm = new LinkedHashMap<>();

            if (redirRowSet.size() == indexTable.size()) {
                // We are including all rows from the index table, we don't need Redirected sources.
                for (Map.Entry<String, ? extends ColumnSource<?>> entry : indexTable.getColumnSourceMap().entrySet()) {
                    final String columnName = entry.getKey();
                    if (columnName.equals(INDEX_COL_NAME)) {
                        // Add the result row set column source.
                        csm.put(columnName, resultIndexSource);
                    } else {
                        csm.put(columnName, entry.getValue());
                    }
                }

                // Close the temporary row set.
                redirRowSet.close();
            } else {
                final WrappedRowSetRowRedirection redirection =
                        new WrappedRowSetRowRedirection(redirRowSet.toTracking());

                // Add a redirected column source for each key column.
                for (Map.Entry<String, ? extends ColumnSource<?>> entry : indexTable.getColumnSourceMap().entrySet()) {
                    final String columnName = entry.getKey();
                    if (columnName.equals(INDEX_COL_NAME)) {
                        // Add the result row set column source.
                        csm.put(columnName, resultIndexSource);
                    } else {
                        csm.put(columnName,
                                RedirectedColumnSource.maybeRedirect(redirection, entry.getValue()));
                    }
                }
            }

            return new QueryTable(RowSetFactory.flat(outputPosition).toTracking(), csm);
        }
    }

    /**
     * If requested, sort the input table by the first key of its index column.
     *
     * @param indexTable the input index table.
     * @return the table sorted by first key, if requested.
     */
    protected Table maybeSortByFirsKey(final @NotNull Table indexTable) {
        if (!sortByFirstRowKey) {
            return indexTable;
        }

        return indexTable.updateView("FirstKey=" + INDEX_COL_NAME + ".firstRowKey()")
                .sort("FirstKey")
                .dropColumns("FirstKey");
    }

    /**
     * If requested, return an immutable version of the index table.
     *
     * @param indexTable the input index table.
     * @return the table sorted by first key, if requested.
     */
    protected Table maybeMakeImmutable(final @NotNull Table indexTable) {
        if (!immutableResult) {
            return indexTable;
        }

        // Make a static copy of the table with immutable column sources.
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        final long size = indexTable.size();

        final ArrayList<ColumnSource<?>> cloneSources = new ArrayList<>();
        final ArrayList<WritableColumnSource<?>> cloneDestinations = new ArrayList<>();

        for (final Map.Entry<String, ? extends ColumnSource<?>> entry : indexTable.getColumnSourceMap().entrySet()) {
            final String columnName = entry.getKey();
            final ColumnSource<?> columnSource = entry.getValue();

            if (columnSource instanceof ImmutableColumnSource) {
                // We can use this directly, no need to clone.
                columnSourceMap.put(columnName, columnSource);
            } else {
                // Create a new immutable column source and copy all the data.
                final WritableColumnSource<?> immutableColumnSource =
                        InMemoryColumnSource.getImmutableMemoryColumnSource(size, columnSource.getType(),
                                columnSource.getComponentType());
                cloneSources.add(columnSource);
                cloneDestinations.add(immutableColumnSource);
            }
        }

        final WritableRowSet outputRowSet = RowSetFactory.flat(size);
        if (!cloneSources.isEmpty()) {
            ColumnSource<?>[] cloneSourceArr = (ColumnSource<?>[]) cloneSources.toArray();
            WritableColumnSource<?>[] cloneDestinationArr = (WritableColumnSource<?>[]) cloneDestinations.toArray();

            ChunkUtils.copyData(cloneSourceArr, indexTable.getRowSet(),
                    cloneDestinationArr, outputRowSet,
                    false);
        }

        return new QueryTable(outputRowSet.toTracking(), columnSourceMap);
    }

    // endregion DataIndex materialization operations
}
