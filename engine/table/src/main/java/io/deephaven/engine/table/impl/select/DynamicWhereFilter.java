//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.setinclusion.SetInclusionKernel;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

/**
 * A where filter that extracts a set of inclusion or exclusion keys from a set table.
 * <p>
 * Each time the set table ticks, the entire where filter is recalculated.
 */
public class DynamicWhereFilter extends WhereFilterLivenessArtifactImpl implements NotificationQueue.Dependency {

    private static final int CHUNK_SIZE = 1 << 16;

    private final MatchPair[] sourceToSetColumnNamePairs;
    private final boolean inclusion;

    @SuppressWarnings("FieldCanBeLocal")
    @ReferentialIntegrity
    private final InstrumentedTableUpdateListener setUpdateListener;

    private final SetInclusionKernel setKernel;
    private final Class<?> @NotNull [] setKeyTypes;

    private final QueryTable setTable;
    private List<Object> staticSetLookupKeys;

    private ColumnSource<?>[] sourceKeyColumns;
    private TupleSource<Object> sourceKeySource;
    /**
     * The optimal data index for this filter.
     */
    private @Nullable DataIndex sourceDataIndex;
    private int @Nullable [] tupleToIndexMap;
    private int @Nullable [] indexToTupleMap;

    private RecomputeListener listener;
    private QueryTable resultTable;

    public DynamicWhereFilter(
            @NotNull final QueryTable setTable,
            final boolean inclusion,
            final MatchPair... sourceToSetColumnNamePairs) {
        if (setTable.isRefreshing()) {
            updateGraph.checkInitiateSerialTableOperation();
        }
        this.sourceToSetColumnNamePairs = sourceToSetColumnNamePairs;
        this.inclusion = inclusion;

        // Use reinterpreted column sources for the set table tuple source.
        final ColumnSource<?>[] setColumns = Arrays.stream(this.sourceToSetColumnNamePairs)
                .map(mp -> setTable.getColumnSource(mp.rightColumn()))
                .map(ReinterpretUtils::maybeConvertToPrimitive)
                .toArray(ColumnSource[]::new);
        setKeyTypes = Arrays.stream(setColumns).map(ColumnSource::getType).toArray(Class[]::new);
        final TupleSource<?> setKeySource = TupleSourceFactory.makeTupleSource(setColumns);
        setKernel = SetInclusionKernel.makeKernel(setKeySource.getChunkType(), inclusion);

        // Fill liveValues and the set kernel with the initial keys from the set table.
        if (setTable.getRowSet().isNonempty()) {
            try (final CloseableIterator<?> initialKeysIterator = ChunkedColumnIterator.make(
                    setKeySource, setTable.getRowSet(), getChunkSize(setTable.getRowSet()))) {
                initialKeysIterator.forEachRemaining(this::addKeyUnchecked);
            }
        }

        if (setTable.isRefreshing()) {
            this.setTable = setTable;

            final String[] setColumnNames =
                    Arrays.stream(this.sourceToSetColumnNamePairs).map(MatchPair::rightColumn).toArray(String[]::new);
            final ModifiedColumnSet setColumnsMCS = setTable.newModifiedColumnSet(setColumnNames);
            setUpdateListener = new InstrumentedTableUpdateListenerAdapter(
                    "DynamicWhereFilter(" + Arrays.toString(sourceToSetColumnNamePairs) + ")", setTable, false) {

                @Override
                public void onUpdate(final TableUpdate upstream) {
                    final boolean hasAdds = upstream.added().isNonempty();
                    final boolean hasRemoves = upstream.removed().isNonempty();
                    final boolean hasModifies = upstream.modified().isNonempty()
                            && upstream.modifiedColumnSet().containsAny(setColumnsMCS);
                    if (!hasAdds && !hasRemoves && !hasModifies) {
                        return;
                    }

                    // Remove removed keys
                    if (hasRemoves) {
                        try (final CloseableIterator<?> removedKeysIterator = ChunkedColumnIterator.make(
                                setKeySource.getPrevSource(), upstream.removed(), getChunkSize(upstream.removed()))) {
                            removedKeysIterator.forEachRemaining(DynamicWhereFilter.this::removeKey);
                        }
                    }

                    // Update modified keys
                    boolean trueModification = false;
                    if (hasModifies) {
                        // @formatter:off
                        try (final CloseableIterator<?> preModifiedKeysIterator = ChunkedColumnIterator.make(
                                     setKeySource.getPrevSource(), upstream.getModifiedPreShift(),
                                     getChunkSize(upstream.getModifiedPreShift()));
                             final CloseableIterator<?> postModifiedKeysIterator = ChunkedColumnIterator.make(
                                     setKeySource, upstream.modified(),
                                     getChunkSize(upstream.modified()))) {
                            // @formatter:on
                            while (preModifiedKeysIterator.hasNext()) {
                                Assert.assertion(postModifiedKeysIterator.hasNext(),
                                        "Pre and post modified row sets must be the same size; post is exhausted, but pre is not");
                                final Object oldKey = preModifiedKeysIterator.next();
                                final Object newKey = postModifiedKeysIterator.next();
                                if (!Objects.equals(oldKey, newKey)) {
                                    trueModification = true;
                                    removeKey(oldKey);
                                    addKey(newKey);
                                }
                            }
                            Assert.assertion(!postModifiedKeysIterator.hasNext(),
                                    "Pre and post modified row sets must be the same size; pre is exhausted, but post is not");
                        }
                    }

                    // Add added keys
                    if (hasAdds) {
                        try (final CloseableIterator<?> addedKeysIterator = ChunkedColumnIterator.make(
                                setKeySource, upstream.added(), getChunkSize(upstream.added()))) {
                            addedKeysIterator.forEachRemaining(DynamicWhereFilter.this::addKey);
                        }
                    }

                    // Pretend every row of the original table was modified, this is essential so that the where clause
                    // can be re-evaluated based on the updated live set.
                    if (listener != null) {
                        if (hasAdds || trueModification) {
                            if (inclusion) {
                                listener.requestRecomputeUnmatched();
                            } else {
                                listener.requestRecomputeMatched();
                            }
                        }
                        if (hasRemoves || trueModification) {
                            if (inclusion) {
                                listener.requestRecomputeMatched();
                            } else {
                                listener.requestRecomputeUnmatched();
                            }
                        }
                    }
                }

                @Override
                public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                    if (listener != null) {
                        resultTable.notifyListenersOnError(originalException, sourceEntry);
                    }
                }
            };
            setTable.addUpdateListener(setUpdateListener);

            manage(setUpdateListener);
        } else {
            this.setTable = null;
            setUpdateListener = null;
        }
    }

    /**
     * "Copy constructor" for DynamicWhereFilter's with static set tables.
     */
    private DynamicWhereFilter(
            @NotNull final Class<?> @NotNull [] setKeyTypes,
            @NotNull final SetInclusionKernel setKernel,
            final boolean inclusion,
            final MatchPair... sourceToSetColumnNamePairs) {
        this.setKeyTypes = setKeyTypes;
        this.setKernel = setKernel;
        this.inclusion = inclusion;
        this.sourceToSetColumnNamePairs = sourceToSetColumnNamePairs;
        setTable = null;
        setUpdateListener = null;
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    private void removeKey(Object key) {
        if (!setKernel.remove(key)) {
            throw new RuntimeException("Inconsistent state, key not found in set: " + key);
        }
    }

    private void addKey(Object key) {
        if (!setKernel.add(key)) {
            throw new RuntimeException("Inconsistent state, key already in set:" + key);
        }
    }

    private void addKeyUnchecked(Object key) {
        setKernel.add(key);
    }

    /**
     * {@inheritDoc}
     * <p>
     * If {@code sourceTable#isRefreshing()}, this method must only be invoked when it's
     * {@link UpdateGraph#checkInitiateSerialTableOperation() safe} to initialize serial table operations.
     */
    @Override
    public SafeCloseable beginOperation(@NotNull final Table sourceTable) {
        if (sourceDataIndex != null) {
            throw new IllegalStateException("Inputs already initialized, use copy() instead of re-using a WhereFilter");
        }
        getUpdateGraph(this, sourceTable);
        final String[] keyColumnNames = MatchPair.getLeftColumns(sourceToSetColumnNamePairs);
        sourceKeyColumns = Arrays.stream(sourceToSetColumnNamePairs)
                .map(mp -> sourceTable.getColumnSource(mp.leftColumn())).toArray(ColumnSource[]::new);
        try (final SafeCloseable ignored = sourceTable.isRefreshing() ? LivenessScopeStack.open() : null) {
            sourceDataIndex = optimalIndex(sourceTable, keyColumnNames);
            if (sourceDataIndex != null) {
                if (sourceDataIndex.isRefreshing()) {
                    manage(sourceDataIndex);
                }
                computeTupleIndexMaps();
            }
        }

        final ColumnSource<?>[] reinterpretedSourceKeyColumns = Arrays.stream(sourceKeyColumns)
                .map(ReinterpretUtils::maybeConvertToPrimitive)
                .toArray(ColumnSource[]::new);
        for (int ki = 0; ki < setKeyTypes.length; ++ki) {
            if (setKeyTypes[ki] != reinterpretedSourceKeyColumns[ki].getType()) {
                throw new IllegalArgumentException(String.format(
                        "Reinterpreted key type mismatch: (set key type) %s != %s (source key type)",
                        setKeyTypes[ki], reinterpretedSourceKeyColumns[ki].getType()));
            }
        }
        sourceKeySource = TupleSourceFactory.makeTupleSource(reinterpretedSourceKeyColumns);

        if (setTable == null // Set table is static
                && staticSetLookupKeys == null // We haven't already computed the lookup keys
                && sourceDataIndex != null // We might use the lookup keys if we compute them
                && sourceDataIndex.isRefreshing() // We might use the lookup keys more than once
                && sourceKeyColumns.length > 1 // Making a lookup key is more complicated than boxing a primitive
        ) {
            // Convert the tuples in liveValues to be lookup keys in the sourceDataIndex
            staticSetLookupKeys = new ArrayList<>(setKernel.size());
            final int indexKeySize = sourceDataIndex.keyColumns().length;
            if (indexKeySize > 1) {
                final Function<Object, Object> keyMappingFunction = indexKeySize == keyColumnNames.length
                        ? tupleToFullKeyMappingFunction()
                        : tupleToPartialKeyMappingFunction();

                setKernel.iterator().forEachRemaining(key -> {
                    final Object[] lookupKey = (Object[]) keyMappingFunction.apply(key);
                    // Store a copy because the mapping function returns the same array each invocation.
                    staticSetLookupKeys.add(Arrays.copyOf(lookupKey, indexKeySize));
                });
            } else {
                final int keyOffset = indexToTupleMap == null ? 0 : indexToTupleMap[0];
                setKernel.iterator().forEachRemaining(
                        key -> staticSetLookupKeys.add(sourceKeySource.exportElement(key, keyOffset)));
            }
        }

        return () -> {
        };
    }

    /**
     * Returns the optimal data index for the supplied table, or null if no index is available. The ideal index would
     * contain all key columns but a partial match is also acceptable.
     */
    @Nullable
    private static DataIndex optimalIndex(final Table inputTable, final String[] keyColumnNames) {
        final DataIndex fullIndex = DataIndexer.getDataIndex(inputTable, keyColumnNames);
        if (fullIndex != null) {
            return fullIndex;
        }
        return DataIndexer.getOptimalPartialIndex(inputTable, keyColumnNames);
    }

    /**
     * Calculates mappings from the offset of a {@link ColumnSource} in the {@code sourceDataIndex} to the offset of the
     * corresponding {@link ColumnSource} in the key sources from the set or source table of a DynamicWhereFilter
     * ({@code indexToTupleMap}, as well as the reverse ({@code tupleToIndexMap}). This allows for mapping keys from the
     * {@link #setKernel} to keys in the {@link #sourceDataIndex}.
     */
    private void computeTupleIndexMaps() {
        assert sourceDataIndex != null;

        if (sourceDataIndex.keyColumns().length == 1) {
            // Trivial mapping, no need to compute anything.
            return;
        }

        final ColumnSource<?>[] dataIndexSources = sourceDataIndex
                .keyColumnNamesByIndexedColumn()
                .keySet()
                .toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);

        // Bi-directional mapping (note that the sizes can be different, e.g. partial matching).
        final int[] tupleToIndexMap = new int[sourceKeyColumns.length];
        final int[] indexToTupleMap = new int[dataIndexSources.length];

        // Fill with -1 to indicate no mapping. This value will remain in the case of a partial index to indicate that
        // the key column is not in the index, thereby poisoning any mistaken attempt to use the mapping.
        Arrays.fill(tupleToIndexMap, -1);

        boolean sameOrder = true;

        // The tuples will be in sourceKeyColumns order (same as set table key columns order). We need to find the
        // dataIndex offset for each key source.
        for (int ii = 0; ii < sourceKeyColumns.length; ++ii) {
            for (int jj = 0; jj < dataIndexSources.length; ++jj) {
                if (sourceKeyColumns[ii] == dataIndexSources[jj]) {
                    tupleToIndexMap[ii] = jj;
                    indexToTupleMap[jj] = ii;
                    sameOrder &= ii == jj;
                    break;
                }
            }
        }

        // Return null if the map is the identity map
        this.tupleToIndexMap = sameOrder ? null : tupleToIndexMap;
        this.indexToTupleMap = sameOrder ? null : indexToTupleMap;
    }

    @NotNull
    private Function<Object, Object> tupleToFullKeyMappingFunction() {
        final Object[] keysInDataIndexOrder = new Object[sourceKeyColumns.length];
        if (tupleToIndexMap == null) {
            return (final Object tupleKey) -> {
                sourceKeySource.exportAllTo(keysInDataIndexOrder, tupleKey);
                return keysInDataIndexOrder;
            };
        }
        return (final Object tupleKey) -> {
            sourceKeySource.exportAllTo(keysInDataIndexOrder, tupleKey, tupleToIndexMap);
            return keysInDataIndexOrder;
        };
    }

    @NotNull
    private Function<Object, Object> tupleToPartialKeyMappingFunction() {
        assert sourceDataIndex != null;

        final int partialKeySize = sourceDataIndex.keyColumns().length;

        // This function is not needed when the partial key is a single column and should not be called.
        Assert.gt(partialKeySize, "partialKeySize", 1);

        final Object[] keysInDataIndexOrder = new Object[partialKeySize];
        if (indexToTupleMap == null) {
            return (final Object tupleKey) -> {
                for (int ii = 0; ii < partialKeySize; ++ii) {
                    keysInDataIndexOrder[ii] = sourceKeySource.exportElement(tupleKey, ii);
                }
                return keysInDataIndexOrder;
            };
        } else {
            return (final Object tupleKey) -> {
                for (int ii = 0; ii < partialKeySize; ++ii) {
                    keysInDataIndexOrder[ii] = sourceKeySource.exportElement(tupleKey, indexToTupleMap[ii]);
                }
                return keysInDataIndexOrder;
            };
        }
    }

    @Override
    public List<String> getColumns() {
        return Arrays.asList(MatchPair.getLeftColumns(sourceToSetColumnNamePairs));
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {}

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        if (sourceDataIndex != null) {
            // Does our index contain every key column?

            if (sourceDataIndex.keyColumnNames().size() == sourceKeyColumns.length) {
                // Even if we have an index, we may be better off with a linear search.
                if (selection.size() > (sourceDataIndex.table().size() * 2L)) {
                    return filterFullIndex(selection);
                } else {
                    return filterLinear(selection, inclusion);
                }
            }

            // We have a partial index, should we use it?
            if (selection.size() > (sourceDataIndex.table().size() * 4L)) {
                return filterPartialIndex(selection);
            }
        }
        return filterLinear(selection, inclusion);
    }

    @NotNull
    private WritableRowSet filterFullIndex(@NotNull final RowSet selection) {
        Assert.neqNull(sourceDataIndex, "sourceDataIndex");

        final WritableRowSet filtered = inclusion ? RowSetFactory.empty() : selection.copy();
        // noinspection DataFlowIssue
        final DataIndex.RowKeyLookup rowKeyLookup = sourceDataIndex.rowKeyLookup();
        final ColumnSource<RowSet> rowSetColumn = sourceDataIndex.rowSetColumn();

        final Iterator<Object> values;
        final Function<Object, Object> keyMappingFunction;
        if (staticSetLookupKeys != null) {
            values = staticSetLookupKeys.iterator();
            keyMappingFunction = Function.identity();
        } else if (sourceKeyColumns.length == 1) {
            values = setKernel.iterator();
            keyMappingFunction = Function.identity();
        } else {
            values = setKernel.iterator();
            keyMappingFunction = tupleToFullKeyMappingFunction();
        }

        values.forEachRemaining(key -> {
            final Object mappedKey = keyMappingFunction.apply(key);
            final long rowKey = rowKeyLookup.apply(mappedKey, false);
            final RowSet rowSet = rowSetColumn.get(rowKey);
            if (rowSet != null) {
                if (inclusion) {
                    try (final RowSet intersected = rowSet.intersect(selection)) {
                        filtered.insert(intersected);
                    }
                } else {
                    filtered.remove(rowSet);
                }
            }
        });
        return filtered;
    }

    @NotNull
    private WritableRowSet filterPartialIndex(@NotNull final RowSet selection) {
        Assert.neqNull(sourceDataIndex, "sourceDataIndex");
        Assert.gt(sourceKeyColumns.length, "sourceKeyColumns.length", 1);

        final WritableRowSet matching;
        try (final WritableRowSet possiblyMatching = RowSetFactory.empty()) {
            // First, compute a possibly-matching subset of selection based on the partial index.

            // noinspection DataFlowIssue
            final DataIndex.RowKeyLookup rowKeyLookup = sourceDataIndex.rowKeyLookup();
            final ColumnSource<RowSet> rowSetColumn = sourceDataIndex.rowSetColumn();

            final Iterator<Object> values;
            final Function<Object, Object> keyMappingFunction;

            if (staticSetLookupKeys != null) {
                values = staticSetLookupKeys.iterator();
                keyMappingFunction = Function.identity();
            } else {
                values = setKernel.iterator();
                if (sourceDataIndex.keyColumnNames().size() == 1) {
                    final int keyOffset = indexToTupleMap == null ? 0 : indexToTupleMap[0];
                    keyMappingFunction = (final Object key) -> sourceKeySource.exportElement(key, keyOffset);
                } else {
                    keyMappingFunction = tupleToPartialKeyMappingFunction();
                }
            }

            values.forEachRemaining(key -> {
                final Object lookupKey = keyMappingFunction.apply(key);
                final long rowKey = rowKeyLookup.apply(lookupKey, false);
                final RowSet rowSet = rowSetColumn.get(rowKey);
                if (rowSet != null) {
                    try (final RowSet intersected = rowSet.intersect(selection)) {
                        possiblyMatching.insert(intersected);
                    }
                }
            });

            // Now, do linear filter on possiblyMatching to determine the values to include or exclude from selection.
            matching = filterLinear(possiblyMatching, true);
        }
        if (inclusion) {
            return matching;
        }
        try (final SafeCloseable ignored = matching) {
            return selection.minus(matching);
        }
    }

    private WritableRowSet filterLinear(final RowSet selection, final boolean filterInclusion) {
        if (selection.isEmpty()) {
            return RowSetFactory.empty();
        }

        final RowSetBuilderSequential filteredRowSetBuilder = RowSetFactory.builderSequential();

        final int maxChunkSize = getChunkSize(selection);
        // @formatter:off
        try (final ColumnSource.GetContext keyGetContext = sourceKeySource.makeGetContext(maxChunkSize);
             final RowSequence.Iterator selectionIterator = selection.getRowSequenceIterator();
             final WritableLongChunk<OrderedRowKeys> matchingKeys = WritableLongChunk.makeWritableChunk(maxChunkSize)) {
            // @formatter:on

            while (selectionIterator.hasMore()) {
                final RowSequence selectionChunk = selectionIterator.getNextRowSequenceWithLength(maxChunkSize);
                final LongChunk<OrderedRowKeys> selectionRowKeyChunk = selectionChunk.asRowKeyChunk();
                final Chunk<Values> keyChunk = Chunk.downcast(sourceKeySource.getChunk(keyGetContext, selectionChunk));
                setKernel.matchValues(keyChunk, selectionRowKeyChunk, matchingKeys, filterInclusion);
                filteredRowSetBuilder.appendOrderedRowKeysChunk(matchingKeys);
            }
        }

        return filteredRowSetBuilder.build();
    }

    private static int getChunkSize(@NotNull final RowSet selection) {
        return (int) Math.min(selection.size(), CHUNK_SIZE);
    }

    @Override
    public boolean isSimpleFilter() {
        /* This doesn't execute any user code, so it should be safe to execute it against untrusted data. */
        return true;
    }

    @Override
    public boolean isRefreshing() {
        return setUpdateListener != null;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {
        this.listener = listener;
        this.resultTable = listener.getTable();
        if (isRefreshing()) {
            listener.setIsRefreshing(true);
        }
    }

    @Override
    public DynamicWhereFilter copy() {
        if (setTable == null) {
            return new DynamicWhereFilter(setKeyTypes, setKernel, inclusion, sourceToSetColumnNamePairs);
        }
        return new DynamicWhereFilter(setTable, inclusion, sourceToSetColumnNamePairs);
    }

    @Override
    public boolean satisfied(final long step) {
        final boolean indexSatisfied = sourceDataIndex == null || sourceDataIndex.table().satisfied(step);
        return indexSatisfied && (setUpdateListener == null || setUpdateListener.satisfied(step));
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("DynamicWhereFilter(")
                .append(MatchPair.MATCH_PAIR_ARRAY_FORMATTER, sourceToSetColumnNamePairs)
                .append(')');
    }
}
