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
import java.util.stream.IntStream;

/**
 * A where filter that extracts a set of inclusion or exclusion keys from a set table.
 * <p>
 * Each time the set table ticks, the entire where filter is recalculated.
 */
public class DynamicWhereFilter extends WhereFilterLivenessArtifactImpl implements NotificationQueue.Dependency {

    private static final int CHUNK_SIZE = 1 << 16;

    private final MatchPair[] matchPairs;
    private final boolean inclusion;

    private final Set<Object> liveValues;
    private List<Object> staticLookupKeys;

    @SuppressWarnings("FieldCanBeLocal")
    @ReferentialIntegrity
    private final InstrumentedTableUpdateListener setUpdateListener;

    private final SetInclusionKernel setKernel;

    private QueryTable setTable;
    private TupleSource<Object> setKeySource;

    private ColumnSource<?>[] sourceKeyColumns;
    /**
     * The optimal data index for this filter.
     */
    @Nullable
    private DataIndex sourceDataIndex;
    private int[] tupleToIndexMap;
    private int[] indexToTupleMap;

    private RecomputeListener listener;
    private QueryTable resultTable;

    public DynamicWhereFilter(
            @NotNull final QueryTable setTable,
            final boolean inclusion,
            final MatchPair... setColumnsNames) {
        if (setTable.isRefreshing()) {
            updateGraph.checkInitiateSerialTableOperation();
        }
        this.matchPairs = setColumnsNames;
        this.inclusion = inclusion;

        // We will use tuples even when there are multiple key columns.
        liveValues = new HashSet<>();

        final ColumnSource<?>[] setColumns = Arrays.stream(matchPairs)
                .map(mp -> setTable.getColumnSource(mp.rightColumn())).toArray(ColumnSource[]::new);

        this.setTable = setTable;
        setKeySource = TupleSourceFactory.makeTupleSource(setColumns);

        if (setTable.isRefreshing()) {
            setKeySource = TupleSourceFactory.makeTupleSource(setColumns);
            setKernel = SetInclusionKernel.makeKernel(setKeySource.getChunkType(), inclusion);
            if (setTable.getRowSet().isNonempty()) {
                try (final CloseableIterator<?> initialKeysIterator = ChunkedColumnIterator.make(
                        setKeySource, setTable.getRowSet(), getChunkSize(setTable.getRowSet()))) {
                    initialKeysIterator.forEachRemaining(this::addKey);
                }
            }

            final String[] setColumnNames =
                    Arrays.stream(matchPairs).map(MatchPair::rightColumn).toArray(String[]::new);
            final ModifiedColumnSet setColumnsMCS = setTable.newModifiedColumnSet(setColumnNames);
            setUpdateListener = new InstrumentedTableUpdateListenerAdapter(
                    "DynamicWhereFilter(" + Arrays.toString(setColumnsNames) + ")", setTable, false) {

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
            setUpdateListener = null;
            setKernel = SetInclusionKernel.makeKernel(setKeySource.getChunkType(), inclusion);
        }
    }

    /**
     * "Copy constructor" for DynamicWhereFilter's with static set tables.
     */
    private DynamicWhereFilter(
            @NotNull final Set<Object> liveValues,
            final boolean inclusion,
            final MatchPair... setColumnsNames) {
        this.liveValues = liveValues;
        this.matchPairs = setColumnsNames;
        this.inclusion = inclusion;
        setTable = null;
        setKeySource = null;
        setUpdateListener = null;
        setKernel = SetInclusionKernel.makeKernel(ChunkType.Object, liveValues, inclusion);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    private void removeKey(Object key) {
        final boolean removed = liveValues.remove(key);
        if (!removed) {
            throw new RuntimeException("Inconsistent state, key not found in set: " + key);
        }
        setKernel.removeItem(key);
    }

    private void addKey(Object key) {
        final boolean added = liveValues.add(key);
        if (!added) {
            throw new RuntimeException("Inconsistent state, key already in set:" + key);
        }
        setKernel.addItem(key);
    }

    private void addKeyUnchecked(Object key) {
        liveValues.add(key);
        setKernel.addItem(key);
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
        final String[] keyColumnNames = MatchPair.getLeftColumns(matchPairs);
        sourceKeyColumns = Arrays.stream(matchPairs)
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

        // Under certain conditions, we will pre-compute all the lookup keys from the set table.
        if (keyColumnNames.length > 1
                && sourceDataIndex != null
                && !setTable.isRefreshing()
                && setTable.getRowSet().isNonempty()) {
            staticLookupKeys = new ArrayList<>(liveValues);
            final int subKeySize = sourceDataIndex.keyColumnNames().size();

            try (final CloseableIterator<?> initialKeysIterator = ChunkedColumnIterator.make(
                    setKeySource, setTable.getRowSet(), getChunkSize(setTable.getRowSet()))) {
                if (subKeySize == 1) {
                    final int offset = indexToTupleMap == null ? 0 : indexToTupleMap[0];
                    initialKeysIterator.forEachRemaining(key -> {
                        addKeyUnchecked(key);
                        // TODO: if we are using a partial index, we are potentially adding duplicate sub-keys. Should
                        // we
                        // track the sub-keys in a hash set to avoid this?
                        staticLookupKeys.add(setKeySource.exportElementReinterpreted(key, offset));
                    });
                } else if (subKeySize < keyColumnNames.length) {
                    // We are using a partial index so our lookup keys are smaller than the setTable tuple
                    final Object[] dest = new Object[keyColumnNames.length];
                    initialKeysIterator.forEachRemaining(key -> {
                        addKeyUnchecked(key);

                        if (tupleToIndexMap == null) {
                            setKeySource.exportAllReinterpretedTo(dest, key);
                        } else {
                            setKeySource.exportAllReinterpretedTo(dest, key, tupleToIndexMap);
                        }
                        final Object[] lookupKey = Arrays.copyOf(dest, subKeySize);
                        // TODO: if we are using a partial index, we are potentially adding duplicate sub-keys. Should
                        // we
                        // track the sub-keys in a hash set to avoid this?
                        staticLookupKeys.add(lookupKey);
                    });
                } else {
                    initialKeysIterator.forEachRemaining(key -> {
                        addKeyUnchecked(key);

                        final Object[] dest = new Object[keyColumnNames.length];
                        if (tupleToIndexMap == null) {
                            setKeySource.exportAllReinterpretedTo(dest, key);
                        } else {
                            setKeySource.exportAllReinterpretedTo(dest, key, tupleToIndexMap);
                        }
                        staticLookupKeys.add(dest);
                    });
                }
            }



            // We have computed the static lookup keys, we can release the set table and column source
            setTable = null;
            setKeySource = null;
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
     * Calculates a mapping from the index of a {@link ColumnSource} in the data index to the index of the corresponding
     * {@link ColumnSource} in the key sources from the source table for a DynamicWhereFilter. This allows for mapping
     * keys from the {@link #liveValues} to keys in the {@link #sourceDataIndex}.
     */
    private void computeTupleIndexMaps() {

        final ColumnSource<?>[] dataIndexSources = sourceDataIndex.keyColumnNamesByIndexedColumn().keySet()
                .toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);

        // Pre-fill with the identity map
        final int[] tupleToIndexMap = IntStream.range(0, sourceKeyColumns.length).toArray();
        final int[] indexToTupleMap = Arrays.copyOf(tupleToIndexMap, tupleToIndexMap.length);

        boolean sameOrder = true;

        // The tuples will be in keySources order, we need to find the dataIndex offset for each keySource.
        // This is an N^2 loop but N is expected to be very small and this is called only at creation.
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
    private Function<Object, Object> compoundKeyMappingFunction() {
        final Object[] keysInDataIndexOrder = new Object[sourceKeyColumns.length];
        if (tupleToIndexMap == null) {
            return (final Object key) -> {
                setKeySource.exportAllReinterpretedTo(keysInDataIndexOrder, key);
                return keysInDataIndexOrder;
            };
        }
        return (final Object key) -> {
            setKeySource.exportAllReinterpretedTo(keysInDataIndexOrder, key, tupleToIndexMap);
            return keysInDataIndexOrder;
        };
    }

    @Override
    public List<String> getColumns() {
        return Arrays.asList(MatchPair.getLeftColumns(matchPairs));
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(TableDefinition tableDefinition) {}

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

        if (matchPairs.length == 1) {
            // If we have a dataIndex, we can delegate to the column source.
            if (sourceDataIndex != null) {
                final ColumnSource<?> source = ReinterpretUtils.maybeConvertToPrimitive(sourceKeyColumns[0]);
                return source.match(!inclusion, false, false, sourceDataIndex, selection, liveValues.toArray());
            }
            return filterLinear(selection, inclusion);
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
        Assert.gt(sourceKeyColumns.length, "sourceKeyColumns.length", 1);

        final WritableRowSet filtered = inclusion ? RowSetFactory.empty() : selection.copy();
        // noinspection DataFlowIssue
        final DataIndex.RowKeyLookup rowKeyLookup = sourceDataIndex.rowKeyLookup();
        final ColumnSource<RowSet> rowSetColumn = sourceDataIndex.rowSetColumn();

        if (staticLookupKeys != null) {
            staticLookupKeys.forEach(key -> {
                final long rowKey = rowKeyLookup.apply(key, false);
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

        final Function<Object, Object> keyMappingFunction = compoundKeyMappingFunction();

        liveValues.forEach(key -> {
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

            if (sourceDataIndex.keyColumnNames().size() == 1) {
                // Only one indexed source, so we can use the RowSetLookup directly on the right sub-key.
                if (staticLookupKeys != null) {
                    staticLookupKeys.forEach(key -> {
                        final long rowKey = rowKeyLookup.apply(key, false);
                        final RowSet rowSet = rowSetColumn.get(rowKey);
                        if (rowSet != null) {
                            try (final RowSet intersected = rowSet.intersect(selection)) {
                                possiblyMatching.insert(intersected);
                            }
                        }
                    });
                } else {
                    final int keyOffset = indexToTupleMap == null ? 0 : indexToTupleMap[0];
                    liveValues.forEach(key -> {
                        final Object lookupKey = setKeySource.exportElement(key, keyOffset);
                        final long rowKey = rowKeyLookup.apply(lookupKey, false);
                        final RowSet rowSet = rowSetColumn.get(rowKey);
                        if (rowSet != null) {
                            try (final RowSet intersected = rowSet.intersect(selection)) {
                                possiblyMatching.insert(intersected);
                            }
                        }
                    });
                }
            } else {
                if (staticLookupKeys != null) {
                    staticLookupKeys.forEach(key -> {
                        final long rowKey = rowKeyLookup.apply(key, false);
                        final RowSet rowSet = rowSetColumn.get(rowKey);
                        if (rowSet != null) {
                            try (final RowSet intersected = rowSet.intersect(selection)) {
                                possiblyMatching.insert(intersected);
                            }
                        }
                    });
                } else {
                    final Function<Object, Object> keyMappingFunction = compoundKeyMappingFunction();
                    liveValues.forEach(key -> {
                        final Object mappedKey = keyMappingFunction.apply(key);
                        final long rowKey = rowKeyLookup.apply(mappedKey, false);
                        final RowSet rowSet = rowSetColumn.get(rowKey);
                        if (rowSet != null) {
                            try (final RowSet intersected = rowSet.intersect(selection)) {
                                possiblyMatching.insert(intersected);
                            }
                        }
                    });
                }
            }

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

        final RowSetBuilderSequential indexBuilder = RowSetFactory.builderSequential();

        final TupleSource<Object> tmpKeySource = TupleSourceFactory.makeTupleSource(sourceKeyColumns);

        final int maxChunkSize = getChunkSize(selection);
        // @formatter:off
        try (final ColumnSource.GetContext keyGetContext = tmpKeySource.makeGetContext(maxChunkSize);
             final RowSequence.Iterator selectionIterator = selection.getRowSequenceIterator();
             final WritableLongChunk<OrderedRowKeys> selectionRowKeyChunk =
                     WritableLongChunk.makeWritableChunk(maxChunkSize);
             final WritableBooleanChunk<Values> matches = WritableBooleanChunk.makeWritableChunk(maxChunkSize)) {
            // @formatter:on

            while (selectionIterator.hasMore()) {
                final RowSequence selectionChunk = selectionIterator.getNextRowSequenceWithLength(maxChunkSize);

                final Chunk<Values> keyChunk = Chunk.downcast(tmpKeySource.getChunk(keyGetContext, selectionChunk));
                final int thisChunkSize = keyChunk.size();
                setKernel.matchValues(keyChunk, matches, filterInclusion);

                selectionRowKeyChunk.setSize(thisChunkSize);
                selectionChunk.fillRowKeyChunk(selectionRowKeyChunk);

                for (int ii = 0; ii < thisChunkSize; ++ii) {
                    if (matches.get(ii)) {
                        indexBuilder.appendKey(selectionRowKeyChunk.get(ii));
                    }
                }
            }
        }

        return indexBuilder.build();
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
            return new DynamicWhereFilter(liveValues, inclusion, matchPairs);
        }
        return new DynamicWhereFilter(setTable, inclusion, matchPairs);
    }

    @Override
    public boolean satisfied(final long step) {
        final boolean indexSatisfied = sourceDataIndex == null || sourceDataIndex.table().satisfied(step);
        return indexSatisfied && (setUpdateListener == null || setUpdateListener.satisfied(step));
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("DynamicWhereFilter(").append(MatchPair.MATCH_PAIR_ARRAY_FORMATTER, matchPairs)
                .append(")");
    }
}
