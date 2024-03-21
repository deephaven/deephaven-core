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
import io.deephaven.engine.table.impl.dataindex.DataIndexUtils;
import io.deephaven.engine.table.impl.dataindex.DataIndexKeySet;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A where filter that extracts a set of inclusion or exclusion keys from a set table.
 * <p>
 * Each time the set table ticks, the entire where filter is recalculated.
 */
public class DynamicWhereFilter extends WhereFilterLivenessArtifactImpl implements NotificationQueue.Dependency {

    private static final int CHUNK_SIZE = 1 << 16;

    private final MatchPair[] matchPairs;
    private final boolean inclusion;

    private final DataIndexKeySet liveValues;

    private final QueryTable setTable;
    private final ChunkSource.WithPrev<Values> setKeySource;
    @SuppressWarnings("FieldCanBeLocal")
    @ReferentialIntegrity
    private final InstrumentedTableUpdateListener setUpdateListener;

    private Object[] liveValuesArray;

    private ColumnSource<?>[] sourceKeyColumns;
    /**
     * The optimal data index for this filter.
     */
    @Nullable
    private DataIndex sourceDataIndex;
    private int[] indexToSetKeyOffsets;

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

        liveValues = DataIndexUtils.makeKeySet(setColumnsNames.length);

        final ColumnSource<?>[] setColumns = Arrays.stream(matchPairs)
                .map(mp -> setTable.getColumnSource(mp.rightColumn())).toArray(ColumnSource[]::new);

        if (setTable.isRefreshing()) {
            setKeySource = DataIndexUtils.makeBoxedKeySource(setColumns);
            if (setTable.getRowSet().isNonempty()) {
                try (final CloseableIterator<?> initialKeysIterator = ChunkedColumnIterator.make(
                        setKeySource, setTable.getRowSet(), getChunkSize(setTable.getRowSet()))) {
                    initialKeysIterator.forEachRemaining(this::addKey);
                }
            }

            final String[] setColumnNames =
                    Arrays.stream(matchPairs).map(MatchPair::rightColumn).toArray(String[]::new);
            final ModifiedColumnSet setColumnsMCS = setTable.newModifiedColumnSet(setColumnNames);
            this.setTable = setTable;
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
            this.setTable = null;
            setKeySource = null;
            setUpdateListener = null;
            if (setTable.getRowSet().isNonempty()) {
                final ChunkSource.WithPrev<Values> tmpKeySource = DataIndexUtils.makeBoxedKeySource(setColumns);
                try (final CloseableIterator<?> initialKeysIterator = ChunkedColumnIterator.make(
                        tmpKeySource, setTable.getRowSet(), getChunkSize(setTable.getRowSet()))) {
                    initialKeysIterator.forEachRemaining(this::addKeyUnchecked);
                }
            }
        }
    }

    /**
     * "Copy constructor" for DynamicWhereFilter's with static set tables.
     */
    private DynamicWhereFilter(
            @NotNull final DataIndexKeySet liveValues,
            final boolean inclusion,
            final MatchPair... setColumnsNames) {
        this.liveValues = liveValues;
        this.matchPairs = setColumnsNames;
        this.inclusion = inclusion;
        setTable = null;
        setKeySource = null;
        setUpdateListener = null;
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    private void removeKey(Object key) {
        final boolean removed = liveValues.remove(key);
        if (!removed && key != null) {
            throw new RuntimeException("Inconsistent state, key not found in set: " + key);
        }
        liveValuesArray = null;
    }

    private void addKey(Object key) {
        final boolean added = liveValues.add(key);
        if (!added) {
            throw new RuntimeException("Inconsistent state, key already in set:" + key);
        }
        liveValuesArray = null;
    }

    private void addKeyUnchecked(Object key) {
        liveValues.add(key);
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
                indexToSetKeyOffsets = computeIndexToSetKeyOffsets(sourceDataIndex, sourceKeyColumns);
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
     * Calculates a mapping from the index of a {@link ColumnSource} in the data index to the index of the corresponding
     * {@link ColumnSource} in the key sources from the source table for a DynamicWhereFilter. This allows for mapping
     * keys from the {@link #liveValues} to keys in the {@link #sourceDataIndex}.
     *
     * @param dataIndex The {@link DataIndex} to use for the mapping
     * @param keySources The key {@link ColumnSource ColumnSources} from the source table
     * @return A mapping from the data index source offset to the key source offset for the same {@link ColumnSource},
     *         or {@code null} if there is no mapping needed (because {@code dataIndex} and {@code keySources} have the
     *         same columns in the same order
     */
    private static int[] computeIndexToSetKeyOffsets(
            @NotNull final DataIndex dataIndex,
            @NotNull final ColumnSource<?>[] keySources) {

        final class SourceOffsetPair {

            private final ColumnSource<?> source;
            private final int offset;

            private SourceOffsetPair(@NotNull final ColumnSource<?> source, final int offset) {
                this.source = source;
                this.offset = offset;
            }
        }

        final MutableInt dataIndexSourceOffset = new MutableInt(0);
        final KeyedObjectHashMap<ColumnSource<?>, SourceOffsetPair> dataIndexSources =
                dataIndex.keyColumnNamesByIndexedColumn().keySet()
                        .stream()
                        .collect(Collectors.toMap(
                                cs -> cs,
                                cs -> new SourceOffsetPair(cs, dataIndexSourceOffset.getAndIncrement()),
                                Assert::neverInvoked,
                                () -> new KeyedObjectHashMap<>(new KeyedObjectKey.ExactAdapter<>(sop -> sop.source))));

        final int[] indexToKeySourceOffsets = new int[dataIndexSources.size()];
        boolean isAscending = true;
        for (int kci = 0; kci < keySources.length; ++kci) {
            final SourceOffsetPair dataIndexSource = dataIndexSources.get(keySources[kci]);
            if (dataIndexSource != null) {
                indexToKeySourceOffsets[dataIndexSource.offset] = kci;
                isAscending &= dataIndexSource.offset == kci;
                if (dataIndexSourceOffset.decrementAndGet() == 0) {
                    return isAscending && keySources.length == indexToKeySourceOffsets.length
                            ? null
                            : indexToKeySourceOffsets;
                }
            }
        }

        throw new IllegalArgumentException(String.format(
                "The provided key sources %s don't match the data index key sources %s",
                Arrays.toString(keySources), dataIndex.keyColumnNamesByIndexedColumn().keySet()));
    }

    @NotNull
    private Function<Object, Object> compoundKeyMappingFunction() {
        if (indexToSetKeyOffsets == null) {
            return Function.identity();
        }
        final Object[] keysInDataIndexOrder = new Object[indexToSetKeyOffsets.length];
        return (final Object key) -> {
            final Object[] keysInSetOrder = (Object[]) key;
            for (int ki = 0; ki < keysInDataIndexOrder.length; ++ki) {
                keysInDataIndexOrder[ki] = keysInSetOrder[indexToSetKeyOffsets[ki]];
            }
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
            // Single column filter, delegate to the column source.
            if (liveValuesArray == null) {
                liveValuesArray = liveValues.toArray();
            }
            // Our keys are reinterpreted, so we need to reinterpret the column source for correct matching.
            final ColumnSource<?> source = ReinterpretUtils.maybeConvertToPrimitive(sourceKeyColumns[0]);
            return source.match(!inclusion, false, false, sourceDataIndex, selection, liveValuesArray);
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

            if (indexToSetKeyOffsets.length == 1) {
                // We could delegate to the column source, but we'd have to create a single-column view of liveValues.

                // Only one indexed source, so we can use the RowSetLookup directly on the right sub-key.
                final int keyOffset = indexToSetKeyOffsets[0];
                liveValues.forEach(key -> {
                    final Object[] keysInSetOrder = (Object[]) key;
                    final long rowKey = rowKeyLookup.apply(keysInSetOrder[keyOffset], false);
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
        // Any single column filter is pushed through AbstractColumnSource.match()
        Assert.gt(sourceKeyColumns.length, "sourceKeyColumns.length", 1);

        final RowSetBuilderSequential indexBuilder = RowSetFactory.builderSequential();

        final ChunkSource<Values> keySource = DataIndexUtils.makeBoxedKeySource(sourceKeyColumns);

        final int maxChunkSize = getChunkSize(selection);
        // @formatter:off
        try (final ChunkSource.GetContext keyGetContext = keySource.makeGetContext(maxChunkSize);
             final RowSequence.Iterator selectionIterator = selection.getRowSequenceIterator()) {
            // @formatter:on

            while (selectionIterator.hasMore()) {
                final RowSequence selectionChunk = selectionIterator.getNextRowSequenceWithLength(maxChunkSize);
                final LongChunk<OrderedRowKeys> selectionRowKeyChunk = selectionChunk.asRowKeyChunk();
                final ObjectChunk<Object, ? extends Values> keyChunk =
                        keySource.getChunk(keyGetContext, selectionChunk).asObjectChunk();
                final int thisChunkSize = keyChunk.size();

                for (int ii = 0; ii < thisChunkSize; ++ii) {
                    if (liveValues.contains(keyChunk.get(ii)) == filterInclusion)
                        indexBuilder.appendKey(selectionRowKeyChunk.get(ii));
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
