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

    private boolean liveValuesArrayValid;
    private boolean kernelValid;
    private Object[] liveValuesArray;
    private SetInclusionKernel setInclusionKernel;

    /**
     * The optimal data index for this filter.
     */
    @Nullable
    private DataIndex sourceDataIndex;

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
        kernelValid = liveValuesArrayValid = false;
        setInclusionKernel = null;
    }

    private void addKey(Object key) {
        final boolean added = liveValues.add(key);
        if (!added) {
            throw new RuntimeException("Inconsistent state, key already in set:" + key);
        }
        kernelValid = liveValuesArrayValid = false;
        setInclusionKernel = null;
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
        try (final SafeCloseable ignored = sourceTable.isRefreshing() ? LivenessScopeStack.open() : null) {
            sourceDataIndex = optimalIndex(matchPairs, sourceTable);
            if (sourceDataIndex != null && sourceDataIndex.isRefreshing()) {
                manage(sourceDataIndex);
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
    private static DataIndex optimalIndex(final MatchPair[] filterPairs, final Table inputTable) {
        final String[] keyColumnNames = MatchPair.getLeftColumns(filterPairs);

        final DataIndex fullIndex = DataIndexer.getDataIndex(inputTable, keyColumnNames);
        if (fullIndex != null) {
            return fullIndex;
        } else {
            return DataIndexer.getOptimalPartialIndex(inputTable, keyColumnNames);
        }
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
            if (!liveValuesArrayValid) {
                liveValuesArray = liveValues.toArray();
                liveValuesArrayValid = true;
            }
            // Our keys are reinterpreted, so we need to reinterpret the column source for correct matching.
            final ColumnSource<?> source =
                    ReinterpretUtils.maybeConvertToPrimitive(table.getColumnSource(matchPairs[0].leftColumn()));
            return source.match(!inclusion, false, false, sourceDataIndex, selection, liveValuesArray);
        }

        final ColumnSource<?>[] keyColumns = Arrays.stream(matchPairs)
                .map(mp -> table.getColumnSource(mp.leftColumn())).toArray(ColumnSource[]::new);
        final ChunkSource<Values> keySource = DataIndexUtils.makeBoxedKeySource(keyColumns);

        if (sourceDataIndex != null) {
            // Does our index contain every key column?

            if (sourceDataIndex.keyColumnMap().keySet().containsAll(Arrays.asList(keyColumns))) {
                // Even if we have an index, we may be better off with a linear search.
                if (selection.size() > (sourceDataIndex.table().size() * 2L)) {
                    return filterFullIndex(selection, sourceDataIndex, keyColumns);
                } else {
                    return filterLinear(selection, keyColumns);
                }
            }

            // We have a partial index, should we use it?
            if (selection.size() > (sourceDataIndex.table().size() * 4L)) {
                return filterPartialIndex(selection, sourceDataIndex, keyColumns);
            }
        }
        return filterLinear(selection, keyColumns);
    }

    @NotNull
    private WritableRowSet filterFullIndex(
            @NotNull final RowSet selection,
            final DataIndex dataIndex,
            final ColumnSource<?>[] keyColumns) {
        // Use the index RowSetLookup to create a combined row set of matching rows.
        final RowSetBuilderRandom rowSetBuilder = RowSetFactory.builderRandom();
        final DataIndex.RowKeyLookup rowKeyLookup = dataIndex.rowKeyLookup(keyColumns);
        final ColumnSource<RowSet> rowSetColumn = dataIndex.rowSetColumn();

        liveValues.forEach(key -> {
            final long rowKey = rowKeyLookup.apply(key, false);
            final RowSet rowSet = rowSetColumn.get(rowKey);
            if (rowSet != null) {
                rowSetBuilder.addRowSet(rowSet);
            }
        });


        try (final RowSet matchingKeys = rowSetBuilder.build()) {
            return (inclusion ? matchingKeys.copy() : selection.minus(matchingKeys));
        }
    }

    @NotNull
    private WritableRowSet filterPartialIndex(
            @NotNull final RowSet selection,
            final DataIndex dataIndex,
            final ColumnSource<?>[] keyColumns) {

        List<ColumnSource<?>> indexedSourceList = new ArrayList<>();
        List<Integer> indexedSourceIndices = new ArrayList<>();

        final Set<ColumnSource<?>> indexSourceSet = dataIndex.keyColumnMap().keySet();
        for (int ii = 0; ii < keyColumns.length; ++ii) {
            final ColumnSource<?> source = keyColumns[ii];
            if (indexSourceSet.contains(source)) {
                indexedSourceList.add(source);
                indexedSourceIndices.add(ii);
            }
        }

        Assert.geqZero(indexedSourceList.size(), "indexedSourceList.size()");

        final List<RowSet> indexRowSets = new ArrayList<>(indexedSourceList.size());
        final DataIndex.RowKeyLookup rowKeyLookup = dataIndex.rowKeyLookup();
        final ColumnSource<RowSet> rowSetColumn = dataIndex.rowSetColumn();

        if (indexedSourceIndices.size() == 1) {
            // Only one indexed source, so we can use the RowSetLookup directly.
            final int keyIndex = indexedSourceIndices.get(0);
            liveValues.forEach(key -> {
                final Object[] keys = (Object[]) key;
                final long rowKey = rowKeyLookup.apply(keys[keyIndex], false);
                final RowSet rowSet = rowSetColumn.get(rowKey);
                if (rowSet != null) {
                    indexRowSets.add(rowSet);
                }
            });
        } else {
            final Object[] partialKey = new Object[indexedSourceList.size()];

            liveValues.forEach(key -> {
                final Object[] keys = (Object[]) key;

                // Build the partial lookup key for the supplied key.
                int pos = 0;
                for (int keyIndex : indexedSourceIndices) {
                    partialKey[pos++] = keys[keyIndex];
                }

                // Perform the lookup using the partial key.
                final long rowKey = rowKeyLookup.apply(partialKey, false);
                final RowSet rowSet = rowSetColumn.get(rowKey);
                if (rowSet != null) {
                    indexRowSets.add(rowSet);
                }
            });
        }

        // We have some non-indexed sources, so we need to filter them manually. Iterate through the indexed
        // row sets and build a new row set where all keys match.
        final ChunkSource<Values> indexKeySource =
                DataIndexUtils.makeBoxedKeySource(indexedSourceList.toArray(new ColumnSource[0]));

        final List<RowSetBuilderSequential> builders = new ArrayList<>();

        final int CHUNK_SIZE = 1 << 10; // 1024
        try (final ColumnSource.GetContext keyGetContext = indexKeySource.makeGetContext(CHUNK_SIZE)) {
            for (final RowSet resultRowSet : indexRowSets) {
                if (resultRowSet.isEmpty()) {
                    continue;
                }

                try (final RowSequence.Iterator rsIt = resultRowSet.getRowSequenceIterator()) {
                    final RowSequence rsChunk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                    final ObjectChunk<Object, ? extends Values> valueChunk =
                            indexKeySource.getChunk(keyGetContext, rsChunk).asObjectChunk();
                    LongChunk<OrderedRowKeys> keyChunk = rsChunk.asRowKeyChunk();

                    RowSetBuilderSequential builder = null;

                    final int chunkSize = rsChunk.intSize();
                    for (int ii = 0; ii < chunkSize; ++ii) {
                        final Object key = valueChunk.get(ii);
                        if (!liveValues.contains(key)) {
                            continue;
                        }
                        if (builder == null) {
                            builder = RowSetFactory.builderSequential();
                            builders.add(builder);
                        }
                        builder.appendKey(keyChunk.get(ii));
                    }
                }
            }
        }

        // Combine the final answers and return the result.
        final RowSetBuilderRandom resultBuilder = RowSetFactory.builderRandom();
        for (final RowSetBuilderSequential builder : builders) {
            try (final RowSet ignored = builder.build()) {
                resultBuilder.addRowSet(ignored);
            }
        }
        return resultBuilder.build();
    }

    private WritableRowSet filterLinear(final RowSet selection, final ColumnSource<?>[] keyColumns) {
        if (selection.isEmpty()) {
            return RowSetFactory.empty();
        }

        final ChunkSource<Values> keySource = DataIndexUtils.makeBoxedKeySource(keyColumns);

        if (!kernelValid) {
            if (!liveValuesArrayValid) {
                liveValuesArray = liveValues.toArray();
                liveValuesArrayValid = true;
            }
            setInclusionKernel =
                    SetInclusionKernel.makeKernel(keySource.getChunkType(), List.of(liveValuesArray), inclusion);
            kernelValid = true;
        }

        final RowSetBuilderSequential indexBuilder = RowSetFactory.builderSequential();

        final int maxChunkSize = getChunkSize(selection);
        // @formatter:off
        try (final ChunkSource.GetContext keyGetContext = keySource.makeGetContext(maxChunkSize);
             final RowSequence.Iterator selectionIterator = selection.getRowSequenceIterator();
             final WritableLongChunk<OrderedRowKeys> selectionRowKeyChunk =
                     WritableLongChunk.makeWritableChunk(maxChunkSize);
             final WritableBooleanChunk<Values> matches = WritableBooleanChunk.makeWritableChunk(maxChunkSize)) {
            // @formatter:on

            while (selectionIterator.hasMore()) {
                final RowSequence selectionChunk = selectionIterator.getNextRowSequenceWithLength(maxChunkSize);

                final Chunk<Values> keyChunk = Chunk.downcast(keySource.getChunk(keyGetContext, selectionChunk));
                final int thisChunkSize = keyChunk.size();
                setInclusionKernel.matchValues(keyChunk, matches);

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
