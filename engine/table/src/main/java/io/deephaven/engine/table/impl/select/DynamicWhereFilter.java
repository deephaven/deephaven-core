/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.select.setinclusion.SetInclusionKernel;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * A where filter that extracts a set of inclusion or exclusion keys from a set table.
 *
 * Each time the set table ticks, the entire where filter is recalculated.
 */
public class DynamicWhereFilter extends WhereFilterLivenessArtifactImpl implements NotificationQueue.Dependency {
    private static final int CHUNK_SIZE = 1 << 16;

    private final boolean setRefreshing;
    private final MatchPair[] matchPairs;
    private final TupleSource<?> setTupleSource;
    private final boolean inclusion;

    private final HashSet<Object> liveValues = new HashSet<>();
    private boolean liveValuesArrayValid = false;
    private boolean kernelValid = false;
    private Object[] liveValuesArray = null;
    private SetInclusionKernel setInclusionKernel = null;

    // this reference must be maintained for reachability
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final QueryTable setTable;
    @SuppressWarnings("FieldCanBeLocal")
    // this reference must be maintained for reachability
    private final InstrumentedTableUpdateListener setUpdateListener;

    private RecomputeListener listener;
    private QueryTable resultTable;

    public DynamicWhereFilter(final QueryTable setTable, final boolean inclusion, final MatchPair... setColumnsNames) {
        setRefreshing = setTable.isRefreshing();
        if (setRefreshing) {
            updateGraph.checkInitiateSerialTableOperation();
        }

        this.matchPairs = setColumnsNames;
        this.inclusion = inclusion;

        final ColumnSource<?>[] setColumns = Arrays.stream(matchPairs)
                .map(mp -> setTable.getColumnSource(mp.rightColumn())).toArray(ColumnSource[]::new);

        if (setRefreshing) {
            this.setTable = setTable;
            setTupleSource = TupleSourceFactory.makeTupleSource(setColumns);
            setTable.getRowSet().forAllRowKeys((final long v) -> addKey(makeKey(v)));

            final String[] columnNames = Arrays.stream(matchPairs).map(MatchPair::rightColumn).toArray(String[]::new);
            final ModifiedColumnSet modTokenSet = setTable.newModifiedColumnSet(columnNames);
            setUpdateListener = new InstrumentedTableUpdateListenerAdapter(
                    "DynamicWhereFilter(" + Arrays.toString(setColumnsNames) + ")", setTable, false) {

                @Override
                public void onUpdate(final TableUpdate upstream) {
                    if (upstream.added().isEmpty() && upstream.removed().isEmpty()
                            && !upstream.modifiedColumnSet().containsAny(modTokenSet)) {
                        return;
                    }

                    final MutableBoolean trueModification = new MutableBoolean(false);

                    upstream.added().forAllRowKeys((final long v) -> addKey(makeKey(v)));
                    upstream.removed().forAllRowKeys((final long v) -> removeKey(makePrevKey(v)));

                    upstream.forAllModified((preIndex, postIndex) -> {
                        final Object oldKey = makePrevKey(preIndex);
                        final Object newKey = makeKey(postIndex);
                        if (!Objects.equals(oldKey, newKey)) {
                            trueModification.setTrue();
                            removeKey(oldKey);
                            addKey(newKey);
                        }
                    });

                    // Pretend every row of the original table was modified, this is essential so that the where clause
                    // can be re-evaluated based on the updated live set.
                    if (listener != null) {
                        if (upstream.added().isNonempty() || trueModification.booleanValue()) {
                            if (inclusion) {
                                listener.requestRecomputeUnmatched();
                            } else {
                                listener.requestRecomputeMatched();
                            }
                        }
                        if (upstream.removed().isNonempty() || trueModification.booleanValue()) {
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
            setTupleSource = null;
            final TupleSource<?> temporaryTupleSource = TupleSourceFactory.makeTupleSource(setColumns);
            setTable.getRowSet().forAllRowKeys((final long v) -> addKeyUnchecked(makeKey(temporaryTupleSource, v)));
            kernelValid = liveValuesArrayValid = false;
            setInclusionKernel = null;
            setUpdateListener = null;
        }
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    private Object makeKey(long index) {
        return makeKey(setTupleSource, index);
    }

    private static Object makeKey(TupleSource<?> tupleSource, long index) {
        return tupleSource.createTuple(index);
    }

    private Object makePrevKey(long index) {
        return setTupleSource.createPreviousTuple(index);
    }

    private void removeKey(Object key) {
        final boolean removed = liveValues.remove(key);
        if (!removed) {
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

        final ColumnSource<?>[] keyColumns = Arrays.stream(matchPairs)
                .map(mp -> table.getColumnSource(mp.leftColumn())).toArray(ColumnSource[]::new);
        final TupleSource<?> tupleSource = TupleSourceFactory.makeTupleSource(keyColumns);
        final TrackingRowSet trackingSelection = selection.isTracking() ? selection.trackingCast() : null;

        if (matchPairs.length == 1) {
            // this is just a single column filter so it will actually be exactly right
            if (!liveValuesArrayValid) {
                liveValuesArray = liveValues.toArray(CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                liveValuesArrayValid = true;
            }
            return table.getColumnSource(matchPairs[0].leftColumn()).match(!inclusion, false, false, selection,
                    liveValuesArray);
        }

        // pick something sensible
        if (trackingSelection != null) {
            final DataIndexer dataIndexer = DataIndexer.of(trackingSelection);

            // Do we have an index exactly matching the key columns?
            if (dataIndexer.hasDataIndex(keyColumns)) {
                final DataIndex dataIndex = dataIndexer.getDataIndex(keyColumns);
                final Table indexTable = dataIndex.table();

                if (selection.size() > (indexTable.size() * 2L)) {
                    return filterFullIndex(selection, dataIndex);
                } else {
                    return filterLinear(selection, keyColumns, tupleSource);
                }
            }

            // Do we have any indexes that partially match the key columns?
            final ColumnSource<?>[] indexedSources = Arrays.stream(keyColumns)
                    .filter(dataIndexer::hasDataIndex).toArray(ColumnSource[]::new);
            final ColumnSource<?>[] notIndexedSources = Arrays.stream(keyColumns)
                    .filter(col -> !dataIndexer.hasDataIndex(col)).toArray(ColumnSource[]::new);

            final OptionalInt minCount = Arrays.stream(indexedSources)
                    .mapToInt(x -> dataIndexer.getDataIndex(x).table().intSize()).min();

            if (minCount.isPresent() && (minCount.getAsInt() * 4L) < selection.size()) {
                return filterPartialIndexes(trackingSelection, dataIndexer, tupleSource);
            }
        }
        return filterLinear(selection, keyColumns, tupleSource);
    }

    @NotNull
    private WritableRowSet filterFullIndex(@NotNull final RowSet selection, final DataIndex dataIndex) {
        // Use the RowSetLookup to create a combined row set of matching rows.
        final RowSetBuilderRandom rowSetBuilder = RowSetFactory.builderRandom();
        final DataIndex.RowSetLookup rowSetLookup = dataIndex.rowSetLookup();
        liveValues.forEach(key -> {
            final RowSet rowSet = rowSetLookup.apply(key);
            if (rowSet != null) {
                rowSetBuilder.addRowSet(rowSet);
            }
        });

        try (final RowSet matchingKeys = rowSetBuilder.build()) {
            return (inclusion ? matchingKeys.copy() : selection.minus(matchingKeys));
        }
    }

    @NotNull
    private WritableRowSet filterPartialIndexes(
            @NotNull final RowSet selection,
            final DataIndexer dataIndexer,
            final TupleSource<?> tupleSource) {

        List<ColumnSource<?>> sourceList = tupleSource.getColumnSources();

        List<ColumnSource<?>> indexedSourceList = new ArrayList<>();
        List<ColumnSource<?>> notIndexSourceList = new ArrayList<>();
        List<Integer> indexedSourceIndices = new ArrayList<>();
        List<Integer> notIndexedSourceIndices = new ArrayList<>();

        for (int ii = 0; ii < sourceList.size(); ++ii) {
            final ColumnSource<?> source = sourceList.get(ii);
            if (dataIndexer.hasDataIndex(source)) {
                indexedSourceList.add(source);
                indexedSourceIndices.add(ii);
            } else {
                notIndexSourceList.add(source);
                notIndexedSourceIndices.add(ii);
            }
        }

        Assert.geqZero(indexedSourceList.size(), "indexedSourceList.size()");

        final ColumnSource<?>[] indexedSources = indexedSourceList.toArray(new ColumnSource<?>[0]);
        final TupleSource indexedTupleSource = TupleSourceFactory.makeTupleSource(indexedSources);

        // Get the data indexes for each of the indexed sources.
        final DataIndex.RowSetLookup[] indexLookupArr = Arrays.stream(indexedSources)
                .map(source -> dataIndexer.getDataIndex(source).rowSetLookup()).toArray(DataIndex.RowSetLookup[]::new);

        final Map<Object, RowSet> indexKeyRowSetMap = new LinkedHashMap<>();

        if (indexedSourceIndices.size() == 1) {
            // Only one indexed source, so we can use the RowSetLookup directly and return the row set.
            liveValues.forEach(key -> {
                final RowSet rowSet = indexLookupArr[0].apply(key);
                if (rowSet != null) {
                    // Make a copy of the row set.
                    indexKeyRowSetMap.put(key, rowSet.copy());
                }
            });
        } else {
            // Intersect the retrieved row sets to get the final row set for this key.
            liveValues.forEach(key -> {
                RowSet result = null;
                for (int ii = 0; ii < indexedSourceIndices.size(); ++ii) {
                    final int tupleIndex = indexedSourceIndices.get(ii);
                    // noinspection unchecked
                    final Object singleKey = indexedTupleSource.exportElementReinterpreted(key, tupleIndex);
                    final RowSet rowSet = indexLookupArr[ii].apply(singleKey);
                    if (rowSet != null) {
                        result = result == null ? rowSet.copy() : result.intersect(rowSet);
                    }
                }
                if (result != null) {
                    indexKeyRowSetMap.put(key, result);
                }
            });
        }

        if (notIndexSourceList.size() == 0) {
            // Combine the indexed answers and return the result.
            final RowSetBuilderRandom resultBuilder = RowSetFactory.builderRandom();
            for (final RowSet rowSet : indexKeyRowSetMap.values()) {
                try (final SafeCloseable ignored = rowSet) {
                    resultBuilder.addRowSet(rowSet);
                }
            }
            return resultBuilder.build();
        } else {
            // We have some non-indexed sources, so we need to filter them manually. Iterate through the indexed
            // row sets and build a new row set where all keys match.
            final Map<Object, RowSetBuilderSequential> keyRowSetBuilder = new LinkedHashMap<>();

            for (final Map.Entry<Object, RowSet> entry : indexKeyRowSetMap.entrySet()) {
                try (final RowSet resultRowSet = entry.getValue()) {
                    if (resultRowSet.isEmpty()) {
                        continue;
                    }

                    // Iterate through the index-restricted row set for matches.
                    for (final RowSet.Iterator iterator = resultRowSet.iterator(); iterator.hasNext();) {
                        final long rowKey = iterator.nextLong();
                        final Object key = tupleSource.createTuple(rowKey);

                        if (!liveValues.contains(key)) {
                            continue;
                        }

                        final RowSetBuilderSequential rowSetForKey =
                                keyRowSetBuilder.computeIfAbsent(key, k -> RowSetFactory.builderSequential());
                        rowSetForKey.appendKey(rowKey);
                    }
                }
            }

            // Combine the final answers and return the result.
            final RowSetBuilderRandom resultBuilder = RowSetFactory.builderRandom();
            for (final RowSetBuilderSequential builder : keyRowSetBuilder.values()) {
                try (final RowSet ignored = builder.build()) {
                    resultBuilder.addRowSet(ignored);
                }
            }
            return resultBuilder.build();
        }
    }

    private WritableRowSet filterLinear(RowSet selection, ColumnSource<?>[] keyColumns, TupleSource<?> tupleSource) {
        if (keyColumns.length == 1) {
            return filterLinearOne(selection, keyColumns[0]);
        } else {
            return filterLinearTuple(selection, tupleSource);
        }
    }

    private WritableRowSet filterLinearOne(RowSet selection, ColumnSource<?> keyColumn) {
        if (selection.isEmpty()) {
            return RowSetFactory.empty();
        }

        if (!kernelValid) {
            setInclusionKernel = SetInclusionKernel.makeKernel(keyColumn.getChunkType(), liveValues, inclusion);
            kernelValid = true;
        }

        final RowSetBuilderSequential indexBuilder = RowSetFactory.builderSequential();

        try (final ColumnSource.GetContext getContext = keyColumn.makeGetContext(CHUNK_SIZE);
                final RowSequence.Iterator rsIt = selection.getRowSequenceIterator()) {
            final WritableLongChunk<OrderedRowKeys> keyIndices = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            final WritableBooleanChunk<Values> matches = WritableBooleanChunk.makeWritableChunk(CHUNK_SIZE);

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);

                final Chunk<Values> chunk = Chunk.downcast(keyColumn.getChunk(getContext, chunkOk));
                setInclusionKernel.matchValues(chunk, matches);

                keyIndices.setSize(chunk.size());
                chunkOk.fillRowKeyChunk(keyIndices);

                for (int ii = 0; ii < chunk.size(); ++ii) {
                    if (matches.get(ii)) {
                        indexBuilder.appendKey(keyIndices.get(ii));
                    }
                }
            }
        }


        return indexBuilder.build();
    }

    private WritableRowSet filterLinearTuple(RowSet selection, TupleSource<?> tupleSource) {
        final RowSetBuilderSequential indexBuilder = RowSetFactory.builderSequential();

        for (final RowSet.Iterator it = selection.iterator(); it.hasNext();) {
            final long row = it.nextLong();
            final Object tuple = tupleSource.createTuple(row);
            if (liveValues.contains(tuple) == inclusion) {
                indexBuilder.appendKey(row);
            }
        }

        return indexBuilder.build();
    }

    @Override
    public boolean isSimpleFilter() {
        /* This doesn't execute any user code, so it should be safe to execute it against untrusted data. */
        return true;
    }

    @Override
    public boolean isRefreshing() {
        return setRefreshing;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {
        this.listener = listener;
        this.resultTable = listener.getTable();
        if (DynamicNode.isDynamicAndIsRefreshing(setTable)) {
            listener.setIsRefreshing(true);
        }
    }

    @Override
    public DynamicWhereFilter copy() {
        return new DynamicWhereFilter(setTable, inclusion, matchPairs);
    }

    @Override
    public boolean satisfied(final long step) {
        return setUpdateListener == null || setUpdateListener.satisfied(step);
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("DynamicWhereFilter(").append(MatchPair.MATCH_PAIR_ARRAY_FORMATTER, matchPairs)
                .append(")");
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }
}
