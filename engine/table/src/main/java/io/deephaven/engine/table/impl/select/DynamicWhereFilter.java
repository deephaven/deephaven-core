/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.RowSetIndexer;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.select.setinclusion.SetInclusionKernel;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * A where filter that extracts a set of inclusion or exclusion keys from a set table.
 * <p>
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
            try (final CloseableIterator<?> initialKeysIterator = ChunkedColumnIterator.make(
                    setTupleSource, setTable.getRowSet())) {
                initialKeysIterator.forEachRemaining(this::addKey);
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
                                setTupleSource.getPrevSource(), upstream.removed())) {
                            removedKeysIterator.forEachRemaining(DynamicWhereFilter.this::removeKey);
                        }
                    }

                    // Update modified keys
                    boolean trueModification = false;
                    if (hasModifies) {
                        // @formatter:off
                        try (final CloseableIterator<?> preModifiedKeysIterator = ChunkedColumnIterator.make(
                                     setTupleSource.getPrevSource(), upstream.getModifiedPreShift());
                             final CloseableIterator<?> postModifiedKeysIterator = ChunkedColumnIterator.make(
                                     setTupleSource, upstream.modified())) {
                            // @formatter:on
                            while (preModifiedKeysIterator.hasNext()) {
                                Assert.assertion(postModifiedKeysIterator.hasNext(),
                                        "Pre and post modified row sets must be the same size");
                                final Object oldKey = preModifiedKeysIterator.next();
                                final Object newKey = postModifiedKeysIterator.next();
                                if (!Objects.equals(oldKey, newKey)) {
                                    trueModification = true;
                                    removeKey(oldKey);
                                    addKey(newKey);
                                }
                            }
                        }
                    }

                    // Add added keys
                    if (hasAdds) {
                        try (final CloseableIterator<?> addedKeysIterator = ChunkedColumnIterator.make(
                                setTupleSource, upstream.added())) {
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
            final RowSetIndexer selectionIndexer = RowSetIndexer.of(trackingSelection);
            if (selectionIndexer.hasGrouping(keyColumns)) {
                if (selection.size() > (selectionIndexer.getGrouping(tupleSource).size() * 2L)) {
                    return filterGrouping(trackingSelection, selectionIndexer, tupleSource);
                } else {
                    return filterLinear(selection, keyColumns, tupleSource);
                }
            }
            final boolean allGrouping = Arrays.stream(keyColumns).allMatch(selectionIndexer::hasGrouping);
            if (allGrouping) {
                return filterGrouping(trackingSelection, selectionIndexer, tupleSource);
            }

            final ColumnSource<?>[] sourcesWithGroupings = Arrays.stream(keyColumns)
                    .filter(selectionIndexer::hasGrouping).toArray(ColumnSource[]::new);
            final OptionalInt minGroupCount = Arrays.stream(sourcesWithGroupings)
                    .mapToInt(x -> selectionIndexer.getGrouping(x).size()).min();
            if (minGroupCount.isPresent() && (minGroupCount.getAsInt() * 4L) < selection.size()) {
                return filterGrouping(trackingSelection, selectionIndexer, tupleSource);
            }
        }
        return filterLinear(selection, keyColumns, tupleSource);
    }

    private WritableRowSet filterGrouping(
            TrackingRowSet selection,
            RowSetIndexer selectionIndexer,
            TupleSource<?> tupleSource) {
        final RowSet matchingKeys = selectionIndexer.getSubSetForKeySet(liveValues, tupleSource);
        return (inclusion ? matchingKeys.copy() : selection.minus(matchingKeys));
    }

    private WritableRowSet filterGrouping(TrackingRowSet selection, RowSetIndexer selectionIndexer, Table table) {
        final ColumnSource<?>[] keyColumns = Arrays.stream(matchPairs)
                .map(mp -> table.getColumnSource(mp.leftColumn())).toArray(ColumnSource[]::new);
        final TupleSource<?> tupleSource = TupleSourceFactory.makeTupleSource(keyColumns);
        return filterGrouping(selection, selectionIndexer, tupleSource);
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

        final int maxChunkSize = (int) Math.min(CHUNK_SIZE, selection.size());
        // @formatter:off
        try (final ColumnSource.GetContext keyGetContext = keyColumn.makeGetContext(maxChunkSize);
             final RowSequence.Iterator selectionIterator = selection.getRowSequenceIterator();
             final WritableLongChunk<OrderedRowKeys> selectionRowKeyChunk =
                     WritableLongChunk.makeWritableChunk(maxChunkSize);
             final WritableBooleanChunk<Values> matches = WritableBooleanChunk.makeWritableChunk(maxChunkSize)) {
            // @formatter:on

            while (selectionIterator.hasMore()) {
                final RowSequence selectionChunk = selectionIterator.getNextRowSequenceWithLength(maxChunkSize);

                final Chunk<Values> keyChunk = Chunk.downcast(keyColumn.getChunk(keyGetContext, selectionChunk));
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

    private WritableRowSet filterLinearTuple(RowSet selection, TupleSource<?> tupleSource) {
        final RowSetBuilderSequential indexBuilder = RowSetFactory.builderSequential();

        final int maxChunkSize = (int) Math.min(CHUNK_SIZE, selection.size());
        // @formatter:off
        try (final ColumnSource.GetContext keyGetContext = tupleSource.makeGetContext(maxChunkSize);
             final RowSequence.Iterator selectionIterator = selection.getRowSequenceIterator();
             final WritableLongChunk<OrderedRowKeys> selectionRowKeyChunk =
                     WritableLongChunk.makeWritableChunk(maxChunkSize)) {
            // @formatter:on

            while (selectionIterator.hasMore()) {
                final RowSequence selectionChunk = selectionIterator.getNextRowSequenceWithLength(maxChunkSize);

                final ObjectChunk<?, ?> keyChunk = tupleSource.getChunk(keyGetContext, selectionChunk).asObjectChunk();
                final int thisChunkSize = keyChunk.size();

                selectionRowKeyChunk.setSize(thisChunkSize);
                selectionChunk.fillRowKeyChunk(selectionRowKeyChunk);

                for (int ii = 0; ii < thisChunkSize; ++ii) {
                    if (liveValues.contains(keyChunk.get(ii)) == inclusion) {
                        indexBuilder.appendKey(selectionRowKeyChunk.get(ii));
                    }
                }
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
}
