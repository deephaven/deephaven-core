/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.RowSetIndexer;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.select.setinclusion.SetInclusionKernel;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.*;

/**
 * A where filter that extracts a set of inclusion or exclusion keys from a set table.
 *
 * Each time the set table ticks, the entire where filter is recalculated.
 */
public class DynamicWhereFilter extends WhereFilterLivenessArtifactImpl implements NotificationQueue.Dependency {
    private static final int CHUNK_SIZE = 1 << 16;

    private final MatchPair[] matchPairs;
    private final TupleSource setTupleSource;
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
        if (setTable.isRefreshing()) {
            UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
        }

        this.matchPairs = setColumnsNames;
        this.inclusion = inclusion;

        this.setTable = setTable;
        final ColumnSource[] setColumns =
                Arrays.stream(matchPairs).map(mp -> setTable.getColumnSource(mp.rightColumn()))
                        .toArray(ColumnSource[]::new);
        setTupleSource = TupleSourceFactory.makeTupleSource(setColumns);

        setTable.getRowSet().forAllRowKeys((final long v) -> addKey(makeKey(v)));

        if (DynamicNode.isDynamicAndIsRefreshing(setTable)) {
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
            setTable.listenForUpdates(setUpdateListener);

            manage(setUpdateListener);
        } else {
            setUpdateListener = null;
        }
    }

    private Object makeKey(long index) {
        return setTupleSource.createTuple(index);
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

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        final ColumnSource[] keyColumns =
                Arrays.stream(matchPairs).map(mp -> table.getColumnSource(mp.leftColumn()))
                        .toArray(ColumnSource[]::new);
        final TupleSource tupleSource = TupleSourceFactory.makeTupleSource(keyColumns);
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

            final ColumnSource[] sourcesWithGroupings =
                    Arrays.stream(keyColumns).filter(selectionIndexer::hasGrouping)
                            .toArray(ColumnSource[]::new);
            final OptionalInt minGroupCount =
                    Arrays.stream(sourcesWithGroupings).mapToInt(x -> selectionIndexer.getGrouping(x).size())
                            .min();
            if (minGroupCount.isPresent() && (minGroupCount.getAsInt() * 4L) < selection.size()) {
                return filterGrouping(trackingSelection, selectionIndexer, tupleSource);
            }
        }
        return filterLinear(selection, keyColumns, tupleSource);
    }

    private WritableRowSet filterGrouping(TrackingRowSet selection, RowSetIndexer selectionIndexer,
            TupleSource tupleSource) {
        final RowSet matchingKeys = selectionIndexer.getSubSetForKeySet(liveValues, tupleSource);
        return (inclusion ? matchingKeys.copy() : selection.minus(matchingKeys));
    }

    private WritableRowSet filterGrouping(TrackingRowSet selection, RowSetIndexer selectionIndexer, Table table) {
        final ColumnSource[] keyColumns =
                Arrays.stream(matchPairs).map(mp -> table.getColumnSource(mp.leftColumn()))
                        .toArray(ColumnSource[]::new);
        final TupleSource tupleSource = TupleSourceFactory.makeTupleSource(keyColumns);
        return filterGrouping(selection, selectionIndexer, tupleSource);
    }

    private WritableRowSet filterLinear(RowSet selection, ColumnSource[] keyColumns, TupleSource tupleSource) {
        if (keyColumns.length == 1) {
            return filterLinearOne(selection, keyColumns[0]);
        } else {
            return filterLinearTuple(selection, tupleSource);
        }
    }

    private WritableRowSet filterLinearOne(RowSet selection, ColumnSource keyColumn) {
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
            final WritableLongChunk<OrderedRowKeys> keyIndices =
                    WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            final WritableBooleanChunk<Values> matches = WritableBooleanChunk.makeWritableChunk(CHUNK_SIZE);

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);

                final Chunk<Values> chunk = keyColumn.getChunk(getContext, chunkOk);
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

    private WritableRowSet filterLinearTuple(RowSet selection, TupleSource tupleSource) {
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
        return setTable.isRefreshing();
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
