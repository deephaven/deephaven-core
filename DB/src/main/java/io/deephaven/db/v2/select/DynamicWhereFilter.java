/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.select.setinclusion.SetInclusionKernel;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableBooleanChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.db.v2.tuples.TupleSourceFactory;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.*;

/**
 * A where filter that extracts a set of inclusion or exclusion keys from a set table.
 *
 * Each time the set table ticks, the entire where filter is recalculated.
 */
public class DynamicWhereFilter extends SelectFilterLivenessArtifactImpl
    implements NotificationQueue.Dependency {
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
    private final Table setTable;
    @SuppressWarnings("FieldCanBeLocal")
    // this reference must be maintained for reachability
    private final InstrumentedShiftAwareListener setUpdateListener;

    private final Table.GroupStrategy groupStrategy;

    private RecomputeListener listener;
    private QueryTable resultTable;

    public DynamicWhereFilter(final Table setTable, final boolean inclusion,
        final MatchPair... setColumnsNames) {
        this(Table.GroupStrategy.DEFAULT, setTable, inclusion, setColumnsNames);
    }

    public DynamicWhereFilter(final Table.GroupStrategy groupStrategy, final Table setTable,
        final boolean inclusion, final MatchPair... setColumnsNames) {
        if (setTable.isLive()) {
            LiveTableMonitor.DEFAULT.checkInitiateTableOperation();
        }

        this.groupStrategy = groupStrategy;
        this.matchPairs = setColumnsNames;
        this.inclusion = inclusion;

        this.setTable = setTable;
        final ColumnSource[] setColumns = Arrays.stream(matchPairs)
            .map(mp -> setTable.getColumnSource(mp.right())).toArray(ColumnSource[]::new);
        setTupleSource = TupleSourceFactory.makeTupleSource(setColumns);

        setTable.getIndex().forAllLongs((final long v) -> addKey(makeKey(v)));

        if (DynamicNode.isDynamicAndIsRefreshing(setTable)) {
            final String[] columnNames =
                Arrays.stream(matchPairs).map(MatchPair::right).toArray(String[]::new);
            final ModifiedColumnSet modTokenSet =
                ((DynamicTable) setTable).newModifiedColumnSet(columnNames);
            setUpdateListener = new InstrumentedShiftAwareListenerAdapter(
                "DynamicWhereFilter(" + Arrays.toString(setColumnsNames) + ")",
                (DynamicTable) setTable, false) {

                @Override
                public void onUpdate(final Update upstream) {
                    if (upstream.added.empty() && upstream.removed.empty()
                        && !upstream.modifiedColumnSet.containsAny(modTokenSet)) {
                        return;
                    }

                    final MutableBoolean trueModification = new MutableBoolean(false);

                    upstream.added.forAllLongs((final long v) -> addKey(makeKey(v)));
                    upstream.removed.forAllLongs((final long v) -> removeKey(makePrevKey(v)));

                    upstream.forAllModified((preIndex, postIndex) -> {
                        final Object oldKey = makePrevKey(preIndex);
                        final Object newKey = makeKey(postIndex);
                        if (!Objects.equals(oldKey, newKey)) {
                            trueModification.setTrue();
                            removeKey(oldKey);
                            addKey(newKey);
                        }
                    });

                    // Pretend every row of the original table was modified, this is essential so
                    // that the where clause
                    // can be re-evaluated based on the updated live set.
                    if (listener != null) {
                        if (upstream.added.nonempty() || trueModification.booleanValue()) {
                            if (inclusion) {
                                listener.requestRecomputeUnmatched();
                            } else {
                                listener.requestRecomputeMatched();
                            }
                        }
                        if (upstream.removed.nonempty() || trueModification.booleanValue()) {
                            if (inclusion) {
                                listener.requestRecomputeMatched();
                            } else {
                                listener.requestRecomputeUnmatched();
                            }
                        }
                    }
                }

                @Override
                public void onFailureInternal(Throwable originalException,
                    UpdatePerformanceTracker.Entry sourceEntry) {
                    if (listener != null) {
                        resultTable.notifyListenersOnError(originalException, sourceEntry);
                    }
                }
            };
            ((DynamicTable) setTable).listenForUpdates(setUpdateListener);

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
    public Index filter(Index selection, Index fullSet, Table table, boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        final ColumnSource[] keyColumns = Arrays.stream(matchPairs)
            .map(mp -> table.getColumnSource(mp.left())).toArray(ColumnSource[]::new);
        final TupleSource tupleSource = TupleSourceFactory.makeTupleSource(keyColumns);

        switch (groupStrategy) {
            case DEFAULT: {
                if (matchPairs.length == 1) {
                    // this is just a single column filter so it will actually be exactly right
                    if (!liveValuesArrayValid) {
                        liveValuesArray =
                            liveValues.toArray(CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);
                        liveValuesArrayValid = true;
                    }
                    return table.getColumnSource(matchPairs[0].left()).match(!inclusion, false,
                        false, selection, liveValuesArray);
                }

                // pick something sensible
                if (selection.hasGrouping(keyColumns)) {
                    if (selection.size() > (selection.getGrouping(tupleSource).size() * 2)) {
                        return filterGrouping(selection, tupleSource);
                    } else {
                        return filterLinear(selection, keyColumns, tupleSource);
                    }
                }
                final boolean allGrouping =
                    Arrays.stream(keyColumns).allMatch(selection::hasGrouping);
                if (allGrouping) {
                    return filterGrouping(selection, tupleSource);
                }

                final ColumnSource[] sourcesWithGroupings = Arrays.stream(keyColumns)
                    .filter(selection::hasGrouping).toArray(ColumnSource[]::new);
                final OptionalInt minGroupCount = Arrays.stream(sourcesWithGroupings)
                    .mapToInt(x -> selection.getGrouping(x).size()).min();
                if (minGroupCount.isPresent()
                    && (minGroupCount.getAsInt() * 4) < selection.size()) {
                    return filterGrouping(selection, tupleSource);
                }
                return filterLinear(selection, keyColumns, tupleSource);
            }
            case USE_EXISTING_GROUPS:
                if (selection.hasGrouping(keyColumns)) {
                    return filterGrouping(selection, tupleSource);
                } else {
                    return filterLinear(selection, keyColumns, tupleSource);
                }
            case CREATE_GROUPS:
                return filterGrouping(selection, table);
            case LINEAR:
                return filterLinear(selection, keyColumns, tupleSource);
        }
        throw Assert.statementNeverExecuted();
    }

    private Index filterGrouping(Index selection, TupleSource tupleSource) {
        final Index matchingKeys = selection.getSubIndexForKeySet(liveValues, tupleSource);
        return inclusion ? matchingKeys : selection.minus(matchingKeys);
    }

    private Index filterGrouping(Index selection, Table table) {
        final ColumnSource[] keyColumns = Arrays.stream(matchPairs)
            .map(mp -> table.getColumnSource(mp.left())).toArray(ColumnSource[]::new);
        final TupleSource tupleSource = TupleSourceFactory.makeTupleSource(keyColumns);
        return filterGrouping(selection, tupleSource);
    }

    private Index filterLinear(Index selection, ColumnSource[] keyColumns,
        TupleSource tupleSource) {
        if (keyColumns.length == 1) {
            return filterLinearOne(selection, keyColumns[0]);
        } else {
            return filterLinearTuple(selection, tupleSource);
        }
    }

    private Index filterLinearOne(Index selection, ColumnSource keyColumn) {
        if (selection.empty()) {
            return Index.FACTORY.getEmptyIndex();
        }

        if (!kernelValid) {
            setInclusionKernel =
                SetInclusionKernel.makeKernel(keyColumn.getChunkType(), liveValues, inclusion);
            kernelValid = true;
        }

        final Index.SequentialBuilder indexBuilder = Index.FACTORY.getSequentialBuilder();

        try (final ColumnSource.GetContext getContext = keyColumn.makeGetContext(CHUNK_SIZE);
            final OrderedKeys.Iterator okIt = selection.getOrderedKeysIterator()) {
            final WritableLongChunk<Attributes.OrderedKeyIndices> keyIndices =
                WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            final WritableBooleanChunk<Attributes.Values> matches =
                WritableBooleanChunk.makeWritableChunk(CHUNK_SIZE);

            while (okIt.hasMore()) {
                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(CHUNK_SIZE);

                final Chunk<Attributes.Values> chunk = keyColumn.getChunk(getContext, chunkOk);
                setInclusionKernel.matchValues(chunk, matches);

                keyIndices.setSize(chunk.size());
                chunkOk.fillKeyIndicesChunk(keyIndices);

                for (int ii = 0; ii < chunk.size(); ++ii) {
                    if (matches.get(ii)) {
                        indexBuilder.appendKey(keyIndices.get(ii));
                    }
                }
            }
        }


        return indexBuilder.getIndex();
    }

    private Index filterLinearTuple(Index selection, TupleSource tupleSource) {
        final Index.SequentialBuilder indexBuilder = Index.FACTORY.getSequentialBuilder();

        for (final Index.Iterator it = selection.iterator(); it.hasNext();) {
            final long row = it.nextLong();
            final Object tuple = tupleSource.createTuple(row);
            if (liveValues.contains(tuple) == inclusion) {
                indexBuilder.appendKey(row);
            }
        }

        return indexBuilder.getIndex();
    }

    @Override
    public boolean isSimpleFilter() {
        /*
         * This doesn't execute any user code, so it should be safe to execute it against untrusted
         * data.
         */
        return true;
    }

    @Override
    public boolean isRefreshing() {
        return setTable.isLive();
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
        return new DynamicWhereFilter(groupStrategy, setTable, inclusion, matchPairs);
    }

    @Override
    public boolean satisfied(final long step) {
        return setUpdateListener == null || setUpdateListener.satisfied(step);
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("DynamicWhereFilter(")
            .append(MatchPair.MATCH_PAIR_ARRAY_FORMATTER, matchPairs).append(")");
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }
}
