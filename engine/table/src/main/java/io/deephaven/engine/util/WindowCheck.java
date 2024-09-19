//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.base.Pair;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.iterators.ChunkedLongColumnIterator;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.base.RAPriQueue;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * Adds a Boolean column that is true if a Timestamp is within the specified window.
 */
public class WindowCheck {

    private WindowCheck() {}

    /**
     * <p>
     * Adds a Boolean column that is false when a timestamp column is older than windowNanos.
     * </p>
     *
     * <p>
     * If the timestamp is greater than or equal to the curent time - windowNanos, then the result column is true. If
     * the timestamp is null; the InWindow value is null.
     * </p>
     *
     * <p>
     * The resultant table ticks whenever the input table ticks, or modifies a row when it passes out of the window.
     * </p>
     *
     * <p>
     * The timestamp column must be an Instant or a long value expressed as nanoseconds since the epoch.
     * </p>
     *
     * @param table the input table
     * @param timestampColumn the timestamp column to monitor in table
     * @param windowNanos how many nanoseconds in the past a timestamp can be before it is out of the window
     * @param inWindowColumn the name of the new Boolean column.
     * @return a new table that contains an in-window Boolean column
     */
    @SuppressWarnings("unused")
    public static Table addTimeWindow(QueryTable table, String timestampColumn, long windowNanos,
            String inWindowColumn) {
        return QueryPerformanceRecorder.withNugget("addTimeWindow(" + timestampColumn + ", " + windowNanos + ")",
                table.sizeForInstrumentation(),
                () -> addTimeWindowInternal(null, table, timestampColumn, windowNanos, inWindowColumn, true).first);
    }

    private static class WindowListenerRecorder extends ListenerRecorder {
        private WindowListenerRecorder(Table parent, BaseTable<?> dependent) {
            super("WindowCheck", parent, dependent);
        }
    }

    /**
     * See {@link WindowCheck#addTimeWindow(QueryTable, String, long, String)} for a description, the internal version
     * gives you access to the TimeWindowListener for unit testing purposes.
     *
     * @param addToMonitor should we add this to the PeriodicUpdateGraph
     * @return a pair of the result table and the TimeWindowListener that drives it
     */
    @InternalUseOnly
    public static Pair<Table, TimeWindowListener> addTimeWindowInternal(Clock clock, QueryTable table,
            String timestampColumn, long windowNanos, String inWindowColumn, boolean addToMonitor) {
        if (table.isRefreshing()) {
            table.getUpdateGraph().checkInitiateSerialTableOperation();
        }
        final Map<String, ColumnSource<?>> resultColumns = new LinkedHashMap<>(table.getColumnSourceMap());

        final InWindowColumnSource inWindowColumnSource;
        if (clock == null) {
            inWindowColumnSource = new InWindowColumnSource(table, timestampColumn, windowNanos);
        } else {
            inWindowColumnSource =
                    new InWindowColumnSourceWithClock(clock, table, timestampColumn, windowNanos);
        }
        inWindowColumnSource.init();
        resultColumns.put(inWindowColumn, inWindowColumnSource);

        final QueryTable result = new QueryTable(table.getRowSet(), resultColumns);
        final WindowListenerRecorder recorder = new WindowListenerRecorder(table, result);
        final TimeWindowListenerImpl timeWindowListenerImpl =
                new TimeWindowListenerImpl(inWindowColumn, inWindowColumnSource, recorder, table, result);
        recorder.setMergedListener(timeWindowListenerImpl);
        if (table.isRefreshing()) {
            table.addUpdateListener(recorder);
        }
        timeWindowListenerImpl.addRowSequence(table.getRowSet(), false);
        result.addParentReference(timeWindowListenerImpl);
        if (addToMonitor) {
            result.getUpdateGraph().addSource(timeWindowListenerImpl);
        }
        return new Pair<>(result, timeWindowListenerImpl);
    }

    /**
     * This interface is only for Deephaven internal use.
     */
    @InternalUseOnly
    public interface TimeWindowListener extends Runnable {
        void validateQueue();

        void dumpQueue();
    }

    /**
     * The TimeWindowListener maintains a priority queue of rows that are within a configured window, when they pass out
     * of the window, the InWindow column is set to false and a modification tick happens.
     *
     * <p>
     * It implements {@link Runnable}, so that we can be inserted into the {@link PeriodicUpdateGraph}.
     * </p>
     */
    static class TimeWindowListenerImpl extends MergedListener implements TimeWindowListener {
        private final InWindowColumnSource inWindowColumnSource;
        private final QueryTable result;
        /**
         * A priority queue of entries within our window, with the least recent timestamps getting pulled out first.
         */
        private final RAPriQueue<Entry> priorityQueue;
        /**
         * A sorted map from the last row key in an entry, to our entries.
         */
        private final Long2ObjectAVLTreeMap<Entry> rowKeyToEntry;
        private final ModifiedColumnSet.Transformer mcsTransformer;
        private final ModifiedColumnSet mcsResultWindowColumn;
        private final ModifiedColumnSet mcsSourceTimestamp;
        private final Table source;
        private final ListenerRecorder recorder;

        /**
         * An intrusive entry in priorityQueue, also stored in rowKeyToEntry (for tables with
         * modifications/removes/shifts).
         *
         * <p>
         * Each entry contains a contiguous range of row keys, with non-descending timestamps.
         * </p>
         */
        private static class Entry {
            /**
             * position in the priority queue
             */
            int pos;
            /**
             * the timestamp of the first row key
             */
            long nanos;

            /**
             * the first row key within the source (and result) table
             */
            long firstRowKey;
            /**
             * the last row key within the source (and result) table
             */
            long lastRowKey;


            Entry(final long firstRowKey, final long lastRowKey, final long firstTimestamp) {
                this.firstRowKey = Require.geqZero(firstRowKey, "firstRowKey");
                this.lastRowKey = Require.geq(lastRowKey, "lastRowKey", firstRowKey, "firstRowKey");
                this.nanos = firstTimestamp;
            }

            @Override
            public String toString() {
                return "Entry{" +
                        "nanos=" + nanos +
                        ", firstRowKey=" + firstRowKey +
                        ", lastRowKey=" + lastRowKey +
                        '}';
            }
        }

        /**
         * Creates a TimeWindowListener.
         *
         * @param inWindowColumnSource the resulting InWindowColumnSource, which contains the timestamp source
         * @param source the source table
         * @param result our initialized result table
         */
        private TimeWindowListenerImpl(final String inWindowColumnName, final InWindowColumnSource inWindowColumnSource,
                final ListenerRecorder recorder, final QueryTable source, final QueryTable result) {
            super(Collections.singleton(recorder), List.of(), "WindowCheck", result);
            this.source = source;
            this.recorder = recorder;
            this.inWindowColumnSource = inWindowColumnSource;
            this.result = result;
            // if most things have already passed out of the window, there is no point in allocating a large priority
            // queue; we'll just depend on exponential doubling to get us there if need be
            this.priorityQueue = new RAPriQueue<>(4096, new RAPriQueue.Adapter<>() {
                @Override
                public boolean less(final Entry a, final Entry b) {
                    return a.nanos < b.nanos;
                }

                @Override
                public void setPos(final Entry el, final int pos) {
                    el.pos = pos;
                }

                @Override
                public int getPos(final Entry el) {
                    return el.pos;
                }
            }, Entry.class);

            if (source.isAddOnly()) {
                this.rowKeyToEntry = null;
            } else {
                this.rowKeyToEntry = new Long2ObjectAVLTreeMap<>();
            }

            this.mcsTransformer = source.newModifiedColumnSetTransformer(result,
                    source.getDefinition().getColumnNamesArray());
            this.mcsSourceTimestamp = source.newModifiedColumnSet(inWindowColumnSource.timeStampName);
            this.mcsResultWindowColumn = result.newModifiedColumnSet(inWindowColumnName);
        }

        @Override
        protected void process() {
            if (recorder.recordedVariablesAreValid()) {
                final TableUpdate upstream = recorder.getUpdate();

                // remove the removed row keys from the priority queue
                removeRowSet(upstream.removed(), true);

                // anything that was shifted needs to be placed in the proper slots
                try (final WritableRowSet preShiftRowSet = source.getRowSet().copyPrev()) {
                    preShiftRowSet.remove(upstream.removed());
                    upstream.shifted().apply((start, end, delta) -> {
                        try (final RowSet subRowSet = preShiftRowSet.subSetByKeyRange(start, end)) {
                            shiftSubRowset(subRowSet, delta);
                        }
                    });
                }

                // figure out for all the modified row keys if the timestamp or row key changed
                if (upstream.modifiedColumnSet().containsAny(mcsSourceTimestamp)) {
                    final RowSetBuilderSequential changedTimestampRowsToRemovePost = RowSetFactory.builderSequential();
                    final RowSetBuilderSequential changedTimestampRowsToAddPost = RowSetFactory.builderSequential();

                    final int chunkSize = (int) Math.min(upstream.modified().size(), 4096);

                    try (final ChunkSource.GetContext prevContext =
                            inWindowColumnSource.timeStampSource.makeGetContext(chunkSize);
                            final ChunkSource.GetContext currContext =
                                    inWindowColumnSource.timeStampSource.makeGetContext(chunkSize);
                            final RowSequence.Iterator prevIt = upstream.getModifiedPreShift().getRowSequenceIterator();
                            final RowSequence.Iterator currIt = upstream.modified().getRowSequenceIterator()) {
                        while (currIt.hasMore()) {
                            final RowSequence prevRows = prevIt.getNextRowSequenceWithLength(chunkSize);
                            final RowSequence currRows = currIt.getNextRowSequenceWithLength(chunkSize);
                            final LongChunk<OrderedRowKeys> chunkKeys = currRows.asRowKeyChunk();
                            final LongChunk<? extends Values> prevTimestamps = inWindowColumnSource.timeStampSource
                                    .getPrevChunk(prevContext, prevRows).asLongChunk();
                            final LongChunk<? extends Values> currTimestamps =
                                    inWindowColumnSource.timeStampSource.getChunk(currContext, currRows).asLongChunk();

                            for (int ii = 0; ii < prevTimestamps.size(); ++ii) {
                                final long prevTimestamp = prevTimestamps.get(ii);
                                final long currentTimestamp = currTimestamps.get(ii);
                                if (currentTimestamp != prevTimestamp) {
                                    final boolean prevInWindow = prevTimestamp != QueryConstants.NULL_LONG
                                            && inWindowColumnSource.computeInWindowUnsafePrev(prevTimestamp);
                                    final boolean curInWindow = currentTimestamp != QueryConstants.NULL_LONG
                                            && inWindowColumnSource.computeInWindowUnsafe(currentTimestamp);
                                    final long rowKey = chunkKeys.get(ii);
                                    if (prevInWindow && curInWindow) {
                                        // we might not have actually reordered anything, if we can check that "easily"
                                        // we should do it to avoid churn and reading from the column, first find the
                                        // entry based on our row key
                                        final LongBidirectionalIterator iterator =
                                                rowKeyToEntry.keySet().iterator(rowKey - 1);
                                        // we have to have an entry, otherwise we would not be in the window
                                        Assert.assertion(iterator.hasNext(), "iterator.hasNext()");
                                        final Entry foundEntry = rowKeyToEntry.get(iterator.nextLong());
                                        Assert.neqNull(foundEntry, "foundEntry");

                                        if (foundEntry.firstRowKey == rowKey
                                                && foundEntry.lastRowKey == foundEntry.firstRowKey) {
                                            // we should update the nanos for this entry
                                            foundEntry.nanos = currentTimestamp;
                                            priorityQueue.enter(foundEntry);
                                            continue;
                                        }

                                        /*
                                         * If we want to get fancier, there are some more cases where we could determine
                                         * that there is no need to re-read the data. In particular, we would have to
                                         * know that we have both the previous and next values in our chunk; otherwise
                                         * we would be re-reading data anyway. The counterpoint is that if we are
                                         * actually in those cases, where we are modifying Timestamps that are in the
                                         * window it seems unlikely that the table is going to have consecutive
                                         * timestamp ranges. To encode that logic would be fairly complex, and I think
                                         * not actually worth it.
                                         */
                                    }
                                    if (prevInWindow) {
                                        changedTimestampRowsToRemovePost.appendKey(rowKey);
                                    }
                                    if (curInWindow) {
                                        changedTimestampRowsToAddPost.appendKey(rowKey);
                                    }
                                }
                            }
                        }
                    }

                    // we should have shifted values where relevant above, so we only operate on the new row key
                    try (final RowSet changedTimestamps = changedTimestampRowsToRemovePost.build()) {
                        if (changedTimestamps.isNonempty()) {
                            removeRowSet(changedTimestamps, false);
                        }
                    }
                    try (final RowSet changedTimestamps = changedTimestampRowsToAddPost.build()) {
                        if (changedTimestamps.isNonempty()) {
                            addRowSequence(changedTimestamps, rowKeyToEntry != null);
                        }
                    }
                }

                // now add the new timestamps
                addRowSequence(upstream.added(), rowKeyToEntry != null);

                final TableUpdateImpl downstream =
                        TableUpdateImpl.copy(upstream, result.getModifiedColumnSetForUpdates());

                try (final RowSet modifiedByTime = recomputeModified()) {
                    if (modifiedByTime.isNonempty()) {
                        downstream.modified.writableCast().insert(modifiedByTime);
                    }
                }

                // everything that was added, removed, or modified stays added removed or modified
                if (downstream.modified.isNonempty()) {
                    mcsTransformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
                    downstream.modifiedColumnSet.setAll(mcsResultWindowColumn);
                } else {
                    downstream.modifiedColumnSet.clear();
                }
                result.notifyListeners(downstream);
            } else {
                final RowSet modifiedByTime = recomputeModified();
                if (modifiedByTime.isNonempty()) {
                    final TableUpdateImpl downstream = new TableUpdateImpl();
                    downstream.modified = modifiedByTime;
                    downstream.added = RowSetFactory.empty();
                    downstream.removed = RowSetFactory.empty();
                    downstream.shifted = RowSetShiftData.EMPTY;
                    downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                    downstream.modifiedColumnSet.clear();
                    downstream.modifiedColumnSet.setAll(mcsResultWindowColumn);
                    result.notifyListeners(downstream);
                } else {
                    modifiedByTime.close();
                }
            }
        }

        /**
         * If the value of the timestamp is within the window, insert it into the queue and map.
         *
         * @param rowSequence the row sequence to insert into the table
         * @param tryCombine try to combine newly added ranges with those already in the maps. For initial addition,
         *        there is nothing to combine with, so we do not spend the time on map lookups. For add-only tables, we
         *        do not maintain the rowKeyToEntry map, so cannot find adjacent ranges for combination.
         */
        private void addRowSequence(RowSequence rowSequence, boolean tryCombine) {
            final int chunkSize = (int) Math.min(rowSequence.size(), 4096);
            Entry pendingEntry = null;
            long lastNanos = Long.MAX_VALUE;

            try (final ChunkSource.GetContext getContext =
                    inWindowColumnSource.timeStampSource.makeGetContext(chunkSize);
                    final RowSequence.Iterator rsit = rowSequence.getRowSequenceIterator()) {
                while (rsit.hasMore()) {
                    final RowSequence chunkRows = rsit.getNextRowSequenceWithLength(chunkSize);
                    final LongChunk<OrderedRowKeys> rowKeys = chunkRows.asRowKeyChunk();
                    final LongChunk<? extends Values> timestampValues =
                            inWindowColumnSource.timeStampSource.getChunk(getContext, chunkRows).asLongChunk();
                    for (int ii = 0; ii < rowKeys.size(); ++ii) {
                        final long currentRowKey = rowKeys.get(ii);
                        final long currentTimestamp = timestampValues.get(ii);
                        if (currentTimestamp == QueryConstants.NULL_LONG) {
                            if (pendingEntry != null) {
                                enter(pendingEntry, lastNanos, tryCombine);
                                pendingEntry = null;
                            }
                            continue;
                        }
                        if (pendingEntry != null && (currentTimestamp < lastNanos
                                || pendingEntry.lastRowKey + 1 != currentRowKey)) {
                            enter(pendingEntry, lastNanos, tryCombine);
                            pendingEntry = null;
                        }
                        if (inWindowColumnSource.computeInWindowUnsafe(currentTimestamp)) {
                            lastNanos = currentTimestamp;
                            if (pendingEntry == null) {
                                if (tryCombine) {
                                    // see if this can be combined with the prior entry
                                    final Entry priorEntry = rowKeyToEntry.get(currentRowKey - 1);
                                    if (priorEntry != null && priorEntry.nanos <= currentTimestamp) {
                                        Assert.eq(priorEntry.lastRowKey, "priorEntry.lastRowKey", currentRowKey - 1,
                                                "currentRowKey - 1");
                                        final boolean canCombine;
                                        if (priorEntry.firstRowKey != priorEntry.lastRowKey) {
                                            final long priorEntryLastNanos =
                                                    inWindowColumnSource.timeStampSource.getLong(priorEntry.lastRowKey);
                                            canCombine = priorEntryLastNanos <= currentTimestamp;
                                        } else {
                                            canCombine = true;
                                        }
                                        if (canCombine) {
                                            rowKeyToEntry.remove(currentRowKey - 1);
                                            // Since we might be combining this with an entry later, we should remove it
                                            // so that we don't have extra entries
                                            priorityQueue.remove(priorEntry);
                                            priorEntry.lastRowKey = currentRowKey;
                                            pendingEntry = priorEntry;
                                            continue;
                                        }
                                    }
                                }
                                pendingEntry = new Entry(currentRowKey, currentRowKey, currentTimestamp);
                            } else {
                                Assert.eq(pendingEntry.lastRowKey, "pendingEntry.lastRowKey", currentRowKey - 1,
                                        "currentRowKey - 1");
                                pendingEntry.lastRowKey = currentRowKey;
                            }
                        } else {
                            Assert.eqNull(pendingEntry, "pendingEntry");
                        }
                    }
                }
                if (pendingEntry != null) {
                    enter(pendingEntry, lastNanos, tryCombine);
                }
            }
        }

        /**
         * Add an entry into the priority queue, and if applicable the reverse map
         *
         * @param pendingEntry the entry to insert
         */
        void enter(@NotNull final Entry pendingEntry) {
            priorityQueue.enter(pendingEntry);
            if (rowKeyToEntry != null) {
                rowKeyToEntry.put(pendingEntry.lastRowKey, pendingEntry);
            }
        }

        /**
         * Insert pendingEntry into the queue and map (if applicable).
         *
         * @param pendingEntry the entry to insert into our queue and reverse map
         * @param lastNanos the final nanosecond value of the pending entry to insert, used to determine if we may
         *        combine with the next entry
         * @param tryCombine true if we should combine values with the next entry, previous entries would have been
         *        combined during addRowSequence
         */
        void enter(@NotNull final Entry pendingEntry, final long lastNanos, final boolean tryCombine) {
            if (tryCombine) {
                final LongBidirectionalIterator it = rowKeyToEntry.keySet().iterator(pendingEntry.lastRowKey);
                if (it.hasNext()) {
                    final long nextKey = it.nextLong();
                    final Entry nextEntry = rowKeyToEntry.get(nextKey);
                    if (nextEntry.firstRowKey == pendingEntry.lastRowKey + 1 && nextEntry.nanos >= lastNanos) {
                        // we can combine ourselves into next entry, because it is contiguous and has a timestamp
                        // greater than or equal to our entries last timestamp
                        nextEntry.nanos = pendingEntry.nanos;
                        nextEntry.firstRowKey = pendingEntry.firstRowKey;
                        priorityQueue.enter(nextEntry);
                        return;
                    }
                }
            }
            enter(pendingEntry);
        }

        /**
         * If the keys are in the window, remove them from the map and queue.
         *
         * @param rowSet the row keys to remove
         * @param previous whether to operate in previous space
         */
        private void removeRowSet(final RowSet rowSet, final boolean previous) {
            if (rowSet.isEmpty()) {
                return;
            }
            Assert.neqNull(rowKeyToEntry, "rowKeyToEntry");

            RANGE: for (final RowSet.RangeIterator rangeIterator = rowSet.rangeIterator(); rangeIterator.hasNext();) {
                rangeIterator.next();
                long start = rangeIterator.currentRangeStart();
                final long end = rangeIterator.currentRangeEnd();

                // We have some range in the rowSet that is removed. This range (or part thereof) may or may not exist
                // in one or more entries. We process from the front of the range to the end of the range, possibly
                // advancing the range start.

                while (start <= end) {
                    // we look for start - 1, so that we will find start if it exists
                    // https://fastutil.di.unimi.it/docs/it/unimi/dsi/fastutil/longs/LongSortedSet.html#iterator(long)
                    // "The next element of the returned iterator is the least element of the set that is greater than
                    // the starting point (if there are no elements greater than the starting point, hasNext() will
                    // return false)."
                    final LongBidirectionalIterator reverseMapIterator = rowKeyToEntry.keySet().iterator(start - 1);
                    // if there is no next, then the reverse map contains no values that are greater than or equal to
                    // start, we can actually break out of the entire loop
                    if (!reverseMapIterator.hasNext()) {
                        break RANGE;
                    }

                    final long entryLastKey = reverseMapIterator.nextLong();
                    final Entry entry = rowKeyToEntry.get(entryLastKey);
                    if (entry.firstRowKey > end) {
                        // there is nothing here for us
                        start = entry.lastRowKey + 1;
                        continue;
                    }

                    // there is some part of our start to end range that could be present in this entry.
                    if (entry.firstRowKey >= start) {
                        // we have visually one of the following three situations when start == firstRowKey:
                        // @formatter:off
                        //   [  RANGE  ]
                        //   [  ENTRY    ] - the entry exceeds the range ( case a)
                        //   [  ENTRY  ] - the whole entry is contained (case b)
                        //   [ ENTRY ] - the entry is a prefix - (case c)
                        // @formatter:on

                        // we have visually one of the following three situations when start > firstRowKey:
                        // @formatter:off
                        //   [  RANGE    ]
                        //      [  ENTRY  ] - the entry starts in the middle and terminates after (case a); so we remove a prefix of the entry
                        //      [  ENTRY ] - entry starts in the middle and terminates the at same value (case b); delete the entry
                        //                  [ ENTRY   ] - this cannot happen based on the search (case c)
                        // @formatter:on

                        if (entry.lastRowKey > end) { // (case a)
                            // slice off the beginning of the entry
                            entry.firstRowKey = end + 1;
                            entry.nanos = previous
                                    ? inWindowColumnSource.timeStampSource.getPrevLong(entry.firstRowKey)
                                    : inWindowColumnSource.timeStampSource.getLong(entry.firstRowKey);
                            priorityQueue.enter(entry);
                        } else { // (case b and c)
                            // we are consuming the entire entry, so can remove it from the queue
                            reverseMapIterator.remove();
                            priorityQueue.remove(entry);
                        }
                        // and we look for the next entry after this one
                        start = entry.lastRowKey + 1;
                    } else {
                        // our entry is at least partially before end (because of the check after retrieving it),
                        // and is after start (because of how we searched in the map).

                        // we have visually one of the following three situations:
                        // @formatter:off
                        //     [  RANGE  ]
                        //   [  ENTRY      ] - the entry exceeds the range ( case a), we must split into two entries
                        //   [  ENTRY    ] - the entry starts before the range but ends with the range (case b); so we remove a suffix of the entry
                        //   [ ENTRY  ] - the entry starts before the range and ends inside the range(case c); so we must remove a suffix of the entry
                        // @formatter:on

                        if (entry.lastRowKey > end) {
                            final Entry frontEntry = new Entry(entry.firstRowKey, start - 1, entry.nanos);
                            enter(frontEntry);

                            entry.firstRowKey = end + 1;
                            entry.nanos = previous
                                    ? inWindowColumnSource.timeStampSource.getPrevLong(entry.firstRowKey)
                                    : inWindowColumnSource.timeStampSource.getLong(entry.firstRowKey);
                            priorityQueue.enter(entry);
                        } else { // case b and c
                            entry.lastRowKey = start - 1;
                            reverseMapIterator.remove();
                            rowKeyToEntry.put(entry.lastRowKey, entry);
                        }
                    }
                }
            }
        }

        private void shiftSubRowset(final RowSet rowSet, final long delta) {
            Assert.neqNull(rowKeyToEntry, "rowKeyToEntry");

            // We need to be careful about reinserting entries into the correct order, if we are traversing forward,
            // then we need to add the entries in opposite order to avoid overwriting another entry. We remove the
            // entries
            // in the loop, and if entriesToInsert is non-null add them to the list. If entriesToInsert is null, then
            // we add them to the map.
            final List<Entry> entriesToInsert = delta > 0 ? new ArrayList<>() : null;

            RANGE: for (final RowSet.RangeIterator rangeIterator = rowSet.rangeIterator(); rangeIterator.hasNext();) {
                rangeIterator.next();
                long start = rangeIterator.currentRangeStart();
                final long end = rangeIterator.currentRangeEnd();

                // We have some range in the rowSet that has been moved about. This range (or part thereof) may or may
                // not exist in one or more entries. We process from the front of the range to the end of the range,
                // possibly advancing the range start.

                while (start <= end) {
                    // we look for start - 1, so that we will find start if it exists
                    // https://fastutil.di.unimi.it/docs/it/unimi/dsi/fastutil/longs/LongSortedSet.html#iterator(long)
                    // "The next element of the returned iterator is the least element of the set that is greater than
                    // the starting point (if there are no elements greater than the starting point, hasNext() will
                    // return false)."
                    final LongBidirectionalIterator reverseMapIterator = rowKeyToEntry.keySet().iterator(start - 1);
                    // if there is no next, then the reverse map contains no values that are greater than or equal to
                    // start, we can actually break out of the entire loop
                    if (!reverseMapIterator.hasNext()) {
                        break RANGE;
                    }

                    final long entryLastKey = reverseMapIterator.nextLong();
                    final Entry entry = rowKeyToEntry.get(entryLastKey);
                    if (entry.firstRowKey > end) {
                        // there is nothing here for us
                        start = entry.lastRowKey + 1;
                        continue;
                    }

                    // there is some part of our start to end range that could be present in this entry.
                    if (entry.firstRowKey >= start) {

                        // @formatter:off
                        // we have visually one of the following three situations when start == firstRowKey:
                        //   [  RANGE  ]
                        //   [  ENTRY    ] - the entry exceeds the range ( case a)
                        //   [  ENTRY  ] - the whole entry is contained (case b)
                        //   [ ENTRY ] - the entry is a prefix - (case c)

                        // we have visually one of the following three situations when start > firstRowKey:
                        //   [  RANGE    ]
                        //      [  ENTRY  ] - the entry starts in the middle and terminates after (case a)
                        //      [  ENTRY ] - entry starts in the middle and terminates the at same value (case b)
                        //                  [ ENTRY   ] - this cannot happen based on the search (case c)
                        // @formatter:on

                        // we look for the next entry after this one, but need to make sure to keep that happening in
                        // pre-shift space
                        start = entry.lastRowKey + 1;

                        if (entry.lastRowKey > end) { // (case a)
                            // slice off the beginning of the entry, creating a new entry for the shift
                            final Entry newEntry = new Entry(entry.firstRowKey + delta, end + delta, entry.nanos);

                            entry.firstRowKey = end + 1;
                            entry.nanos = inWindowColumnSource.timeStampSource.getPrevLong(entry.firstRowKey);
                            priorityQueue.enter(entry);
                            priorityQueue.enter(newEntry);

                            addOrDeferEntry(entriesToInsert, newEntry);
                        } else { // (case b and c)
                            // we are consuming the entire entry, so can leave it in the queue as is, but need to change
                            // its reverse mapping
                            entry.firstRowKey += delta;
                            entry.lastRowKey += delta;
                            reverseMapIterator.remove();
                            addOrDeferEntry(entriesToInsert, entry);
                        }
                    } else {
                        // our entry is at least partially before end (because of the check after retrieving it),
                        // and is after start (because of how we searched in the map).

                        // we have visually one of the following three situations:
                        // @formatter:off
                        //     [  RANGE  ]
                        //   [  ENTRY      ] - the entry exceeds the range ( case a), we must split into three entries;
                        //                     but we would be splatting over stuff, so this is not permitted in a reasonable shift
                        //   [  ENTRY    ] - the entry starts before the range but ends with the range (case b)
                        //   [ ENTRY  ] - the entry starts before the range and ends inside the range(case c)
                        // @formatter:on

                        if (entry.lastRowKey > end) {
                            throw new IllegalStateException();
                        } else { // case b and c

                            final long backNanos = inWindowColumnSource.timeStampSource.getPrevLong(start);
                            final Entry backEntry = new Entry(start + delta, entry.lastRowKey + delta, backNanos);
                            priorityQueue.enter(backEntry);

                            // the nanos stays the same, so entry just needs an adjust last rowSet and the reverse map
                            entry.lastRowKey = start - 1;

                            // by reinserting, we preserve the things that we have not changed to enable us to find them
                            // in the rest of the processing
                            reverseMapIterator.remove();
                            rowKeyToEntry.put(entry.lastRowKey, entry);

                            addOrDeferEntry(entriesToInsert, backEntry);
                        }
                    }
                }
            }
            if (entriesToInsert != null) {
                for (int ii = entriesToInsert.size() - 1; ii >= 0; ii--) {
                    final Entry entry = entriesToInsert.get(ii);
                    rowKeyToEntry.put(entry.lastRowKey, entry);
                }
            }
        }

        private void addOrDeferEntry(final List<Entry> entriesToInsert, final Entry entry) {
            if (entriesToInsert == null) {
                rowKeyToEntry.put(entry.lastRowKey, entry);
            } else {
                entriesToInsert.add(entry);
            }
        }

        /**
         * Pop elements out of the queue until we find one that is in the window.
         *
         * <p>
         * Send a modification to the resulting table.
         * </p>
         */
        @Override
        public void run() {
            inWindowColumnSource.captureTime();
            notifyChanges();
        }

        private RowSet recomputeModified() {
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();

            while (true) {
                final Entry entry = priorityQueue.top();
                if (entry == null) {
                    break;
                }

                if (inWindowColumnSource.computeInWindowUnsafe(entry.nanos)) {
                    break;
                }

                // take it out of the queue, and mark it as modified
                final Entry taken = priorityQueue.removeTop();
                Assert.equals(entry, "entry", taken, "taken");


                // now scan the rest of the entry, which requires reading from the timestamp source;
                // this would ideally be done as a chunk, reusing the context
                long newFirst = entry.firstRowKey + 1;
                if (newFirst <= entry.lastRowKey) {
                    try (final RowSequence rowSequence =
                            RowSequenceFactory.forRange(entry.firstRowKey + 1, entry.lastRowKey);
                            final CloseablePrimitiveIteratorOfLong timestampIterator =
                                    new ChunkedLongColumnIterator(inWindowColumnSource.timeStampSource,
                                            rowSequence)) {
                        while (newFirst <= entry.lastRowKey) {
                            final long nanos = timestampIterator.nextLong();
                            if (inWindowColumnSource.computeInWindowUnsafe(nanos)) {
                                // nothing more to do, we've passed out of the window, note the new nanos for this entry
                                entry.nanos = nanos;
                                break;
                            }
                            ++newFirst;
                        }
                    }
                }

                builder.addRange(entry.firstRowKey, newFirst - 1);

                // if anything is left, we need to reinsert it into the priority queue
                if (newFirst <= entry.lastRowKey) {
                    entry.firstRowKey = newFirst;
                    priorityQueue.enter(entry);
                } else if (rowKeyToEntry != null) {
                    rowKeyToEntry.remove(entry.lastRowKey);
                }
            }

            return builder.build();
        }

        @Override
        public void validateQueue() {
            final RowSet resultRowSet = result.getRowSet();
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();

            final Entry[] entries = new Entry[priorityQueue.size()];
            priorityQueue.dump(entries, 0);

            if (rowKeyToEntry != null && entries.length != rowKeyToEntry.size()) {
                dumpQueue();
                Assert.eq(entries.length, "entries.length", rowKeyToEntry.size(), "rowKeyToEntry.size()");
            }

            long entrySize = 0;
            for (final Entry entry : entries) {
                builder.addRange(entry.firstRowKey, entry.lastRowKey);
                entrySize += (entry.lastRowKey - entry.firstRowKey + 1);
                if (rowKeyToEntry != null) {
                    final Entry check = rowKeyToEntry.get(entry.lastRowKey);
                    if (check != entry) {
                        dumpQueue();
                        Assert.equals(check, "check", entry, "entry");
                    }
                }
                // validate that the entry is non-descending
                if (entry.lastRowKey > entry.firstRowKey) {
                    long lastNanos = inWindowColumnSource.timeStampSource.getLong(entry.firstRowKey);
                    for (long rowKey = entry.firstRowKey + 1; rowKey <= entry.lastRowKey; ++rowKey) {
                        long nanos = inWindowColumnSource.timeStampSource.getLong(rowKey);
                        if (nanos < lastNanos) {
                            dumpQueue();
                            Assert.geq(nanos, "nanos at " + rowKey, lastNanos, "lastNanos");
                        }
                        lastNanos = nanos;
                    }
                }
            }

            final RowSet inQueue = builder.build();
            Assert.eq(inQueue.size(), "inQueue.size()", entrySize, "entrySize");

            final boolean condition = inQueue.subsetOf(resultRowSet);
            if (!condition) {
                dumpQueue();
                // noinspection ConstantConditions
                Assert.assertion(condition, "inQueue.subsetOf(resultRowSet)", inQueue, "inQueue", resultRowSet,
                        "resultRowSet", inQueue.minus(resultRowSet), "inQueue.minus(resultRowSet)");
            }

            // Verify that the size of inQueue is equal to the number of values in the window
            final RowSetBuilderSequential inWindowBuilder = RowSetFactory.builderSequential();
            try (final CloseablePrimitiveIteratorOfLong valueIt =
                    new ChunkedLongColumnIterator(inWindowColumnSource.timeStampSource, source.getRowSet())) {
                source.getRowSet().forAllRowKeys(key -> {
                    long value = valueIt.nextLong();
                    if (value != QueryConstants.NULL_LONG && inWindowColumnSource.computeInWindowUnsafe(value)) {
                        inWindowBuilder.appendKey(key);
                    }
                });
            }
            try (final RowSet rowsInWindow = inWindowBuilder.build()) {
                Assert.equals(rowsInWindow, "rowsInWindow", inQueue, "inQueue");
            }
        }

        @Override
        public void dumpQueue() {
            final Entry[] entries = new Entry[priorityQueue.size()];
            priorityQueue.dump(entries, 0);
            System.out.println("Queue size: " + entries.length);
            for (final Entry entry : entries) {
                System.out.println(entry);
            }

            if (rowKeyToEntry != null) {
                System.out.println("Map size: " + rowKeyToEntry.size());
                for (final Long2ObjectMap.Entry<Entry> x : rowKeyToEntry.long2ObjectEntrySet()) {
                    System.out.println(x.getLongKey() + ": " + x.getValue());
                }
            }
        }

        @Override
        public void destroy() {
            super.destroy();
            UpdateGraph updateGraph = result.getUpdateGraph();
            updateGraph.removeSource(this);
        }
    }

    private static class InWindowColumnSourceWithClock extends InWindowColumnSource {
        final private Clock clock;

        InWindowColumnSourceWithClock(Clock clock, Table table, String timestampColumn, long windowNanos) {
            super(table, timestampColumn, windowNanos);
            this.clock = Require.neqNull(clock, "clock");
        }

        @Override
        long getTimeNanos() {
            return clock.currentTimeNanos();
        }
    }

    private static class InWindowColumnSource extends AbstractColumnSource<Boolean>
            implements MutableColumnSourceGetDefaults.ForBoolean {
        private final long windowNanos;
        private final ColumnSource<Long> timeStampSource;
        private final String timeStampName;

        private long prevTime = 0;
        private long currentTime = 0;
        private long clockStep;
        private final long initialStep;

        InWindowColumnSource(Table table, String timestampColumn, long windowNanos) {
            super(Boolean.class);
            this.windowNanos = windowNanos;
            this.timeStampName = timestampColumn;

            clockStep = updateGraph.clock().currentStep();
            initialStep = clockStep;

            final ColumnSource<?> timeStampSource = table.getColumnSource(timestampColumn);
            final ColumnSource<?> reinterpreted = ReinterpretUtils.maybeConvertToPrimitive(timeStampSource);
            Class<?> timestampType = reinterpreted.getType();
            if (timestampType == long.class) {
                // noinspection unchecked
                this.timeStampSource = (ColumnSource<Long>) reinterpreted;
            } else {
                throw new IllegalArgumentException("The timestamp column, " + timestampColumn
                        + ", cannot be interpreted as a long, it should be a supported time type (e.g. long, Instant, ZonedDateTime...)");
            }
        }

        /**
         * Initialize the first currentTime. Called outside the constructor, because subclasses may overload
         * getTimeNanos().
         */
        private void init() {
            currentTime = getTimeNanos();
        }

        long getTimeNanos() {
            return DateTimeUtils.currentClock().currentTimeNanos();
        }

        @Override
        public Boolean get(long rowKey) {
            final long tableTimeStamp = timeStampSource.getLong(rowKey);
            return computeInWindow(tableTimeStamp, currentTime);
        }

        @Override
        public Boolean getPrev(long rowKey) {
            final long time = timeStampForPrev();

            // get the previous value from the underlying column source
            final long tableTimeStamp = timeStampSource.getPrevLong(rowKey);
            return computeInWindow(tableTimeStamp, time);
        }

        private Boolean computeInWindow(long tableNanos, long time) {
            if (tableNanos == QueryConstants.NULL_LONG) {
                return null;
            }
            return (time - tableNanos) < windowNanos;
        }

        private boolean computeInWindowUnsafe(long tableNanos, long time) {
            return (time - tableNanos) < windowNanos;
        }

        private boolean computeInWindowUnsafe(long tableNanos) {
            return computeInWindowUnsafe(tableNanos, currentTime);
        }

        private boolean computeInWindowUnsafePrev(long tableNanos) {
            return computeInWindowUnsafe(tableNanos, timeStampForPrev());
        }

        @Override
        public boolean isImmutable() {
            return false;
        }

        private void captureTime() {
            prevTime = currentTime;
            currentTime = getTimeNanos();
            clockStep = updateGraph.clock().currentStep();
        }

        @Override
        public boolean isStateless() {
            return timeStampSource.isStateless();
        }

        private class InWindowFillContext implements ChunkSource.FillContext {
            private final GetContext innerContext;

            private InWindowFillContext(int size) {
                this.innerContext = timeStampSource.makeGetContext(size);
            }

            @Override
            public void close() {
                innerContext.close();
            }
        }

        @Override
        public InWindowFillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
            return new InWindowFillContext(chunkCapacity);
        }

        @Override
        public void fillChunk(
                @NotNull FillContext context,
                @NotNull WritableChunk<? super Values> destination,
                @NotNull RowSequence rowSequence) {
            final WritableObjectChunk<Boolean, ? super Values> booleanObjectChunk = destination.asWritableObjectChunk();
            final LongChunk<? extends Values> timeChunk = timeStampSource.getChunk(
                    ((InWindowFillContext) context).innerContext, rowSequence).asLongChunk();
            destination.setSize(timeChunk.size());
            for (int ii = 0; ii < timeChunk.size(); ++ii) {
                booleanObjectChunk.set(ii, computeInWindow(timeChunk.get(ii), currentTime));
            }
        }

        @Override
        public void fillPrevChunk(
                @NotNull FillContext context,
                @NotNull WritableChunk<? super Values> destination,
                @NotNull RowSequence rowSequence) {
            final long time = timeStampForPrev();
            final WritableObjectChunk<Boolean, ? super Values> booleanObjectChunk = destination.asWritableObjectChunk();
            final LongChunk<? extends Values> timeChunk = timeStampSource.getPrevChunk(
                    ((InWindowFillContext) context).innerContext, rowSequence).asLongChunk();
            destination.setSize(timeChunk.size());
            for (int ii = 0; ii < timeChunk.size(); ++ii) {
                booleanObjectChunk.set(ii, computeInWindow(timeChunk.get(ii), time));
            }
        }

        private long timeStampForPrev() {
            final long currentStep = updateGraph.clock().currentStep();
            return (clockStep < currentStep || clockStep == initialStep) ? currentTime : prevTime;
        }

        @Override
        public WritableRowSet match(boolean invertMatch, boolean usePrev, boolean caseInsensitive,
                @Nullable DataIndex dataIndex, @NotNull RowSet mapper, Object... keys) {
            final List<Object> keysList = Arrays.asList(keys);
            final boolean includeNull = keysList.contains(null) ^ invertMatch;
            final boolean includeTrue = keysList.contains(true) ^ invertMatch;
            final boolean includeFalse = keysList.contains(false) ^ invertMatch;

            final int getSize = (int) Math.min(4096, mapper.size());

            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

            try (final GetContext getContext = timeStampSource.makeGetContext(getSize);
                    final RowSequence.Iterator rsit = mapper.getRowSequenceIterator()) {
                while (rsit.hasMore()) {
                    final RowSequence chunkRs = rsit.getNextRowSequenceWithLength(getSize);
                    final LongChunk<OrderedRowKeys> rowKeys = chunkRs.asRowKeyChunk();
                    final LongChunk<? extends Values> timeStamps;
                    if (usePrev) {
                        timeStamps = timeStampSource.getPrevChunk(getContext, chunkRs).asLongChunk();
                    } else {
                        timeStamps = timeStampSource.getChunk(getContext, chunkRs).asLongChunk();
                    }
                    final int chunkSize = rowKeys.size();
                    for (int ii = 0; ii < chunkSize; ++ii) {
                        final long rowKey = rowKeys.get(ii);
                        final Boolean inWindow = computeInWindow(timeStamps.get(ii), currentTime);
                        if (inWindow == null) {
                            if (includeNull) {
                                builder.appendKey(rowKey);
                            }
                        } else if (inWindow) {
                            if (includeTrue) {
                                builder.appendKey(rowKey);
                            }
                        } else {
                            if (includeFalse) {
                                builder.appendKey(rowKey);
                            }
                        }
                    }
                }
            }

            return builder.build();
        }
    }
}
