/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.time.TimeProvider;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.base.RAPriQueue;
import gnu.trove.map.hash.TLongObjectHashMap;

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
     * @param table the input table
     * @param timestampColumn the timestamp column to monitor in table
     * @param windowNanos how many nanoseconds in the past a timestamp can be before it is out of the window
     * @param inWindowColumn the name of the new Boolean column.
     * @return a new table that contains an in-window Boolean column
     */
    @SuppressWarnings("unused")
    public static Table addTimeWindow(QueryTable table, String timestampColumn, long windowNanos,
            String inWindowColumn) {
        return addTimeWindowInternal(null, table, timestampColumn, windowNanos, inWindowColumn, true).first;
    }

    private static class WindowListenerRecorder extends ListenerRecorder {
        private WindowListenerRecorder(Table parent, BaseTable dependent) {
            super("WindowCheck", parent, dependent);
        }
    }

    /**
     * See {@link WindowCheck#addTimeWindow(QueryTable, String, long, String)} for a description, the internal version
     * gives you access to the TimeWindowListener for unit testing purposes.
     *
     * @param addToMonitor should we add this to the UpdateGraphProcessor
     * @return a pair of the result table and the TimeWindowListener that drives it
     */
    static Pair<Table, TimeWindowListener> addTimeWindowInternal(TimeProvider timeProvider, QueryTable table,
            String timestampColumn, long windowNanos, String inWindowColumn, boolean addToMonitor) {
        UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
        final Map<String, ColumnSource<?>> resultColumns = new LinkedHashMap<>(table.getColumnSourceMap());

        final InWindowColumnSource inWindowColumnSource;
        if (timeProvider == null) {
            inWindowColumnSource = new InWindowColumnSource(table, timestampColumn, windowNanos);
        } else {
            inWindowColumnSource =
                    new InWindowColumnSourceWithTimeProvider(timeProvider, table, timestampColumn, windowNanos);
        }
        inWindowColumnSource.init();
        resultColumns.put(inWindowColumn, inWindowColumnSource);

        final QueryTable result = new QueryTable(table.getRowSet(), resultColumns);
        final WindowListenerRecorder recorder = new WindowListenerRecorder(table, result);
        final TimeWindowListener timeWindowListener =
                new TimeWindowListener(inWindowColumn, inWindowColumnSource, recorder, table, result);
        recorder.setMergedListener(timeWindowListener);
        table.listenForUpdates(recorder);
        table.getRowSet().forAllRowKeys(timeWindowListener::addIndex);
        result.addParentReference(timeWindowListener);
        result.manage(table);
        if (addToMonitor) {
            UpdateGraphProcessor.DEFAULT.addSource(timeWindowListener);
        }
        return new Pair<>(result, timeWindowListener);
    }

    /**
     * The TimeWindowListener maintains a priority queue of rows that are within a configured window, when they pass out
     * of the window, the InWindow column is set to false and a modification tick happens.
     *
     * It implements {@link Runnable}, so that we can be inserted into the {@link UpdateGraphProcessor}.
     */
    static class TimeWindowListener extends MergedListener implements Runnable {
        private final InWindowColumnSource inWindowColumnSource;
        private final QueryTable result;
        /** a priority queue of InWindow entries, with the least recent timestamps getting pulled out first. */
        private final RAPriQueue<Entry> priorityQueue;
        /** a map from table indices to our entries. */
        private final TLongObjectHashMap<Entry> indexToEntry;
        private final ModifiedColumnSet.Transformer mcsTransformer;
        private final ModifiedColumnSet mcsNewColumns;
        private final ModifiedColumnSet reusableModifiedColumnSet;
        private final Table source;
        private final ListenerRecorder recorder;

        /**
         * An intrusive entry inside of indexToEntry and priorityQueue.
         */
        private static class Entry {
            /** position in the priority queue */
            int pos;
            /** the timestamp */
            long nanos;
            /** the row key within the source (and result) table */
            long index;

            Entry(long index, long timestamp) {
                this.index = Require.geqZero(index, "rowSet");
                this.nanos = timestamp;
            }

            @Override
            public String toString() {
                return "Entry{" +
                        "nanos=" + nanos +
                        ", rowSet=" + index +
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
        private TimeWindowListener(final String inWindowColumnName, final InWindowColumnSource inWindowColumnSource,
                final ListenerRecorder recorder, final QueryTable source, final QueryTable result) {
            super(Collections.singleton(recorder), Collections.singleton(source), "WindowCheck", result);
            this.source = source;
            this.recorder = recorder;
            this.inWindowColumnSource = inWindowColumnSource;
            this.result = result;
            this.priorityQueue = new RAPriQueue<>(1 + source.intSize("WindowCheck"), new RAPriQueue.Adapter<Entry>() {
                @Override
                public boolean less(Entry a, Entry b) {
                    return a.nanos < b.nanos;
                }

                @Override
                public void setPos(Entry el, int pos) {
                    el.pos = pos;
                }

                @Override
                public int getPos(Entry el) {
                    return el.pos;
                }
            }, Entry.class);

            this.indexToEntry = new TLongObjectHashMap<>();

            this.mcsTransformer = source.newModifiedColumnSetTransformer(result,
                    source.getColumnSourceMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
            this.mcsNewColumns = result.newModifiedColumnSet(inWindowColumnName);
            this.reusableModifiedColumnSet = new ModifiedColumnSet(this.mcsNewColumns);
        }

        @Override
        protected void process() {
            if (recorder.recordedVariablesAreValid()) {
                final TableUpdate upstream = recorder.getUpdate();

                // remove the removed indices from the priority queue
                removeIndex(upstream.removed());

                // anything that was shifted needs to be placed in the proper slots
                try (final RowSet preShiftRowSet = source.getRowSet().copyPrev()) {
                    upstream.shifted().apply((start, end, delta) -> {
                        final RowSet subRowSet = preShiftRowSet.subSetByKeyRange(start, end);

                        final RowSet.SearchIterator it =
                                delta < 0 ? subRowSet.searchIterator() : subRowSet.reverseIterator();
                        while (it.hasNext()) {
                            final long idx = it.nextLong();
                            final Entry entry = indexToEntry.remove(idx);
                            if (entry != null) {
                                entry.index = idx + delta;
                                indexToEntry.put(idx + delta, entry);
                            }
                        }
                    });
                }

                // TODO: improve performance with getChunk
                // TODO: reinterpret inWindowColumnSource so that it compares longs instead of objects

                // figure out for all the modified row keys if the timestamp or row key changed
                upstream.forAllModified((oldIndex, newIndex) -> {
                    final DateTime currentTimestamp = inWindowColumnSource.timeStampSource.get(newIndex);
                    final DateTime prevTimestamp = inWindowColumnSource.timeStampSource.getPrev(oldIndex);
                    if (!Objects.equals(currentTimestamp, prevTimestamp)) {
                        updateIndex(newIndex, currentTimestamp);
                    }
                });

                // now add the new timestamps
                upstream.added().forAllRowKeys(this::addIndex);

                final WritableRowSet downstreamModified = upstream.modified().copy();
                try (final RowSet modifiedByTime = recomputeModified()) {
                    if (modifiedByTime.isNonempty()) {
                        downstreamModified.insert(modifiedByTime);
                    }
                }

                // everything that was added, removed, or modified stays added removed or modified
                if (downstreamModified.isNonempty()) {
                    mcsTransformer.clearAndTransform(upstream.modifiedColumnSet(), reusableModifiedColumnSet);
                    reusableModifiedColumnSet.setAll(mcsNewColumns);
                } else {
                    reusableModifiedColumnSet.clear();
                }
                result.notifyListeners(new TableUpdateImpl(upstream.added().copy(), upstream.removed().copy(),
                        downstreamModified, upstream.shifted(), reusableModifiedColumnSet));
            } else {
                final RowSet modifiedByTime = recomputeModified();
                if (modifiedByTime.isNonempty()) {
                    final TableUpdateImpl downstream = new TableUpdateImpl();
                    downstream.modified = modifiedByTime;
                    downstream.added = RowSetFactory.empty();
                    downstream.removed = RowSetFactory.empty();
                    downstream.shifted = RowSetShiftData.EMPTY;
                    downstream.modifiedColumnSet = reusableModifiedColumnSet;
                    downstream.modifiedColumnSet().clear();
                    downstream.modifiedColumnSet().setAll(mcsNewColumns);
                    result.notifyListeners(downstream);
                } else {
                    modifiedByTime.close();
                }
            }
        }

        /**
         * Handles modified indices. If they are outside of the window, they need to be removed from the queue. If they
         * are inside the window, they need to be (re)inserted into the queue.
         */
        private void updateIndex(final long index, DateTime currentTimestamp) {
            Entry entry = indexToEntry.remove(index);
            if (currentTimestamp == null) {
                if (entry != null) {
                    priorityQueue.remove(entry);
                }
                return;
            }
            if (inWindowColumnSource.computeInWindow(currentTimestamp, inWindowColumnSource.currentTime)) {
                if (entry == null) {
                    entry = new Entry(index, 0);
                }
                entry.nanos = currentTimestamp.getNanos();
                priorityQueue.enter(entry);
                indexToEntry.put(entry.index, entry);
            } else if (entry != null) {
                priorityQueue.remove(entry);
            }
        }

        /**
         * If the value of the timestamp is within the window, insert it into the queue and map.
         *
         * @param index the row key inserted into the table
         */
        private void addIndex(long index) {
            final DateTime currentTimestamp = inWindowColumnSource.timeStampSource.get(index);
            if (currentTimestamp == null) {
                return;
            }
            if (inWindowColumnSource.computeInWindow(currentTimestamp, inWindowColumnSource.currentTime)) {
                final Entry el = new Entry(index, currentTimestamp.getNanos());
                priorityQueue.enter(el);
                indexToEntry.put(el.index, el);
            }
        }

        /**
         * If the keys are in the window, remove them from the map and queue.
         *
         * @param rowSet the row keys to remove
         */
        private void removeIndex(final RowSet rowSet) {
            rowSet.forAllRowKeys((final long key) -> {
                final Entry e = indexToEntry.remove(key);
                if (e != null) {
                    priorityQueue.remove(e);
                }
            });
        }

        /**
         * Pop elements out of the queue until we find one that is in the window.
         *
         * Send a modification to the resulting table.
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

                if (inWindowColumnSource.computeInWindow(entry.nanos, inWindowColumnSource.currentTime)) {
                    break;
                } else {
                    // take it out of the queue, and mark it as modified
                    final Entry taken = priorityQueue.removeTop();
                    Assert.equals(entry, "entry", taken, "taken");
                    builder.addKey(entry.index);
                    indexToEntry.remove(entry.index);
                }
            }

            return builder.build();
        }

        void validateQueue() {
            final RowSet resultRowSet = result.getRowSet();
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();

            final Entry[] entries = new Entry[priorityQueue.size()];
            priorityQueue.dump(entries, 0);
            Arrays.stream(entries).mapToLong(entry -> entry.index).forEach(builder::addKey);

            final RowSet inQueue = builder.build();
            Assert.eq(inQueue.size(), "inQueue.size()", priorityQueue.size(), "priorityQueue.size()");
            final boolean condition = inQueue.subsetOf(resultRowSet);
            if (!condition) {
                // noinspection ConstantConditions
                Assert.assertion(condition, "inQueue.subsetOf(resultRowSet)", inQueue, "inQueue", resultRowSet,
                        "resultRowSet", inQueue.minus(resultRowSet), "inQueue.minus(resultRowSet)");
            }
        }

        @Override
        public void destroy() {
            super.destroy();
            UpdateGraphProcessor.DEFAULT.removeSource(this);
        }
    }

    private static class InWindowColumnSourceWithTimeProvider extends InWindowColumnSource {
        final private TimeProvider timeProvider;

        InWindowColumnSourceWithTimeProvider(TimeProvider timeProvider, Table table, String timestampColumn,
                long windowNanos) {
            super(table, timestampColumn, windowNanos);
            this.timeProvider = Require.neqNull(timeProvider, "timeProvider");
        }

        @Override
        long getTimeNanos() {
            return timeProvider.currentTime().getNanos();
        }
    }

    private static class InWindowColumnSource extends AbstractColumnSource<Boolean>
            implements MutableColumnSourceGetDefaults.ForBoolean {
        private final long windowNanos;
        private final ColumnSource<DateTime> timeStampSource;

        private long prevTime = 0;
        private long currentTime = 0;
        private long clockStep = LogicalClock.DEFAULT.currentStep();
        private final long initialStep = clockStep;

        InWindowColumnSource(Table table, String timestampColumn, long windowNanos) {
            super(Boolean.class);
            this.windowNanos = windowNanos;

            this.timeStampSource = table.getColumnSource(timestampColumn, DateTime.class);
            if (!DateTime.class.isAssignableFrom(timeStampSource.getType())) {
                throw new IllegalArgumentException(timestampColumn + " is not of type DateTime!");
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
            return DateTimeUtils.currentTime().getNanos();
        }

        @Override
        public Boolean get(long rowKey) {
            final DateTime tableTimeStamp = timeStampSource.get(rowKey);
            return computeInWindow(tableTimeStamp, currentTime);
        }

        @Override
        public Boolean getPrev(long rowKey) {
            final long currentStep = LogicalClock.DEFAULT.currentStep();

            final long time = (clockStep < currentStep || clockStep == initialStep) ? currentTime : prevTime;

            // get the previous value from the underlying column source
            final DateTime tableTimeStamp = timeStampSource.getPrev(rowKey);
            return computeInWindow(tableTimeStamp, time);
        }

        private Boolean computeInWindow(DateTime tableTimeStamp, long time) {
            if (tableTimeStamp == null) {
                return null;
            }
            final long tableNanos = tableTimeStamp.getNanos();
            return computeInWindow(tableNanos, time);
        }

        private boolean computeInWindow(long tableNanos, long time) {
            return (time - tableNanos) < windowNanos;
        }

        @Override
        public boolean isImmutable() {
            return false;
        }

        private void captureTime() {
            prevTime = currentTime;
            currentTime = getTimeNanos();
            clockStep = LogicalClock.DEFAULT.currentStep();
        }

        @Override
        public boolean isStateless() {
            return timeStampSource.isStateless();
        }
    }
}
