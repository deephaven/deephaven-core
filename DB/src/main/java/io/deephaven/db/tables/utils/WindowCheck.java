package io.deephaven.db.tables.utils;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.sources.MutableColumnSourceGetDefaults;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.TimeProvider;
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
     * If the timestamp is greater than or equal to the curent time - windowNanos, then the result
     * column is true. If the timestamp is null; the InWindow value is null.
     * </p>
     *
     * <p>
     * The resultant table ticks whenever the input table ticks, or modifies a row when it passes
     * out of the window.
     * </p>
     *
     * @param table the input table
     * @param timestampColumn the timestamp column to monitor in table
     * @param windowNanos how many nanoseconds in the past a timestamp can be before it is out of
     *        the window
     * @param inWindowColumn the name of the new Boolean column.
     * @return a new table that contains an in-window Boolean column
     */
    @SuppressWarnings("unused")
    public static Table addTimeWindow(Table table, String timestampColumn, long windowNanos,
        String inWindowColumn) {
        return addTimeWindowInternal(null, table, timestampColumn, windowNanos, inWindowColumn,
            true).first;
    }

    private static class WindowListenerRecorder extends ListenerRecorder {
        private WindowListenerRecorder(DynamicTable parent, DynamicTable dependent) {
            super("WindowCheck", parent, dependent);
        }
    }

    /**
     * See {@link WindowCheck#addTimeWindow(Table, String, long, String)} for a description, the
     * internal version gives you access to the TimeWindowListener for unit testing purposes.
     *
     * @param addToMonitor should we add this to the LiveTableMonitor
     * @return a pair of the result table and the TimeWindowListener that drives it
     */
    static Pair<Table, TimeWindowListener> addTimeWindowInternal(TimeProvider timeProvider,
        Table table, String timestampColumn, long windowNanos, String inWindowColumn,
        boolean addToMonitor) {
        final Map<String, ColumnSource> resultColumns =
            new LinkedHashMap<>(table.getColumnSourceMap());

        final InWindowColumnSource inWindowColumnSource;
        if (timeProvider == null) {
            inWindowColumnSource = new InWindowColumnSource(table, timestampColumn, windowNanos);
        } else {
            inWindowColumnSource = new InWindowColumnSourceWithTimeProvider(timeProvider, table,
                timestampColumn, windowNanos);
        }
        inWindowColumnSource.init();
        resultColumns.put(inWindowColumn, inWindowColumnSource);

        final QueryTable result = new QueryTable(table.getIndex(), resultColumns);

        if (table instanceof DynamicTable) {
            final DynamicTable dynamicSource = (DynamicTable) table;
            final WindowListenerRecorder recorder =
                new WindowListenerRecorder(dynamicSource, result);
            final TimeWindowListener timeWindowListener = new TimeWindowListener(inWindowColumn,
                inWindowColumnSource, recorder, dynamicSource, result);
            recorder.setMergedListener(timeWindowListener);
            dynamicSource.listenForUpdates(recorder);
            table.getIndex().forAllLongs(timeWindowListener::addIndex);
            result.addParentReference(timeWindowListener);
            result.manage(table);
            if (addToMonitor) {
                LiveTableMonitor.DEFAULT.addTable(timeWindowListener);
            }
            return new Pair<>(result, timeWindowListener);
        } else {
            return new Pair<>(result, null);
        }
    }

    /**
     * The TimeWindowListener maintains a priority queue of rows that are within a configured
     * window, when they pass out of the window, the InWindow column is set to false and a
     * modification tick happens.
     *
     * It implements LiveTable, so that we can be inserted into the LiveTableMonitor.
     */
    static class TimeWindowListener extends MergedListener implements LiveTable {
        private final InWindowColumnSource inWindowColumnSource;
        private final QueryTable result;
        /**
         * a priority queue of InWindow entries, with the least recent timestamps getting pulled out
         * first.
         */
        private final RAPriQueue<Entry> priorityQueue;
        /** a map from table indices to our entries. */
        private final TLongObjectHashMap<Entry> indexToEntry;
        private final Index EMPTY_INDEX = Index.FACTORY.getEmptyIndex();
        private final ModifiedColumnSet.Transformer mcsTransformer;
        private final ModifiedColumnSet mcsNewColumns;
        private final ModifiedColumnSet reusableModifiedColumnSet;
        private final DynamicTable source;
        private final ListenerRecorder recorder;

        /**
         * An intrusive entry inside of indexToEntry and priorityQueue.
         */
        private static class Entry {
            /** position in the priority queue */
            int pos;
            /** the timestamp */
            long nanos;
            /** the index within the source (and result) table */
            long index;

            Entry(long index, long timestamp) {
                this.index = Require.geqZero(index, "index");
                this.nanos = timestamp;
            }

            @Override
            public String toString() {
                return "Entry{" +
                    "nanos=" + nanos +
                    ", index=" + index +
                    '}';
            }
        }

        /**
         * Creates a TimeWindowListener.
         *
         * @param inWindowColumnSource the resulting InWindowColumnSource, which contains the
         *        timestamp source
         * @param source the source table
         * @param result our initialized result table
         */
        private TimeWindowListener(final String inWindowColumnName,
            final InWindowColumnSource inWindowColumnSource,
            final ListenerRecorder recorder, final DynamicTable source, final QueryTable result) {
            super(Collections.singleton(recorder), Collections.singleton(source), "WindowCheck",
                result);
            this.source = source;
            this.recorder = recorder;
            this.inWindowColumnSource = inWindowColumnSource;
            this.result = result;
            this.priorityQueue = new RAPriQueue<>(1 + source.intSize("WindowCheck"),
                new RAPriQueue.Adapter<Entry>() {
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

            this.mcsTransformer = source.newModifiedColumnSetTransformer(result, source
                .getColumnSourceMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
            this.mcsNewColumns = result.newModifiedColumnSet(inWindowColumnName);
            this.reusableModifiedColumnSet = new ModifiedColumnSet(this.mcsNewColumns);
        }

        @Override
        protected void process() {
            if (recorder.recordedVariablesAreValid()) {
                final ShiftAwareListener.Update upstream = recorder.getUpdate();

                // remove the removed indices from the priority queue
                removeIndex(upstream.removed);

                // anything that was shifted needs to be placed in the proper slots
                final Index preShiftIndex = source.getIndex().getPrevIndex();
                upstream.shifted.apply((start, end, delta) -> {
                    final Index subIndex = preShiftIndex.subindexByKey(start, end);

                    final Index.SearchIterator it =
                        delta < 0 ? subIndex.searchIterator() : subIndex.reverseIterator();
                    while (it.hasNext()) {
                        final long idx = it.nextLong();
                        final Entry entry = indexToEntry.remove(idx);
                        if (entry != null) {
                            entry.index = idx + delta;
                            indexToEntry.put(idx + delta, entry);
                        }
                    }
                });

                // TODO: improve performance with getChunk
                // TODO: reinterpret inWindowColumnSource so that it compares longs instead of
                // objects

                // figure out for all the modified indices if the timestamp or index changed
                upstream.forAllModified((oldIndex, newIndex) -> {
                    final DBDateTime currentTimestamp =
                        inWindowColumnSource.timeStampSource.get(newIndex);
                    final DBDateTime prevTimestamp =
                        inWindowColumnSource.timeStampSource.getPrev(oldIndex);
                    if (!Objects.equals(currentTimestamp, prevTimestamp)) {
                        updateIndex(newIndex, currentTimestamp);
                    }
                });

                // now add the new timestamps
                upstream.added.forAllLongs(this::addIndex);

                final ShiftAwareListener.Update downstream = upstream.copy();

                try (final Index modifiedByTime = recomputeModified()) {
                    if (modifiedByTime.nonempty()) {
                        downstream.modified.insert(modifiedByTime);
                    }
                }

                // everything that was added, removed, or modified stays added removed or modified
                downstream.modifiedColumnSet = reusableModifiedColumnSet;
                if (downstream.modified.nonempty()) {
                    mcsTransformer.clearAndTransform(upstream.modifiedColumnSet,
                        downstream.modifiedColumnSet);
                    downstream.modifiedColumnSet.setAll(mcsNewColumns);
                } else {
                    downstream.modifiedColumnSet.clear();
                }
                result.notifyListeners(downstream);
            } else {
                final Index modifiedByTime = recomputeModified();
                if (modifiedByTime.nonempty()) {
                    final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();
                    downstream.modified = modifiedByTime;
                    downstream.added = EMPTY_INDEX;
                    downstream.removed = EMPTY_INDEX;
                    downstream.shifted = IndexShiftData.EMPTY;
                    downstream.modifiedColumnSet = reusableModifiedColumnSet;
                    downstream.modifiedColumnSet.clear();
                    downstream.modifiedColumnSet.setAll(mcsNewColumns);
                    result.notifyListeners(downstream);
                } else {
                    modifiedByTime.close();
                }
            }
        }

        /**
         * Handles modified indices. If they are outside of the window, they need to be removed from
         * the queue. If they are inside the window, they need to be (re)inserted into the queue.
         */
        private void updateIndex(final long index, DBDateTime currentTimestamp) {
            Entry entry = indexToEntry.remove(index);
            if (currentTimestamp == null) {
                if (entry != null) {
                    priorityQueue.remove(entry);
                }
                return;
            }
            if (inWindowColumnSource.computeInWindow(currentTimestamp,
                inWindowColumnSource.currentTime)) {
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
         * @param index the index inserted into the table
         */
        private void addIndex(long index) {
            final DBDateTime currentTimestamp = inWindowColumnSource.timeStampSource.get(index);
            if (currentTimestamp == null) {
                return;
            }
            if (inWindowColumnSource.computeInWindow(currentTimestamp,
                inWindowColumnSource.currentTime)) {
                final Entry el = new Entry(index, currentTimestamp.getNanos());
                priorityQueue.enter(el);
                indexToEntry.put(el.index, el);
            }
        }

        /**
         * If the keys are in the window, remove them from the map and queue.
         *
         * @param index the indices to remove
         */
        private void removeIndex(final Index index) {
            index.forAllLongs((final long key) -> {
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
        public void refresh() {
            inWindowColumnSource.captureTime();
            notifyChanges();
        }

        private Index recomputeModified() {
            final Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();

            while (true) {
                final Entry entry = priorityQueue.top();
                if (entry == null) {
                    break;
                }

                if (inWindowColumnSource.computeInWindow(entry.nanos,
                    inWindowColumnSource.currentTime)) {
                    break;
                } else {
                    // take it out of the queue, and mark it as modified
                    final Entry taken = priorityQueue.removeTop();
                    Assert.equals(entry, "entry", taken, "taken");
                    builder.addKey(entry.index);
                    indexToEntry.remove(entry.index);
                }
            }

            return builder.getIndex();
        }

        void validateQueue() {
            final Index resultIndex = result.getIndex();
            final Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();

            final Entry[] entries = new Entry[priorityQueue.size()];
            priorityQueue.dump(entries, 0);
            Arrays.stream(entries).mapToLong(entry -> entry.index).forEach(builder::addKey);

            final Index inQueue = builder.getIndex();
            Assert.eq(inQueue.size(), "inQueue.size()", priorityQueue.size(),
                "priorityQueue.size()");
            final boolean condition = inQueue.subsetOf(resultIndex);
            if (!condition) {
                // noinspection ConstantConditions
                Assert.assertion(condition, "inQueue.subsetOf(resultIndex)", inQueue, "inQueue",
                    resultIndex, "resultIndex", inQueue.minus(resultIndex),
                    "inQueue.minus(resultIndex)");
            }
        }

        @Override
        public void destroy() {
            super.destroy();
            LiveTableMonitor.DEFAULT.removeTable(this);
        }
    }

    private static class InWindowColumnSourceWithTimeProvider extends InWindowColumnSource {
        final private TimeProvider timeProvider;

        InWindowColumnSourceWithTimeProvider(TimeProvider timeProvider, Table table,
            String timestampColumn, long windowNanos) {
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
        private final ColumnSource<DBDateTime> timeStampSource;

        private long prevTime = 0;
        private long currentTime = 0;
        private long clockStep = LogicalClock.DEFAULT.currentStep();
        private final long initialStep = clockStep;

        InWindowColumnSource(Table table, String timestampColumn, long windowNanos) {
            super(Boolean.class);
            this.windowNanos = windowNanos;

            // noinspection unchecked
            this.timeStampSource = table.getColumnSource(timestampColumn);
            if (!DBDateTime.class.isAssignableFrom(timeStampSource.getType())) {
                throw new IllegalArgumentException(timestampColumn + " is not of type DBDateTime!");
            }
        }

        /**
         * Initialize the first currentTime. Called outside the constructor, because subclasses may
         * overload getTimeNanos().
         */
        private void init() {
            currentTime = getTimeNanos();
        }

        long getTimeNanos() {
            return DBTimeUtils.currentTime().getNanos();
        }

        @Override
        public Boolean get(long index) {
            final DBDateTime tableTimeStamp = timeStampSource.get(index);
            return computeInWindow(tableTimeStamp, currentTime);
        }

        @Override
        public Boolean getPrev(long index) {
            final long currentStep = LogicalClock.DEFAULT.currentStep();

            final long time =
                (clockStep < currentStep || clockStep == initialStep) ? currentTime : prevTime;

            // get the previous value from the underlying column source
            final DBDateTime tableTimeStamp = timeStampSource.getPrev(index);
            return computeInWindow(tableTimeStamp, time);
        }

        private Boolean computeInWindow(DBDateTime tableTimeStamp, long time) {
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
    }
}
