/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.function.LongNumericPrimitives;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.TimeProvider;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.util.type.TypeUtils.box;

/**
 * A TimeTable adds rows at a fixed interval with a single column named "Timestamp".
 *
 * To create a TimeTable, you should use the {@link TableTools#timeTable} family of methods.
 */
public class TimeTable extends QueryTable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TimeTable.class);

    private static final String TIMESTAMP = "Timestamp";
    private long lastIndex = -1;
    private final SyntheticDateTimeSource columnSource;
    private final TimeProvider timeProvider;
    private final long binOffset;
    private final PerformanceEntry entry;
    private final boolean isStreaming;

    public TimeTable(TimeProvider timeProvider, long period) {
        this(timeProvider, null, period, false);
    }

    public TimeTable(TimeProvider timeProvider, long period, boolean isStreaming) {
        this(timeProvider, null, period, isStreaming);
    }

    public TimeTable(TimeProvider timeProvider, DateTime firstTime, long period) {
        this(timeProvider, firstTime, period, false);
    }

    public TimeTable(TimeProvider timeProvider, DateTime firstTime, long period, boolean isStreaming) {
        super(RowSetFactory.fromKeys().toTracking(), initColumn(period));
        if (period <= 0) {
            throw new IllegalArgumentException("Invalid time period: " + period + " nanoseconds");
        }
        this.isStreaming = isStreaming;
        final String name = isStreaming ? "TimeTableStream" : "TimeTable";
        this.entry = UpdatePerformanceTracker.getInstance().getEntry(name + "(" + firstTime + "," + period + ")");
        columnSource = (SyntheticDateTimeSource) getColumnSourceMap().get(TIMESTAMP);
        columnSource.baseTime = firstTime == null ? null : new DateTime(firstTime.getNanos() - period);
        binOffset = firstTime == null ? 0 : columnSource.baseTime.getNanos() % period;
        this.timeProvider = timeProvider;
        if (isStreaming) {
            setAttribute(Table.STREAM_TABLE_ATTRIBUTE, Boolean.TRUE);
        } else {
            setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
            setFlat();
        }
        if (firstTime != null) {
            refresh(false);
        }
    }

    private static Map<String, ColumnSource<?>> initColumn(long period) {
        return Collections.singletonMap(TIMESTAMP, new SyntheticDateTimeSource(period));
    }

    @Override
    public void run() {
        refresh(true);
    }

    private void refresh(final boolean notifyListeners) {
        entry.onUpdateStart();
        try {
            final DateTime dateTime = timeProvider.currentTime();
            DateTime currentBinnedTime = new DateTime(
                    LongNumericPrimitives.lowerBin(dateTime.getNanos() - binOffset, columnSource.period) + binOffset);
            long rangeStart = lastIndex + 1;
            if (columnSource.baseTime == null) {
                lastIndex = 0;
                columnSource.baseTime = currentBinnedTime;
                getRowSet().writableCast().insert(lastIndex);
            } else {
                lastIndex = DateTimeUtils.minus(currentBinnedTime, columnSource.baseTime) / columnSource.period;
            }

            if (rangeStart <= lastIndex) {
                // If we have a period longer than 10s, print out that the timetable has been updated. This can be
                // useful when analyzing what's gone wrong in the logs. It is capped at periods of 5s, so we don't
                // end up with too much log spam for short interval time tables. 5s is not so coincidentally the period
                // of the Jvm Heap: messages.
                if (columnSource.period >= 5_000_000_000L) {
                    log.info().append("TimeTable updated to ").append(columnSource.get(lastIndex).toString()).endl();
                }
                final RowSet range = RowSetFactory.fromRange(rangeStart, lastIndex);
                final RowSet removedRange = isStreaming ? getRowSet().copy() : RowSetFactory.empty();
                getRowSet().writableCast().update(range, removedRange);
                if (notifyListeners) {
                    notifyListeners(range, removedRange, RowSetFactory.empty());
                }
            }
        } finally {
            entry.onUpdateEnd();
        }
    }

    @Override
    protected void destroy() {
        super.destroy();
        UpdateGraphProcessor.DEFAULT.removeSource(this);
    }

    private static class SyntheticDateTimeSource implements
            ColumnSource<DateTime>,
            DefaultChunkSource.WithPrev<Values>,
            MutableColumnSourceGetDefaults.LongBacked<DateTime>,
            FillUnordered,
            InMemoryColumnSource {

        private DateTime baseTime;
        private final long period;

        private SyntheticDateTimeSource(long period) {
            this.period = period;
        }

        @Override
        public DateTime get(long index) {
            return DateTimeUtils.plus(baseTime, period * index);
        }

        @Override
        public long getLong(long index) {
            return baseTime.getNanos() + period * index;
        }

        @Override
        public DateTime getPrev(long index) {
            return get(index);
        }

        @Override
        public long getPrevLong(long index) {
            return getLong(index);
        }

        @Override
        public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
                @NotNull RowSequence rowSequence) {
            final WritableObjectChunk<DateTime, ? super Values> objectDest = destination.asWritableObjectChunk();
            objectDest.setSize(0);
            rowSequence.forAllRowKeys(ii -> objectDest.add(DateTimeUtils.plus(baseTime, period * ii)));
        }

        @Override
        public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
                @NotNull RowSequence rowSequence) {
            fillChunk(context, destination, rowSequence);
        }

        @Override
        public Class<DateTime> getType() {
            return DateTime.class;
        }

        @Override
        public Class<?> getComponentType() {
            return null;
        }

        @Override
        public WritableRowSet match(boolean invertMatch, boolean usePrev, boolean caseInsensitive, RowSet selection,
                Object... keys) {
            if (baseTime == null) {
                // there are no valid rows for this column source yet
                return RowSetFactory.empty();
            }

            final RowSetBuilderRandom matchingSet = RowSetFactory.builderRandom();

            for (Object o : keys) {
                if (!(o instanceof DateTime)) {
                    continue;
                }
                final DateTime key = (DateTime) o;

                if (key.getNanos() % period != baseTime.getNanos() % period || DateTimeUtils.isBefore(key, baseTime)) {
                    continue;
                }

                matchingSet.addKey(DateTimeUtils.minus(key, baseTime) / period);
            }

            if (invertMatch) {
                try (final WritableRowSet matching = matchingSet.build()) {
                    return selection.minus(matching);
                }
            }

            final WritableRowSet matching = matchingSet.build();
            matching.retain(selection);
            return matching;
        }

        @Override
        public Map<DateTime, RowSet> getValuesMapping(RowSet subRange) {
            final Map<DateTime, RowSet> result = new LinkedHashMap<>();
            subRange.forAllRowKeys(
                    ii -> result.put(DateTimeUtils.plus(baseTime, period * ii), RowSetFactory.fromKeys(ii)));
            return result;
        }

        @Override
        public Map<DateTime, RowSet> getGroupToRange() {
            return null;
        }

        @Override
        public Map<DateTime, RowSet> getGroupToRange(RowSet rowSet) {
            return null;
        }

        @Override
        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
                @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return alternateDataType == long.class;
        }

        @Override
        public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> reinterpret(
                @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            if (!allowsReinterpret(alternateDataType)) {
                throw new IllegalArgumentException("Unsupported reinterpret for " + getClass().getSimpleName()
                        + ": type=" + getType()
                        + ", alternateDataType=" + alternateDataType);
            }
            // noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) new SyntheticDateTimeAsLongSource();
        }

        @Override
        public ColumnSource<DateTime> getPrevSource() {
            return this;
        }

        @Override
        public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
                @NotNull LongChunk<? extends RowKeys> keys) {
            final WritableObjectChunk<DateTime, ? super Values> objectDest = dest.asWritableObjectChunk();
            objectDest.setSize(keys.size());

            for (int ii = 0; ii < keys.size(); ++ii) {
                objectDest.set(ii, DateTimeUtils.plus(baseTime, period * keys.get(ii)));
            }
        }

        @Override
        public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
                @NotNull LongChunk<? extends RowKeys> keys) {
            fillChunkUnordered(context, dest, keys);
        }

        @Override
        public boolean providesFillUnordered() {
            return true;
        }

        private class SyntheticDateTimeAsLongSource implements
                ColumnSource<Long>,
                DefaultChunkSource.WithPrev<Values>,
                MutableColumnSourceGetDefaults.LongBacked<Long>,
                FillUnordered,
                InMemoryColumnSource {

            @Override
            public Long get(long index) {
                return box(getLong(index));
            }

            @Override
            public long getLong(long index) {
                return baseTime.getNanos() + period * index;
            }

            @Override
            public Long getPrev(long index) {
                return get(index);
            }

            @Override
            public long getPrevLong(long index) {
                return getLong(index);
            }

            @Override
            public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
                    @NotNull RowSequence rowSequence) {
                final WritableLongChunk<? super Values> longDest = destination.asWritableLongChunk();
                longDest.setSize(0);
                rowSequence.forAllRowKeys(ii -> longDest.add(baseTime.getNanos() + period * ii));
            }

            @Override
            public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
                    @NotNull RowSequence rowSequence) {
                fillChunk(context, destination, rowSequence);
            }

            @Override
            public Class<Long> getType() {
                return long.class;
            }

            @Override
            public Class<?> getComponentType() {
                return null;
            }

            @Override
            public WritableRowSet match(boolean invertMatch, boolean usePrev, boolean caseInsensitive, RowSet selection,
                    Object... keys) {
                if (baseTime == null) {
                    // there are no valid rows for this column source yet
                    return RowSetFactory.empty();
                }

                final RowSetBuilderRandom matchingSet = RowSetFactory.builderRandom();

                for (Object o : keys) {
                    if (!(o instanceof Long)) {
                        continue;
                    }
                    final long key = (Long) o;

                    if (key % period != baseTime.getNanos() % period || key < baseTime.getNanos()) {
                        continue;
                    }

                    matchingSet.addKey((key - baseTime.getNanos()) / period);
                }

                if (invertMatch) {
                    try (final WritableRowSet matching = matchingSet.build()) {
                        return selection.minus(matching);
                    }
                }

                final WritableRowSet matching = matchingSet.build();
                matching.retain(selection);
                return matching;
            }

            @Override
            public Map<Long, RowSet> getValuesMapping(RowSet subRange) {
                final Map<Long, RowSet> result = new LinkedHashMap<>();
                subRange.forAllRowKeys(
                        ii -> result.put(box(baseTime.getNanos() + period * ii), RowSetFactory.fromKeys(ii)));
                return result;
            }

            @Override
            public Map<Long, RowSet> getGroupToRange() {
                return null;
            }

            @Override
            public Map<Long, RowSet> getGroupToRange(RowSet rowSet) {
                return null;
            }

            @Override
            public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
                    @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
                return alternateDataType == DateTime.class;
            }

            @Override
            public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> reinterpret(
                    @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
                if (!allowsReinterpret(alternateDataType)) {
                    throw new IllegalArgumentException("Unsupported reinterpret for " + getClass().getSimpleName()
                            + ": type=" + getType()
                            + ", alternateDataType=" + alternateDataType);
                }
                // noinspection unchecked
                return (ColumnSource<ALTERNATE_DATA_TYPE>) SyntheticDateTimeSource.this;
            }

            @Override
            public ColumnSource<Long> getPrevSource() {
                return this;
            }

            @Override
            public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
                    @NotNull LongChunk<? extends RowKeys> keys) {
                final WritableLongChunk<? super Values> longDest = dest.asWritableLongChunk();
                longDest.setSize(keys.size());

                for (int ii = 0; ii < keys.size(); ++ii) {
                    longDest.set(ii, baseTime.getNanos() + period * keys.get(ii));
                }
            }

            @Override
            public void fillPrevChunkUnordered(@NotNull FillContext context,
                    @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
                fillChunkUnordered(context, dest, keys);
            }

            @Override
            public boolean providesFillUnordered() {
                return true;
            }
        }
    }
}
