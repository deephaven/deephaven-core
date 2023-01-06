/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.clock.Clock;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
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
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.util.TableTools;
import io.deephaven.function.Numeric;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.util.type.TypeUtils.box;

/**
 * A TimeTable adds rows at a fixed interval with a single column named "Timestamp".
 *
 * To create a TimeTable, you should use the {@link TableTools#timeTable} family of methods.
 */
public class TimeTable extends QueryTable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TimeTable.class);

    public static class Builder {
        private UpdateSourceRegistrar registrar = UpdateGraphProcessor.DEFAULT;
        private Clock clock;
        private DateTime startTime;
        private long period;
        private boolean streamTable;

        public Builder registrar(UpdateSourceRegistrar registrar) {
            this.registrar = registrar;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder startTime(DateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder startTime(String startTime) {
            this.startTime = DateTimeUtils.convertDateTime(startTime);
            return this;
        }

        public Builder period(long period) {
            this.period = period;
            return this;
        }

        public Builder period(String period) {
            this.period = DateTimeUtils.expressionToNanos(period);
            return this;
        }

        public Builder streamTable(boolean streamTable) {
            this.streamTable = streamTable;
            return this;
        }

        public QueryTable build() {
            return new TimeTable(registrar,
                    Objects.requireNonNullElse(clock, DateTimeUtils.currentClock()),
                    startTime, period, streamTable);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private static final String TIMESTAMP = "Timestamp";
    private long lastIndex = -1;
    private final SyntheticDateTimeSource columnSource;
    private final Clock clock;
    private final PerformanceEntry entry;
    private final boolean isStreamTable;

    public TimeTable(UpdateSourceRegistrar registrar, Clock clock,
            @Nullable DateTime startTime, long period, boolean isStreamTable) {
        super(RowSetFactory.empty().toTracking(), initColumn(startTime, period));
        this.isStreamTable = isStreamTable;
        final String name = isStreamTable ? "TimeTableStream" : "TimeTable";
        this.entry = UpdatePerformanceTracker.getInstance().getEntry(name + "(" + startTime + "," + period + ")");
        columnSource = (SyntheticDateTimeSource) getColumnSourceMap().get(TIMESTAMP);
        this.clock = clock;
        if (isStreamTable) {
            setAttribute(Table.STREAM_TABLE_ATTRIBUTE, Boolean.TRUE);
        } else {
            setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
            setFlat();
        }
        if (startTime != null) {
            refresh(false);
        }
        registrar.addSource(this);
    }

    private static Map<String, ColumnSource<?>> initColumn(DateTime firstTime, long period) {
        if (period <= 0) {
            throw new IllegalArgumentException("Invalid time period: " + period + " nanoseconds");
        }
        return Collections.singletonMap(TIMESTAMP, new SyntheticDateTimeSource(firstTime, period));
    }

    @Override
    public void run() {
        refresh(true);
    }

    private void refresh(final boolean notifyListeners) {
        entry.onUpdateStart();
        try {
            final DateTime dateTime = DateTime.of(clock);
            long rangeStart = lastIndex + 1;
            if (columnSource.startTime == null) {
                lastIndex = 0;
                columnSource.startTime = new DateTime(
                        Numeric.lowerBin(dateTime.getNanos(), columnSource.period));
            } else if (dateTime.compareTo(columnSource.startTime) >= 0) {
                lastIndex = Math.max(lastIndex,
                        DateTimeUtils.minus(dateTime, columnSource.startTime) / columnSource.period);
            }

            final boolean rowsAdded = rangeStart <= lastIndex;
            final boolean rowsRemoved = isStreamTable && getRowSet().isNonempty();
            if (rowsAdded || rowsRemoved) {
                final RowSet addedRange = rowsAdded
                        ? RowSetFactory.fromRange(rangeStart, lastIndex)
                        : RowSetFactory.empty();
                final RowSet removedRange = rowsRemoved
                        ? RowSetFactory.fromRange(getRowSet().firstRowKey(), rangeStart - 1)
                        : RowSetFactory.empty();
                if (rowsAdded) {
                    getRowSet().writableCast().insertRange(rangeStart, lastIndex);
                }
                if (rowsRemoved) {
                    getRowSet().writableCast().removeRange(0, rangeStart - 1);
                }
                if (notifyListeners) {
                    notifyListeners(addedRange, removedRange, RowSetFactory.empty());
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

    private static final class SyntheticDateTimeSource extends AbstractColumnSource<DateTime> implements
            ImmutableColumnSourceGetDefaults.LongBacked<DateTime>,
            FillUnordered<Values> {

        private DateTime startTime;
        private final long period;

        private SyntheticDateTimeSource(DateTime startTime, long period) {
            super(DateTime.class);
            this.startTime = startTime;
            this.period = period;
        }

        private DateTime computeDateTime(long rowKey) {
            return DateTimeUtils.plus(startTime, period * rowKey);
        }

        @Override
        public DateTime get(long rowKey) {
            return computeDateTime(rowKey);
        }

        private long computeNanos(long rowKey) {
            return startTime.getNanos() + period * rowKey;
        }

        @Override
        public long getLong(long rowKey) {
            return computeNanos(rowKey);
        }

        @Override
        public WritableRowSet match(boolean invertMatch, boolean usePrev, boolean caseInsensitive, RowSet selection,
                Object... keys) {
            if (startTime == null) {
                // there are no valid rows for this column source yet
                return RowSetFactory.empty();
            }

            final RowSetBuilderRandom matchingSet = RowSetFactory.builderRandom();

            for (Object o : keys) {
                if (!(o instanceof DateTime)) {
                    continue;
                }
                final DateTime key = (DateTime) o;

                if (key.getNanos() % period != startTime.getNanos() % period
                        || DateTimeUtils.isBefore(key, startTime)) {
                    continue;
                }

                matchingSet.addKey(DateTimeUtils.minus(key, startTime) / period);
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
                    ii -> result.put(computeDateTime(ii), RowSetFactory.fromKeys(ii)));
            return result;
        }

        @Override
        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
                @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return alternateDataType == long.class;
        }

        @Override
        public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
                @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) new SyntheticDateTimeAsLongSource();
        }

        @Override
        public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
                @NotNull LongChunk<? extends RowKeys> keys) {
            final WritableObjectChunk<DateTime, ? super Values> objectDest = dest.asWritableObjectChunk();
            objectDest.setSize(keys.size());

            for (int ii = 0; ii < keys.size(); ++ii) {
                objectDest.set(ii, computeDateTime(keys.get(ii)));
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

        private class SyntheticDateTimeAsLongSource extends AbstractColumnSource<Long> implements
                ImmutableColumnSourceGetDefaults.LongBacked<Long>,
                FillUnordered<Values> {

            SyntheticDateTimeAsLongSource() {
                super(Long.class);
            }

            @Override
            public Long get(long rowKey) {
                return box(computeNanos(rowKey));
            }

            @Override
            public long getLong(long rowKey) {
                return computeNanos(rowKey);
            }

            @Override
            public WritableRowSet match(boolean invertMatch, boolean usePrev, boolean caseInsensitive, RowSet selection,
                    Object... keys) {
                if (startTime == null) {
                    // there are no valid rows for this column source yet
                    return RowSetFactory.empty();
                }

                final RowSetBuilderRandom matchingSet = RowSetFactory.builderRandom();

                for (Object o : keys) {
                    if (!(o instanceof Long)) {
                        continue;
                    }
                    final long key = (Long) o;

                    if (key % period != startTime.getNanos() % period || key < startTime.getNanos()) {
                        continue;
                    }

                    matchingSet.addKey((key - startTime.getNanos()) / period);
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
                        ii -> result.put(box(computeNanos(ii)), RowSetFactory.fromKeys(ii)));
                return result;
            }

            @Override
            public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
                    @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
                return alternateDataType == DateTime.class;
            }

            @Override
            public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
                    @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
                // noinspection unchecked
                return (ColumnSource<ALTERNATE_DATA_TYPE>) SyntheticDateTimeSource.this;
            }

            @Override
            public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
                    @NotNull LongChunk<? extends RowKeys> keys) {
                final WritableLongChunk<? super Values> longDest = dest.asWritableLongChunk();
                longDest.setSize(keys.size());

                for (int ii = 0; ii < keys.size(); ++ii) {
                    longDest.set(ii, computeNanos(keys.get(ii)));
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
