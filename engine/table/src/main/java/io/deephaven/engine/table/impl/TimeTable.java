//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.clock.Clock;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.util.TableTools;
import io.deephaven.function.Numeric;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.time.DateTimeUtils.currentClock;
import static io.deephaven.time.DateTimeUtils.epochNanos;
import static io.deephaven.time.DateTimeUtils.epochNanosToInstant;
import static io.deephaven.time.DateTimeUtils.isBefore;
import static io.deephaven.time.DateTimeUtils.minus;
import static io.deephaven.time.DateTimeUtils.parseInstant;
import static io.deephaven.time.DateTimeUtils.parseDurationNanos;
import static io.deephaven.time.DateTimeUtils.plus;
import static io.deephaven.util.type.TypeUtils.box;

/**
 * A TimeTable adds rows at a fixed interval with a single column named "Timestamp".
 * <p>
 * To create a TimeTable, you should use the {@link TableTools#timeTable} family of methods.
 *
 * @implNote The constructor publishes {@code this} to the {@link UpdateSourceRegistrar} and thus cannot be subclassed.
 */
public final class TimeTable extends QueryTable implements Runnable {

    public static class Builder {
        private UpdateSourceRegistrar registrar = ExecutionContext.getContext().getUpdateGraph();

        private Clock clock;
        private Instant startTime;
        private long period;
        private boolean blinkTable;

        public Builder registrar(UpdateSourceRegistrar registrar) {
            this.registrar = registrar;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder startTime(String startTime) {
            this.startTime = parseInstant(startTime);
            return this;
        }

        public Builder period(Duration period) {
            return period(period.toNanos());
        }

        public Builder period(long period) {
            this.period = period;
            return this;
        }

        public Builder period(String period) {
            return period(parseDurationNanos(period));
        }

        public Builder blinkTable(boolean blinkTable) {
            this.blinkTable = blinkTable;
            return this;
        }

        public QueryTable build() {
            try (final SafeCloseable ignored =
                    ExecutionContext.getContext().withUpdateGraph(registrar.getUpdateGraph()).open()) {
                return new TimeTable(registrar,
                        Objects.requireNonNullElse(clock, currentClock()),
                        startTime, period, blinkTable);
            }
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private static final String TIMESTAMP = "Timestamp";
    private long lastIndex = -1;
    private final SyntheticInstantSource columnSource;
    private final Clock clock;
    private final String name;
    private final boolean isBlinkTable;
    private final UpdateSourceRegistrar registrar;
    private final SourceRefresher refresher;

    public TimeTable(
            UpdateSourceRegistrar registrar,
            Clock clock,
            @Nullable Instant startTime,
            long period,
            boolean isBlinkTable) {
        super(RowSetFactory.empty().toTracking(), initColumn(startTime, period));
        this.registrar = registrar;
        this.isBlinkTable = isBlinkTable;
        this.name = (isBlinkTable ? "TimeTableBlink" : "TimeTable") + "(" + startTime + "," + period + ")";

        columnSource = (SyntheticInstantSource) getColumnSourceMap().get(TIMESTAMP);
        this.clock = clock;
        if (isBlinkTable) {
            setAttribute(Table.BLINK_TABLE_ATTRIBUTE, Boolean.TRUE);
        } else {
            setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
            setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
            setFlat();
        }
        refresher = new SourceRefresher();
        if (startTime != null) {
            refresh(false);
        }
        setRefreshing(true);
        initializeLastNotificationStep(registrar.getUpdateGraph().clock());
        registrar.addSource(refresher);
    }

    @Override
    public boolean satisfied(final long step) {
        return registrar.satisfied(step);
    }

    private static Map<String, ColumnSource<?>> initColumn(Instant firstTime, long period) {
        if (period <= 0) {
            throw new IllegalArgumentException("Invalid time period: " + period + " nanoseconds");
        }
        return Collections.singletonMap(TIMESTAMP, new SyntheticInstantSource(firstTime, period));
    }

    @Override
    @TestUseOnly
    public void run() {
        refresh(true);
    }

    private class SourceRefresher extends InstrumentedTableUpdateSource {

        public SourceRefresher() {
            super(registrar, TimeTable.this, name);
        }

        @Override
        protected void instrumentedRefresh() {
            refresh(true);
        }
    }

    private void refresh(final boolean notifyListeners) {
        final Instant now = clock.instantNanos();
        long rangeStart = lastIndex + 1;
        if (columnSource.startTime == null) {
            lastIndex = 0;
            columnSource.startTime = epochNanosToInstant(
                    Numeric.lowerBin(epochNanos(now), columnSource.period));
        } else if (now.compareTo(columnSource.startTime) >= 0) {
            lastIndex = Math.max(lastIndex,
                    minus(now, columnSource.startTime) / columnSource.period);
        }

        final boolean rowsAdded = rangeStart <= lastIndex;
        final boolean rowsRemoved = isBlinkTable && getRowSet().isNonempty();
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
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    protected void destroy() {
        super.destroy();
        registrar.removeSource(refresher);
    }

    private static final class SyntheticInstantSource extends AbstractColumnSource<Instant> implements
            ImmutableColumnSourceGetDefaults.LongBacked<Instant>,
            FillUnordered<Values> {

        private Instant startTime;
        private final long period;

        private SyntheticInstantSource(Instant startTime, long period) {
            super(Instant.class);
            this.startTime = startTime;
            this.period = period;
        }

        private Instant computeInstant(long rowKey) {
            return plus(startTime, period * rowKey);
        }

        @Override
        public Instant get(long rowKey) {
            if (rowKey < 0) {
                return null;
            }
            return computeInstant(rowKey);
        }

        private long computeNanos(long rowKey) {
            return epochNanos(startTime) + period * rowKey;
        }

        @Override
        public long getLong(long rowKey) {
            if (rowKey < 0) {
                return QueryConstants.NULL_LONG;
            }
            return computeNanos(rowKey);
        }

        @Override
        public WritableRowSet match(
                final boolean invertMatch,
                final boolean usePrev,
                final boolean caseInsensitive,
                @Nullable final DataIndex dataIndex,
                @NotNull final RowSet selection,
                final Object... keys) {
            if (startTime == null) {
                // there are no valid rows for this column source yet
                return RowSetFactory.empty();
            }

            final RowSetBuilderRandom matchingSet = RowSetFactory.builderRandom();

            for (Object o : keys) {
                if (!(o instanceof Instant)) {
                    continue;
                }
                final Instant key = (Instant) o;

                if (epochNanos(key) % period != epochNanos(startTime) % period || isBefore(key, startTime)) {
                    continue;
                }

                matchingSet.addKey(minus(key, startTime) / period);
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
        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
                @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return alternateDataType == long.class;
        }

        @Override
        public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
                @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) new SyntheticInstantAsLongSource();
        }

        @Override
        public void fillChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> dest,
                @NotNull final RowSequence rowSequence) {
            final WritableObjectChunk<Instant, ? super Values> objectDest = dest.asWritableObjectChunk();
            dest.setSize(0);
            rowSequence.forAllRowKeys(rowKey -> objectDest.add(computeInstant(rowKey)));
        }

        @Override
        public void fillPrevChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> dest,
                @NotNull final RowSequence rowSequence) {
            fillChunk(context, dest, rowSequence);
        }

        @Override
        public void fillChunkUnordered(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> dest,
                @NotNull final LongChunk<? extends RowKeys> keys) {
            final WritableObjectChunk<Instant, ? super Values> objectDest = dest.asWritableObjectChunk();
            objectDest.setSize(keys.size());

            for (int ii = 0; ii < keys.size(); ++ii) {
                long rowKey = keys.get(ii);
                if (rowKey < 0) {
                    objectDest.set(ii, null);
                } else {
                    objectDest.set(ii, computeInstant(rowKey));
                }
            }
        }

        @Override
        public void fillPrevChunkUnordered(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> dest,
                @NotNull final LongChunk<? extends RowKeys> keys) {
            fillChunkUnordered(context, dest, keys);
        }

        @Override
        public boolean providesFillUnordered() {
            return true;
        }

        private class SyntheticInstantAsLongSource extends AbstractColumnSource<Long> implements
                ImmutableColumnSourceGetDefaults.LongBacked<Long>,
                FillUnordered<Values> {

            SyntheticInstantAsLongSource() {
                super(long.class);
            }

            @Override
            public Long get(long rowKey) {
                if (rowKey < 0) {
                    return null;
                }
                return box(computeNanos(rowKey));
            }

            @Override
            public long getLong(long rowKey) {
                if (rowKey < 0) {
                    return QueryConstants.NULL_LONG;
                }
                return computeNanos(rowKey);
            }

            @Override
            public void fillChunk(
                    @NotNull final FillContext context,
                    @NotNull final WritableChunk<? super Values> dest,
                    @NotNull final RowSequence rowSequence) {
                WritableLongChunk<? super Values> longDest = dest.asWritableLongChunk();
                dest.setSize(0);
                rowSequence.forAllRowKeys(rowKey -> longDest.add(computeNanos(rowKey)));
            }

            @Override
            public void fillPrevChunk(
                    @NotNull final FillContext context,
                    @NotNull final WritableChunk<? super Values> dest,
                    @NotNull final RowSequence rowSequence) {
                fillChunk(context, dest, rowSequence);
            }

            @Override
            public WritableRowSet match(
                    final boolean invertMatch,
                    final boolean usePrev,
                    final boolean caseInsensitive,
                    @Nullable final DataIndex dataIndex,
                    @NotNull final RowSet selection,
                    final Object... keys) {
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

                    if (key % period != epochNanos(startTime) % period || key < epochNanos(startTime)) {
                        continue;
                    }

                    matchingSet.addKey((key - epochNanos(startTime)) / period);
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
            public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
                    @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
                return alternateDataType == Instant.class;
            }

            @Override
            public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
                    @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
                // noinspection unchecked
                return (ColumnSource<ALTERNATE_DATA_TYPE>) SyntheticInstantSource.this;
            }

            @Override
            public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
                    @NotNull LongChunk<? extends RowKeys> keys) {
                final WritableLongChunk<? super Values> longDest = dest.asWritableLongChunk();
                longDest.setSize(keys.size());

                for (int ii = 0; ii < keys.size(); ++ii) {
                    long rowKey = keys.get(ii);
                    if (rowKey < 0) {
                        longDest.set(ii, QueryConstants.NULL_LONG);
                    } else {
                        longDest.set(ii, computeNanos(rowKey));
                    }
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
