//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.select.AbstractRangeFilter;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.RangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegating;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.util.TableTools;
import io.deephaven.function.Numeric;
import io.deephaven.time.DateTimeUtils;
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
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import static io.deephaven.time.DateTimeUtils.currentClock;
import static io.deephaven.time.DateTimeUtils.epochNanos;
import static io.deephaven.time.DateTimeUtils.epochNanosToInstant;
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
public final class TimeTable extends QueryTable implements Runnable, HasRefreshingSource {

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

    @Override
    public String getDescription() {
        return name;
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
            final long firstRemovedRowKey = rowsRemoved ? getRowSet().firstRowKey() : RowSequence.NULL_ROW_KEY;
            if (rowsAdded) {
                getRowSet().writableCast().insertRange(rangeStart, lastIndex);
            }
            if (rowsRemoved) {
                getRowSet().writableCast().removeRange(0, rangeStart - 1);
            }
            if (notifyListeners) {
                notifyListeners(
                        rowsAdded
                                ? RowSetFactory.fromRange(rangeStart, lastIndex)
                                : RowSetFactory.empty(),
                        rowsRemoved
                                ? RowSetFactory.fromRange(firstRemovedRowKey, rangeStart - 1)
                                : RowSetFactory.empty(),
                        RowSetFactory.empty());
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
        public void estimatePushdownFilterCost(
                final WhereFilter filter,
                final RowSet selection,
                final boolean usePrev,
                final PushdownFilterContext context,
                final JobScheduler jobScheduler,
                final LongConsumer onComplete,
                final Consumer<Exception> onError) {
            // A range on either timestamp column is a LongRangeFilter on epoch nanoseconds: an InstantRangeFilter on
            // the Instant column, and a plain LongRangeFilter on the long reinterpretation. Computing a row key costs
            // about as much as testing one row, so a match with more values than the selection has rows is scanned.
            final WhereFilter unwrapped = WhereFilterDelegating.maybeUnwrapFilter(filter);
            final MatchFilter matchFilter = MatchFilter.extractMatchFilter(unwrapped).orElse(null);
            final boolean supported = matchFilter != null
                    ? matchFilter.getValues().length <= selection.size()
                    : RangeFilter.extractRangeFilter(unwrapped).orElse(null) instanceof LongRangeFilter;
            final long cost = supported ? PushdownResult.TABLE_KEY_LOOKUP_COST : PushdownResult.UNSUPPORTED_ACTION_COST;
            onComplete.accept(cost);
        }

        /**
         * Answer a match or range filter from the start time and period, without reading any row. The row at key
         * {@code k} holds the timestamp {@code startTime + period * k}, which rises with {@code k} and is never null.
         */
        @Override
        public void pushdownFilter(
                final WhereFilter filter,
                final RowSet selection,
                final boolean usePrev,
                final PushdownFilterContext context,
                final long costCeiling,
                final JobScheduler jobScheduler,
                final Consumer<PushdownResult> onComplete,
                final Consumer<Exception> onError) {
            if (costCeiling < PushdownResult.TABLE_KEY_LOOKUP_COST) {
                onComplete.accept(PushdownResult.allMaybeMatch(selection));
                return;
            }
            final WhereFilter unwrapped = WhereFilterDelegating.maybeUnwrapFilter(filter);
            final MatchFilter matchFilter = MatchFilter.extractMatchFilter(unwrapped).orElse(null);
            if (matchFilter != null) {
                onComplete.accept(pushdownMatchFilter(matchFilter, selection));
                return;
            }
            final AbstractRangeFilter rangeFilter = RangeFilter.extractRangeFilter(unwrapped).orElse(null);
            if (rangeFilter instanceof LongRangeFilter) {
                onComplete.accept(pushdownRangeFilter((LongRangeFilter) rangeFilter, selection));
                return;
            }
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
        }

        /**
         * The rows of {@code selection} that {@code matchFilter} selects, from the row key each value computes to. A
         * value matches at most one row. A value that is neither an {@link Instant} nor a {@link Long}, null included,
         * matches no row.
         */
        @NotNull
        private PushdownResult pushdownMatchFilter(
                @NotNull final MatchFilter matchFilter,
                @NotNull final RowSet selection) {
            final boolean inverted = matchFilter.getMatchOptions().inverted();
            final Instant localStartTime = startTime;
            if (localStartTime == null) {
                // the table has no rows until the first refresh sets the start time
                return inverted ? PushdownResult.allMatch(selection) : PushdownResult.noneMatch(selection);
            }
            final long startNanos = epochNanos(localStartTime);
            final RowSetBuilderRandom matchingKeys = RowSetFactory.builderRandom();
            for (final Object value : matchFilter.getValues()) {
                final long valueNanos;
                if (value instanceof Instant) {
                    final Instant instant = (Instant) value;
                    if (instant.isBefore(localStartTime)) {
                        // Before the first row. Checked before the conversion, since epochNanos checks only the upper
                        // bound and wraps an Instant far before the epoch around to an unrelated value.
                        continue;
                    }
                    try {
                        valueNanos = epochNanos(instant);
                    } catch (final DateTimeUtils.DateTimeOverflowException e) {
                        // outside the range of epoch nanoseconds, where every timestamp of the table lies
                        continue;
                    }
                } else if (value instanceof Long) {
                    valueNanos = (Long) value;
                } else {
                    continue;
                }
                if (valueNanos < startNanos) {
                    continue;
                }
                final long offsetNanos;
                try {
                    offsetNanos = Math.subtractExact(valueNanos, startNanos);
                } catch (final ArithmeticException e) {
                    // no row key reaches a timestamp this far after the start time
                    continue;
                }
                if (offsetNanos % period == 0) {
                    matchingKeys.addKey(offsetNanos / period);
                }
            }
            try (final WritableRowSet keys = matchingKeys.build();
                    final WritableRowSet matching = inverted ? selection.minus(keys) : keys.intersect(selection)) {
                return PushdownResult.exactMatch(selection, matching);
            }
        }

        /**
         * The rows of {@code selection} that {@code rangeFilter} selects. The timestamps rise with the row key, so the
         * rows in the range are one run of keys, from the first key at or above the lowest matching timestamp to the
         * last key at or below the highest.
         */
        @NotNull
        private PushdownResult pushdownRangeFilter(
                @NotNull final LongRangeFilter rangeFilter,
                @NotNull final RowSet selection) {
            final Instant localStartTime = startTime;
            if (localStartTime == null) {
                // the table has no rows until the first refresh sets the start time
                return PushdownResult.noneMatch(selection);
            }
            final long lower = rangeFilter.getLower();
            final long upper = rangeFilter.getUpper();
            if ((!rangeFilter.isLowerInclusive() && lower == Long.MAX_VALUE)
                    || (!rangeFilter.isUpperInclusive() && upper == Long.MIN_VALUE)) {
                return PushdownResult.noneMatch(selection);
            }
            final long lowestNanos = rangeFilter.isLowerInclusive() ? lower : lower + 1;
            final long highestNanos = rangeFilter.isUpperInclusive() ? upper : upper - 1;
            final long startNanos = epochNanos(localStartTime);
            if (highestNanos < startNanos || lowestNanos > highestNanos) {
                return PushdownResult.noneMatch(selection);
            }
            // Each offset below is positive, so a negative difference means the subtraction overflowed: the bound lies
            // further after the start time than any row key reaches.
            final long firstKey;
            if (lowestNanos <= startNanos) {
                firstKey = 0;
            } else {
                final long lowestOffset = lowestNanos - startNanos;
                if (lowestOffset < 0) {
                    return PushdownResult.noneMatch(selection);
                }
                firstKey = lowestOffset / period + (lowestOffset % period == 0 ? 0 : 1);
            }
            final long highestOffset = highestNanos - startNanos;
            final long lastKey = highestOffset < 0 ? Long.MAX_VALUE : highestOffset / period;
            if (firstKey > lastKey) {
                return PushdownResult.noneMatch(selection);
            }
            try (final WritableRowSet matching = selection.subSetByKeyRange(firstKey, lastKey)) {
                return PushdownResult.exactMatch(selection, matching);
            }
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
            public void estimatePushdownFilterCost(
                    final WhereFilter filter,
                    final RowSet selection,
                    final boolean usePrev,
                    final PushdownFilterContext context,
                    final JobScheduler jobScheduler,
                    final LongConsumer onComplete,
                    final Consumer<Exception> onError) {
                // the bounds and values of a filter on either timestamp column are epoch nanoseconds
                SyntheticInstantSource.this.estimatePushdownFilterCost(
                        filter, selection, usePrev, context, jobScheduler, onComplete, onError);
            }

            @Override
            public void pushdownFilter(
                    final WhereFilter filter,
                    final RowSet selection,
                    final boolean usePrev,
                    final PushdownFilterContext context,
                    final long costCeiling,
                    final JobScheduler jobScheduler,
                    final Consumer<PushdownResult> onComplete,
                    final Consumer<Exception> onError) {
                SyntheticInstantSource.this.pushdownFilter(
                        filter, selection, usePrev, context, costCeiling, jobScheduler, onComplete, onError);
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

    @NotNull
    @Override
    public Stream<PerformanceEntry> sourceEntries() {
        if (refresher.getEntry() != null) {
            return Stream.of(refresher.getEntry());
        }
        return Stream.empty();
    }
}
