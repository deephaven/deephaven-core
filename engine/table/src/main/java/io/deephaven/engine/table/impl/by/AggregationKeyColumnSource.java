//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegating;
import io.deephaven.engine.table.impl.sources.ConvertibleTimeSource;
import io.deephaven.engine.table.impl.sources.DelegatingColumnSource;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * The key column of an aggregation with exactly one group-by column. Reads are delegated to the aggregation's key
 * source; a {@link MatchFilter} on this column is pushed down by probing the aggregation's hash table once per match
 * value, at a cost independent of the result's size.
 *
 * <p>
 * The aggregation never moves or removes a key's state, so a key's row key is fixed once assigned, and a probe answers
 * for previous values as well as current ones. A refreshing aggregation changes its hash table only in its update
 * cycle, which it {@link IncrementalOperatorAggregationStateManager#updateCycleStep() marks} before the first change. A
 * probe on another thread reads the clock's step first, and is abandoned, so that the filter scans the column, when a
 * cycle at that step or later began, filtering previous values, or a later cycle began, filtering current ones.
 *
 * <p>
 * The key source's time conversions are delegated as well, so that a key column reinterprets as the column it was
 * grouped from does. A {@link ZonedDateTime} key column is not wrapped, since the hash table does not hold its zone.
 */
public final class AggregationKeyColumnSource<T> extends DelegatingColumnSource<T, T>
        implements ConvertibleTimeSource {

    private final ColumnSource<T> keySource;
    private final OperatorAggregationStateManager stateManager;
    private final boolean reinterpretedKeys;
    /** The state manager of a refreshing aggregation, or null for a static one, which never changes. */
    private final IncrementalOperatorAggregationStateManager incrementalStateManager;
    private final UpdateGraph updateGraph;

    /**
     * Create the key column for {@code keySource}.
     *
     * @param keySource the result's key source, of the group-by column's type, which is not {@link ZonedDateTime}
     * @param stateManager the aggregation's state manager, whose output positions are the result's row keys
     * @param reinterpretedKeys whether {@code stateManager} hashes the keys reinterpreted as primitives, per
     *        {@link io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive}
     * @param updateGraph the aggregation's update graph
     * @return the key column
     */
    static <T> AggregationKeyColumnSource<T> create(
            @NotNull final ColumnSource<T> keySource,
            @NotNull final OperatorAggregationStateManager stateManager,
            final boolean reinterpretedKeys,
            @NotNull final UpdateGraph updateGraph) {
        return new AggregationKeyColumnSource<>(keySource, stateManager, reinterpretedKeys, updateGraph);
    }

    private AggregationKeyColumnSource(
            @NotNull final ColumnSource<T> keySource,
            @NotNull final OperatorAggregationStateManager stateManager,
            final boolean reinterpretedKeys,
            @NotNull final UpdateGraph updateGraph) {
        super(keySource.getType(), keySource.getComponentType(), keySource);
        this.keySource = keySource;
        this.stateManager = stateManager;
        this.reinterpretedKeys = reinterpretedKeys;
        incrementalStateManager = stateManager instanceof IncrementalOperatorAggregationStateManager
                ? (IncrementalOperatorAggregationStateManager) stateManager
                : null;
        this.updateGraph = updateGraph;
    }

    @Override
    public boolean supportsTimeConversion() {
        return keySource instanceof ConvertibleTimeSource
                && ((ConvertibleTimeSource) keySource).supportsTimeConversion();
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
        return ((ConvertibleTimeSource) keySource).toZonedDateTime(zone);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {
        return ((ConvertibleTimeSource) keySource).toLocalDate(zone);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {
        return ((ConvertibleTimeSource) keySource).toLocalTime(zone);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return ((ConvertibleTimeSource) keySource).toInstant();
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return ((ConvertibleTimeSource) keySource).toEpochNano();
    }

    /**
     * The match filter this column answers by probing, or null if {@code filter} is not one or probing would cost more
     * than scanning {@code selection}.
     */
    private MatchFilter supportedMatchFilter(@NotNull final WhereFilter filter, @NotNull final RowSet selection) {
        if (QueryTable.DISABLE_WHERE_PUSHDOWN_AGGREGATION_KEY) {
            return null;
        }
        final MatchFilter matchFilter =
                MatchFilter.extractMatchFilter(WhereFilterDelegating.maybeUnwrapFilter(filter)).orElse(null);
        // A BigDecimal match is decided by compareTo, but the aggregation groups BigDecimals by equals. Every positive
        // byte reads as a true Boolean, but the aggregation groups Booleans by their bytes, so a value has no one byte
        // to probe; a Boolean key has few states anyway.
        if (matchFilter == null || matchFilter.getMatchOptions().caseInsensitive()
                || getType() == BigDecimal.class || getType() == Boolean.class) {
            return null;
        }
        // A probe costs about as much as testing one row, so a selection with fewer rows than values is scanned.
        if (matchFilter.getValues().length > selection.size()) {
            return null;
        }
        return matchFilter;
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
        final MatchFilter matchFilter = supportedMatchFilter(filter, selection);
        if (matchFilter == null) {
            onComplete.accept(PushdownResult.UNSUPPORTED_ACTION_COST);
            return;
        }
        onComplete.accept(PushdownResult.TABLE_KEY_LOOKUP_COST);
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
        final MatchFilter matchFilter = supportedMatchFilter(filter, selection);
        if (matchFilter == null || costCeiling < PushdownResult.TABLE_KEY_LOOKUP_COST) {
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }

        final long step = updateGraph.clock().currentStep();
        if (!probesValid(usePrev, step)) {
            // the hash table no longer holds the keys at step
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }
        final RowSet found;
        try {
            found = probe(matchFilter.getValues());
        } catch (final RuntimeException e) {
            if (probesValid(usePrev, step)) {
                throw e;
            }
            // a change to the hash table during the probe tore its reads
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }
        try (found) {
            if (!probesValid(usePrev, step)) {
                // a change to the hash table overlapped the probe
                onComplete.accept(PushdownResult.allMaybeMatch(selection));
                return;
            }
            try (final RowSet matching = matchFilter.getMatchOptions().inverted()
                    ? selection.minus(found)
                    : found.intersect(selection)) {
                onComplete.accept(PushdownResult.exactMatch(selection, matching));
            }
        }
    }

    /**
     * The row keys of the states for {@code values}.
     */
    private RowSet probe(final Object[] values) {
        final RowSetBuilderRandom foundBuilder = RowSetFactory.builderRandom();
        for (final Object value : values) {
            if (!reinterpretedKeys) {
                final int rowKey = stateManager.findPositionForKey(value);
                if (rowKey >= 0) {
                    foundBuilder.addKey(rowKey);
                }
                continue;
            }
            final Object lookupKey;
            try {
                lookupKey = reinterpretKey(value);
            } catch (final DateTimeUtils.DateTimeOverflowException e) {
                // no row holds a time that its primitive can not represent
                continue;
            }
            final int rowKey = stateManager.findPositionForKey(lookupKey);
            // The filter matches the column's values by equals, which distinguishes what the primitive does not: a
            // time that converts to the null primitive.
            if (rowKey >= 0 && Objects.equals(keySource.get(rowKey), value)) {
                foundBuilder.addKey(rowKey);
            }
        }
        return foundBuilder.build();
    }

    /**
     * Whether the hash table held its keys at {@code step}, in {@code usePrev} space, throughout the probes made since
     * {@code step} was read from the clock. Every cycle before {@code step} had finished by then, and a cycle marks its
     * step before it changes the hash table; previous values need no cycle at {@code step} or later to have begun, and
     * current values, which the filter reads once this column is satisfied at {@code step}, no later one.
     */
    private boolean probesValid(final boolean usePrev, final long step) {
        if (incrementalStateManager == null) {
            return true;
        }
        // a volatile read does not keep the hash table reads before it from happening after it
        VarHandle.acquireFence();
        final long updateCycleStep = incrementalStateManager.updateCycleStep();
        return usePrev ? updateCycleStep < step : updateCycleStep <= step;
    }

    /**
     * Convert a match value of this column's type, {@link Instant}, to the epoch nanoseconds the state manager hashes.
     */
    private static Object reinterpretKey(final Object value) {
        return DateTimeUtils.epochNanos((Instant) value);
    }
}
