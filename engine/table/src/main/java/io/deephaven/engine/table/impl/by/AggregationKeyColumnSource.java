//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegating;
import io.deephaven.engine.table.impl.sources.DelegatingColumnSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZonedDateTime;
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
 * probe on another thread is abandoned, and the filter scans the column, when a cycle began during the probe, or when
 * filtering previous values after this cycle began.
 */
public class AggregationKeyColumnSource<T> extends DelegatingColumnSource<T, T> implements FillUnordered<Values> {

    private final ColumnSource<T> keySource;
    private final OperatorAggregationStateManager stateManager;
    private final boolean reinterpretedKeys;
    /** The state manager of a refreshing aggregation, or null for a static one, which never changes. */
    private final IncrementalOperatorAggregationStateManager incrementalStateManager;
    private final UpdateGraph updateGraph;

    /**
     * @param keySource the result's key source, of the group-by column's type
     * @param stateManager the aggregation's state manager, whose output positions are the result's row keys
     * @param reinterpretedKeys whether {@code stateManager} hashes the keys reinterpreted as primitives, per
     *        {@link io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive}
     * @param updateGraph the aggregation's update graph
     */
    AggregationKeyColumnSource(
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
    public boolean providesFillUnordered() {
        return FillUnordered.providesFillUnordered(keySource);
    }

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        // noinspection unchecked
        ((FillUnordered<Values>) keySource).fillChunkUnordered(context, dest, keys);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        // noinspection unchecked
        ((FillUnordered<Values>) keySource).fillPrevChunkUnordered(context, dest, keys);
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
        // A BigDecimal match is decided by compareTo, but the aggregation groups BigDecimals by equals.
        if (matchFilter == null || matchFilter.getMatchOptions().caseInsensitive() || getType() == BigDecimal.class) {
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

        final long startStep =
                incrementalStateManager == null ? QueryConstants.NULL_LONG : incrementalStateManager.updateCycleStep();
        if (usePrev && incrementalStateManager != null && startStep == updateGraph.clock().currentStep()) {
            // this cycle's changes to the hash table have begun, so it no longer holds the previous keys
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }
        final RowSet found;
        try {
            found = probe(matchFilter.getValues());
        } catch (final RuntimeException e) {
            if (incrementalStateManager == null || unchangedSince(startStep)) {
                throw e;
            }
            // a change to the hash table during the probe tore its reads
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }
        try (found) {
            if (incrementalStateManager != null && !unchangedSince(startStep)) {
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
            final int rowKey = stateManager.findPositionForKey(reinterpretedKeys ? reinterpretKey(value) : value);
            if (rowKey >= 0) {
                foundBuilder.addKey(rowKey);
            }
        }
        return foundBuilder.build();
    }

    /**
     * Whether no update cycle began since {@code startStep} was read, so no change to the hash table overlapped the
     * reads in between.
     */
    private boolean unchangedSince(final long startStep) {
        // a volatile read does not keep the hash table reads before it from happening after it
        VarHandle.acquireFence();
        return incrementalStateManager.updateCycleStep() == startStep;
    }

    /**
     * Convert a match value of this column's type to the primitive the state manager hashes.
     */
    private Object reinterpretKey(final Object value) {
        final Class<T> type = getType();
        if (type == Instant.class) {
            return DateTimeUtils.epochNanos((Instant) value);
        }
        if (type == ZonedDateTime.class) {
            return DateTimeUtils.epochNanos((ZonedDateTime) value);
        }
        return BooleanUtils.booleanAsByte((Boolean) value);
    }
}
