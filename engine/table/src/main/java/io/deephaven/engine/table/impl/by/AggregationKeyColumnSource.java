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
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;

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
 * The aggregation never moves or removes a key's state, so a key's row key is fixed once assigned. The probe reads the
 * hash table as it is now; when filtering previous values, the aggregation may be modifying its hash table
 * concurrently, so the pushdown declines and the filter scans the column.
 */
public class AggregationKeyColumnSource<T> extends DelegatingColumnSource<T, T> implements FillUnordered<Values> {

    private final ColumnSource<T> keySource;
    private final OperatorAggregationStateManager stateManager;
    private final boolean reinterpretedKeys;

    /**
     * @param keySource the result's key source, of the group-by column's type
     * @param stateManager the aggregation's state manager, whose output positions are the result's row keys
     * @param reinterpretedKeys whether {@code stateManager} hashes the keys reinterpreted as primitives, per
     *        {@link io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive}
     */
    AggregationKeyColumnSource(
            @NotNull final ColumnSource<T> keySource,
            @NotNull final OperatorAggregationStateManager stateManager,
            final boolean reinterpretedKeys) {
        super(keySource.getType(), keySource.getComponentType(), keySource);
        this.keySource = keySource;
        this.stateManager = stateManager;
        this.reinterpretedKeys = reinterpretedKeys;
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
    private MatchFilter supportedMatchFilter(
            @NotNull final WhereFilter filter,
            @NotNull final RowSet selection,
            final boolean usePrev) {
        if (QueryTable.DISABLE_WHERE_PUSHDOWN_AGGREGATION_KEY || usePrev) {
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
        final MatchFilter matchFilter = supportedMatchFilter(filter, selection, usePrev);
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
        final MatchFilter matchFilter = supportedMatchFilter(filter, selection, usePrev);
        if (matchFilter == null || costCeiling < PushdownResult.TABLE_KEY_LOOKUP_COST) {
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }

        final Object[] values = matchFilter.getValues();
        final RowSetBuilderRandom matchingBuilder = RowSetFactory.builderRandom();
        for (final Object value : values) {
            final int rowKey = stateManager.findPositionForKey(reinterpretedKeys ? reinterpretKey(value) : value);
            if (rowKey >= 0) {
                matchingBuilder.addKey(rowKey);
            }
        }
        try (final RowSet found = matchingBuilder.build();
                final RowSet matching = matchFilter.getMatchOptions().inverted()
                        ? selection.minus(found)
                        : found.intersect(selection)) {
            onComplete.accept(PushdownResult.exactMatch(selection, matching));
        }
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
