//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Releasable;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.MustBeInvokedByOverriders;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface ColumnRegion<ATTR extends Any> extends Page<ATTR>, Releasable, RegionedPushdownFilterMatcher {

    @Override
    @FinalDefault
    default long firstRowOffset() {
        return 0;
    }

    default Optional<ColumnLocation> getColumnLocation() {
        return Optional.empty();
    }

    /**
     * Invalidate the region -- any further reads that cannot be completed consistently and correctly will fail.
     */
    void invalidate();

    @Override
    default void estimatePushdownFilterCost(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final JobScheduler jobScheduler,
            final LongConsumer onComplete,
            final Consumer<Exception> onError) {
        if (selection.isEmpty()) {
            onComplete.accept(PushdownResult.UNSUPPORTED_ACTION_COST);
            return;
        }

        final RegionedPushdownFilterContext filterCtx = (RegionedPushdownFilterContext) context;
        final Optional<ColumnLocation> columnLocation = getColumnLocation();
        final TableLocation tableLocation = columnLocation.map(ColumnLocation::getTableLocation).orElse(null);

        final List<RegionedPushdownAction> sorted =
                Stream.concat(supportedActions().stream(),
                        tableLocation == null ? Stream.empty() : tableLocation.supportedActions().stream())
                        .filter(action -> action.allows(tableLocation, this, filterCtx))
                        .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                        .collect(Collectors.toList());

        if (sorted.isEmpty()) {
            onComplete.accept(PushdownResult.UNSUPPORTED_ACTION_COST);
            return;
        }

        RegionedPushdownAction.EstimateContext regionEstimateCtx = null;
        RegionedPushdownAction.EstimateContext locationEstimateCtx = null;

        long minCost = PushdownResult.UNSUPPORTED_ACTION_COST;
        try {
            for (final RegionedPushdownAction action : sorted) {
                final long cost;
                if (action instanceof RegionedPushdownAction.Location) {
                    cost = tableLocation.estimatePushdownAction(action, filter, selection, usePrev, filterCtx,
                            locationEstimateCtx == null
                                    ? (locationEstimateCtx = tableLocation.makeEstimateContext(filter, filterCtx))
                                    : locationEstimateCtx);
                } else {
                    cost = estimatePushdownAction(action, filter, selection, usePrev, filterCtx,
                            regionEstimateCtx == null
                                    ? (regionEstimateCtx = makeEstimateContext(filter, filterCtx))
                                    : regionEstimateCtx);
                }
                if (cost != PushdownResult.UNSUPPORTED_ACTION_COST) {
                    minCost = cost;
                    break;
                }
            }
        } finally {
            SafeCloseable.closeAll(regionEstimateCtx, locationEstimateCtx);
        }
        onComplete.accept(minCost);
    }

    @Override
    default void pushdownFilter(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {
        if (selection.isEmpty()) {
            onComplete.accept(PushdownResult.noneMatch(selection));
            return;
        }

        final RegionedPushdownFilterContext filterCtx = (RegionedPushdownFilterContext) context;
        final Optional<ColumnLocation> columnLocation = getColumnLocation();
        final TableLocation tableLocation = columnLocation.map(ColumnLocation::getTableLocation).orElse(null);

        final List<RegionedPushdownAction> sorted =
                Stream.concat(supportedActions().stream(),
                        tableLocation == null ? Stream.empty() : tableLocation.supportedActions().stream())
                        .filter(action -> action.allows(tableLocation, this, filterCtx, costCeiling))
                        .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                        .collect(Collectors.toList());

        if (sorted.isEmpty()) {
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }

        PushdownResult result = PushdownResult.allMaybeMatch(selection);
        RegionedPushdownAction.ActionContext regionCtx = null;
        RegionedPushdownAction.ActionContext locationCtx = null;

        try {
            for (final RegionedPushdownAction action : sorted) {
                try (final PushdownResult ignored = result) {
                    if (action instanceof RegionedPushdownAction.Location) {
                        result = tableLocation.performPushdownAction(action, filter, selection, result, usePrev,
                                filterCtx,
                                locationCtx == null
                                        ? (locationCtx = tableLocation.makeActionContext(filter, filterCtx))
                                        : locationCtx);
                    } else {
                        result = performPushdownAction(action, filter, selection, result, usePrev, filterCtx,
                                regionCtx == null
                                        ? (regionCtx = makeActionContext(filter, filterCtx))
                                        : regionCtx);
                    }
                }
                if (result.maybeMatch().isEmpty()) {
                    break;
                }
            }
            onComplete.accept(result);
        } finally {
            SafeCloseable.closeAll(regionCtx, locationCtx);
        }
    }

    abstract class Null<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegion<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private static final RegionedPushdownAction NULL_COLUMN_REGION =
                new RegionedPushdownAction.Region(
                        () -> false,
                        PushdownResult.REGION_SINGLE_VALUE_COST,
                        (ctx) -> true,
                        (tl, cr) -> cr instanceof Null);
        private static final List<RegionedPushdownAction> SUPPORTED_ACTIONS = List.of(NULL_COLUMN_REGION);

        Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();

            destination.fillWithNullValue(offset, length);
            destination.setSize(offset + length);
        }

        @Override
        public List<RegionedPushdownAction> supportedActions() {
            return SUPPORTED_ACTIONS;
        }

        @Override
        public long estimatePushdownAction(
                final RegionedPushdownAction action,
                final WhereFilter filter,
                final RowSet selection,
                final boolean usePrev,
                final PushdownFilterContext filterContext,
                final RegionedPushdownAction.EstimateContext estimateContext) {
            return action == NULL_COLUMN_REGION ? NULL_COLUMN_REGION.filterCost()
                    : PushdownResult.UNSUPPORTED_ACTION_COST;
        }

        @Override
        @MustBeInvokedByOverriders
        public PushdownResult performPushdownAction(
                final RegionedPushdownAction action,
                final WhereFilter filter,
                final RowSet selection,
                final PushdownResult input,
                final boolean usePrev,
                final PushdownFilterContext filterContext,
                final RegionedPushdownAction.ActionContext actionContext) {
            final RegionedPushdownFilterContext filterCtx = (RegionedPushdownFilterContext) filterContext;

            final BasePushdownFilterContext.FilterNullBehavior nullBehavior = filterCtx.filterNullBehavior();
            if (nullBehavior == BasePushdownFilterContext.FilterNullBehavior.FAILS_ON_NULLS) {
                // Bad-behaving filter, but not our responsibility to handle during pushdown.
                return input.copy();
            }
            if (nullBehavior == BasePushdownFilterContext.FilterNullBehavior.INCLUDES_NULLS) {
                // Promote all maybe rows to match.
                try (final RowSet allMatch = input.match().union(input.maybeMatch())) {
                    return PushdownResult.of(selection, allMatch, RowSetFactory.empty());
                }
            }
            // None of these rows match, return the original match rows.
            return PushdownResult.of(selection, input.match(), RowSetFactory.empty());
        }
    }
}
