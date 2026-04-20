//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.locations.InvalidatedRegionException;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.SafeCloseable;

import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base {@link ColumnRegion} implementation.
 */
public abstract class GenericColumnRegionBase<ATTR extends Any> implements ColumnRegion<ATTR> {

    private final long pageMask;
    private volatile boolean invalidated = false;

    public GenericColumnRegionBase(final long pageMask) {
        this.pageMask = pageMask;
    }

    @Override
    public final long mask() {
        return pageMask;
    }

    @Override
    public void invalidate() {
        this.invalidated = true;
    }

    protected final void throwIfInvalidated() {
        if (invalidated) {
            throw new InvalidatedRegionException("Column region has been invalidated due to data removal");
        }
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
        if (selection.isEmpty()) {
            // If the selection is empty, we can skip all pushdown filtering.
            onComplete.accept(Long.MAX_VALUE);
            return;
        }

        final RegionedPushdownFilterLocationContext filterCtx = (RegionedPushdownFilterLocationContext) context;
        Assert.neqNull(filterCtx.tableLocation(), "filterCtx.tableLocation()");

        final TableLocation tableLocation = filterCtx.tableLocation();

        // We must consider all the actions from this region and from the TableLocation and execute them in
        // minimal cost order.
        final List<RegionedPushdownAction> sorted =
                Stream.concat(supportedActions().stream(), tableLocation.supportedActions().stream())
                        .filter(action -> action instanceof RegionedPushdownAction.Region
                                ? ((RegionedPushdownAction.Region) action).allows(tableLocation, this, filterCtx)
                                : action.allows(tableLocation, filterCtx))
                        .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                        .collect(Collectors.toList());

        if (sorted.isEmpty()) {
            onComplete.accept(Long.MAX_VALUE);
            return;
        }

        // Deferred contexts, to be created only when needed.
        RegionedPushdownAction.EstimateContext regionEstimateCtx = null;
        RegionedPushdownAction.EstimateContext locationEstimateCtx = null;

        // The list is sorted by filterCost, so the first supported action is the minimum cost.
        long minCost = Long.MAX_VALUE;
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
            if (cost != Long.MAX_VALUE) {
                minCost = cost;
                break;
            }
        }

        // noinspection ConstantConditions
        SafeCloseable.closeAll(regionEstimateCtx, locationEstimateCtx);

        // Return the lowest cost operation.
        onComplete.accept(minCost);
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
        if (selection.isEmpty()) {
            // If the selection is empty, we can skip all pushdown filtering.
            onComplete.accept(PushdownResult.allNoMatch(selection));
            return;
        }

        final RegionedPushdownFilterLocationContext filterCtx = (RegionedPushdownFilterLocationContext) context;
        Assert.neqNull(filterCtx.tableLocation(), "filterCtx.tableLocation()");

        final TableLocation tableLocation = filterCtx.tableLocation();

        // We must consider all the actions from this region and from the AbstractTableLocation and execute them in
        // minimal cost order
        final List<RegionedPushdownAction> sorted =
                Stream.concat(supportedActions().stream(), tableLocation.supportedActions().stream())
                        .filter(action -> action instanceof RegionedPushdownAction.Region
                                ? ((RegionedPushdownAction.Region) action).allows(tableLocation, this, filterCtx,
                                        costCeiling)
                                : action.allows(tableLocation, filterCtx, costCeiling))
                        .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                        .collect(Collectors.toList());

        // If no modes are allowed, we can skip all pushdown filtering.
        if (sorted.isEmpty()) {
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }

        // Initialize the pushdown result with the selection rowset as "maybe" rows
        PushdownResult result = PushdownResult.allMaybeMatch(selection);

        // Deferred contexts, to be created only when needed.
        RegionedPushdownAction.ActionContext regionCtx = null;
        RegionedPushdownAction.ActionContext locationCtx = null;

        // Iterate through the sorted actions
        for (final RegionedPushdownAction action : sorted) {
            try (final PushdownResult ignored = result) {
                if (action instanceof RegionedPushdownAction.Location) {
                    result = tableLocation.performPushdownAction(action, filter, selection, result, usePrev, filterCtx,
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
                // No maybe rows remaining, so no reason to continue filtering.
                break;
            }
        }

        // noinspection ConstantConditions
        SafeCloseable.closeAll(regionCtx, locationCtx);

        // Return the final result
        onComplete.accept(result);
    }
}
