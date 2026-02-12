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
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;

import java.util.Comparator;
import java.util.Iterator;
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

        final AbstractTableLocation tableLocation = filterCtx.tableLocation();

        // We must consider all the actions from this region and from the AbstractTableLocation and execute them in
        // minimal cost order.
        final List<RegionedPushdownAction> sortedRegion = supportedActions()
                .stream()
                .filter(action -> ((RegionedPushdownAction.Region) action).allows(tableLocation, this, filterCtx))
                .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                .collect(Collectors.toList());

        final long regionCost;
        if (!sortedRegion.isEmpty()) {
            try (final RegionedPushdownAction.EstimateContext estimateCtx = makeEstimateContext(filter, filterCtx)) {
                regionCost = estimatePushdownAction(sortedRegion, filter, selection, usePrev, filterCtx, estimateCtx);
            }
        } else {
            regionCost = Long.MAX_VALUE;
        }

        // Consider the location actions that are less expensive than the lowest cost region action.
        final List<RegionedPushdownAction> sortedLocation = tableLocation.supportedActions()
                .stream()
                .filter(action -> action.allows(tableLocation, filterCtx))
                .filter(action -> action.filterCost() < regionCost)
                .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                .collect(Collectors.toList());

        final long locationCost;
        if (!sortedLocation.isEmpty()) {
            try (final RegionedPushdownAction.EstimateContext estimateCtx =
                    tableLocation.makeEstimateContext(filter, filterCtx)) {
                locationCost =
                        tableLocation.estimatePushdownAction(sortedLocation, filter, selection, usePrev, filterCtx,
                                estimateCtx);
            }
        } else {
            locationCost = Long.MAX_VALUE;
        }

        // Return the lowest cost operation.
        onComplete.accept(Math.min(regionCost, locationCost));
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
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }

        final RegionedPushdownFilterLocationContext filterCtx = (RegionedPushdownFilterLocationContext) context;
        Assert.neqNull(filterCtx.tableLocation(), "filterCtx.tableLocation()");

        final AbstractTableLocation tableLocation = filterCtx.tableLocation();

        // We must consider all the actions from this region and from the AbstractTableLocation and execute them in
        // minimal cost order
        final Stream<RegionedPushdownAction> sorted =
                Stream.concat(supportedActions().stream(), tableLocation.supportedActions().stream())
                        .filter(action -> action instanceof RegionedPushdownAction.Region
                                ? ((RegionedPushdownAction.Region) action).allows(tableLocation, this, filterCtx,
                                        costCeiling)
                                : action.allows(tableLocation, filterCtx, costCeiling))
                        .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost));

        // If no modes are allowed, we can skip all pushdown filtering.
        if (sorted.findAny().isEmpty()) {
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }

        // Initialize the pushdown result with the selection rowset as "maybe" rows
        PushdownResult result = PushdownResult.allMaybeMatch(selection);

        // Deferred contexts, to be created only when needed.
        RegionedPushdownAction.ActionContext regionCtx = null;
        RegionedPushdownAction.ActionContext locationCtx = null;

        // Iterate through the sorted actions
        final Iterator<RegionedPushdownAction> it = sorted.iterator();
        while (it.hasNext()) {
            final RegionedPushdownAction action = it.next();
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
