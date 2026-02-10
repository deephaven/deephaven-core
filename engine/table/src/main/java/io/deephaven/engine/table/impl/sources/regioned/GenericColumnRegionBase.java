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

        final AbstractTableLocation tableLocation = filterCtx.tableLocation();

        // We must consider all the actions from this region and from the AbstractTableLocation and execute them in
        // minimal cost order.
        final List<RegionedPushdownAction> sortedRegion = supportedActions()
                .stream()
                .filter(action -> ((RegionedPushdownAction.Region) action).allows(tableLocation, this, filterCtx))
                .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                .collect(Collectors.toList());

        final List<RegionedPushdownAction> sortedLocation = tableLocation.supportedActions()
                .stream()
                .filter(action -> action.allows(tableLocation, filterCtx))
                .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                .collect(Collectors.toList());

        if (sortedRegion.isEmpty() && sortedLocation.isEmpty()) {
            onComplete.accept(Long.MAX_VALUE);
            return;
        }

        // Get the lowest cost for the region actions.
        final long regionCost;
        try (final RegionedPushdownAction.EstimateContext estimateCtx = makeEstimateContext(filter, filterCtx)) {
            regionCost = estimatePushdownAction(sortedRegion, filter, selection, usePrev, filterCtx, estimateCtx);
        }

        // Get the lowest cost for the location actions.
        final long locationCost;
        try (final RegionedPushdownAction.EstimateContext estimateCtx =
                tableLocation.makeEstimateContext(filter, filterCtx)) {
            locationCost = tableLocation.estimatePushdownAction(sortedLocation, filter, selection, usePrev, filterCtx,
                    estimateCtx);
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
        final List<RegionedPushdownAction> sortedRegion = supportedActions()
                .stream()
                .filter(action -> ((RegionedPushdownAction.Region) action).allows(tableLocation, this, filterCtx,
                        costCeiling))
                .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                .collect(Collectors.toList());

        final List<RegionedPushdownAction> sortedLocation = tableLocation.supportedActions()
                .stream()
                .filter(action -> action.allows(tableLocation, filterCtx, costCeiling))
                .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                .collect(Collectors.toList());

        // combine the two lists and sort by filterCost()
        final List<RegionedPushdownAction> sorted = Stream.concat(sortedRegion.stream(), sortedLocation.stream())
                .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                .collect(Collectors.toList());

        // If no modes are allowed, we can skip all pushdown filtering.
        if (sorted.isEmpty()) {
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
            return;
        }

        // Initialize the pushdown result with the selection rowset as "maybe" rows
        PushdownResult result = PushdownResult.allMaybeMatch(selection);

        // Delegate to TableLocation to determine which of the supported actions applies to this particular location.
        try (final RegionedPushdownAction.ActionContext regionCtx = makeActionContext(filter, filterCtx);
                final RegionedPushdownAction.ActionContext locationCtx =
                        tableLocation.makeActionContext(filter, filterCtx)) {
            for (RegionedPushdownAction action : sorted) {
                try (final PushdownResult ignored = result) {
                    // Execute either the
                    result = action instanceof RegionedPushdownAction.Location
                            ? tableLocation.performPushdownAction(action, filter, selection, result, usePrev, filterCtx,
                                    locationCtx)
                            : performPushdownAction(action, filter, selection, result, usePrev, filterCtx, regionCtx);
                }
                if (result.maybeMatch().isEmpty()) {
                    // No maybe rows remaining, so no reason to continue filtering.
                    break;
                }
            }
            // Return the final result
            onComplete.accept(result);
        }
    }
}
