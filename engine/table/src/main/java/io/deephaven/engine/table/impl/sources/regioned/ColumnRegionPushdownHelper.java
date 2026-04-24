//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.SafeCloseable;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper utilities for column regions that support pushdown operations.
 */
final class ColumnRegionPushdownHelper {

    private ColumnRegionPushdownHelper() {}

    static long estimatePushdownFilterCost(
            final ColumnRegion<?> region,
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final RegionedPushdownFilterContext filterCtx) {
        if (selection.isEmpty()) {
            return Long.MAX_VALUE;
        }

        final Optional<ColumnLocation> columnLocation = region.getColumnLocation();
        final TableLocation tableLocation = columnLocation.map(ColumnLocation::getTableLocation).orElse(null);

        final List<RegionedPushdownAction> sorted =
                Stream.concat(region.supportedActions().stream(),
                        tableLocation == null ? Stream.empty() : tableLocation.supportedActions().stream())
                        .filter(action -> action.allows(tableLocation, region, filterCtx))
                        .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                        .collect(Collectors.toList());

        if (sorted.isEmpty()) {
            return Long.MAX_VALUE;
        }

        RegionedPushdownAction.EstimateContext regionEstimateCtx = null;
        RegionedPushdownAction.EstimateContext locationEstimateCtx = null;

        long minCost = Long.MAX_VALUE;
        try {
            for (final RegionedPushdownAction action : sorted) {
                final long cost;
                if (action instanceof RegionedPushdownAction.Location) {
                    cost = tableLocation.estimatePushdownAction(action, filter, selection, usePrev, filterCtx,
                            locationEstimateCtx == null
                                    ? (locationEstimateCtx = tableLocation.makeEstimateContext(filter, filterCtx))
                                    : locationEstimateCtx);
                } else {
                    cost = region.estimatePushdownAction(action, filter, selection, usePrev, filterCtx,
                            regionEstimateCtx == null
                                    ? (regionEstimateCtx = region.makeEstimateContext(filter, filterCtx))
                                    : regionEstimateCtx);
                }
                if (cost != Long.MAX_VALUE) {
                    minCost = cost;
                    break;
                }
            }
        } finally {
            SafeCloseable.closeAll(regionEstimateCtx, locationEstimateCtx);
        }

        return minCost;
    }

    static PushdownResult pushdownFilter(
            final ColumnRegion<?> region,
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final RegionedPushdownFilterContext filterCtx,
            final long costCeiling) {
        if (selection.isEmpty()) {
            return PushdownResult.noneMatch(selection);
        }

        final Optional<ColumnLocation> columnLocation = region.getColumnLocation();
        final TableLocation tableLocation = columnLocation.map(ColumnLocation::getTableLocation).orElse(null);

        final List<RegionedPushdownAction> sorted =
                Stream.concat(region.supportedActions().stream(),
                        tableLocation == null ? Stream.empty() : tableLocation.supportedActions().stream())
                        .filter(action -> action.allows(tableLocation, region, filterCtx, costCeiling))
                        .sorted(Comparator.comparingLong(RegionedPushdownAction::filterCost))
                        .collect(Collectors.toList());

        if (sorted.isEmpty()) {
            return PushdownResult.allMaybeMatch(selection);
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
                        result = region.performPushdownAction(action, filter, selection, result, usePrev, filterCtx,
                                regionCtx == null
                                        ? (regionCtx = region.makeActionContext(filter, filterCtx))
                                        : regionCtx);
                    }
                }
                if (result.maybeMatch().isEmpty()) {
                    break;
                }
            }
            return result;
        } finally {
            SafeCloseable.closeAll(regionCtx, locationCtx);
        }
    }

    /**
     * Return the supported pushdown actions for a {@link RegionedPageStore} by delegating to its first subregion, or an
     * empty list if the page store has no subregions.
     */
    static <ATTR extends Any, REGION_TYPE extends ColumnRegion<ATTR>> List<RegionedPushdownAction> pageStoreSupportedActions(
            final RegionedPageStore<ATTR, ATTR, REGION_TYPE> pageStore) {
        if (pageStore.getRegionCount() == 0) {
            return List.of();
        }
        return pageStore.getRegion(0).supportedActions();
    }

    /**
     * Estimate the cost of a pushdown action for a {@link RegionedPageStore} by delegating to its first subregion, or
     * {@link PushdownResult#UNSUPPORTED_ACTION_COST} if the page store has no subregions.
     */
    static <ATTR extends Any, REGION_TYPE extends ColumnRegion<ATTR>> long estimatePageStorePushdownAction(
            final RegionedPageStore<ATTR, ATTR, REGION_TYPE> pageStore,
            final RegionedPushdownAction action,
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext filterContext,
            final RegionedPushdownAction.EstimateContext estimateContext) {
        if (pageStore.getRegionCount() == 0) {
            return PushdownResult.UNSUPPORTED_ACTION_COST;
        }
        return pageStore.getRegion(0).estimatePushdownAction(action, filter, selection, usePrev, filterContext,
                estimateContext);
    }

    /**
     * Iterate over the subregions of a {@link RegionedPageStore} that overlap {@code selection}, dispatch
     * {@link RegionedPushdownFilterMatcher#performPushdownAction} on each subregion in local key space, and assemble
     * the per-subregion results into a single page-store-space {@link PushdownResult}.
     */
    static <ATTR extends Any, REGION_TYPE extends ColumnRegion<ATTR>> PushdownResult performPageStorePushdownAction(
            final RegionedPageStore<ATTR, ATTR, REGION_TYPE> pageStore,
            final RegionedPushdownAction action,
            final WhereFilter filter,
            final RowSet selection,
            final PushdownResult input,
            final boolean usePrev,
            final PushdownFilterContext filterContext,
            final RegionedPushdownAction.ActionContext actionContext) {
        final int regionCount = pageStore.getRegionCount();
        if (regionCount == 0) {
            // No regions, no matches.
            return PushdownResult.of(selection, RowSetFactory.empty(), RowSetFactory.empty());
        }

        final RowSetBuilderSequential maybeBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential matchBuilder = RowSetFactory.builderSequential();

        final long regionSize = pageStore.regionMask();

        // Only testing "maybe" rows
        try (final RowSequence.Iterator maybeIt = input.maybeMatch().getRowSequenceIterator()) {
            for (int regionIndex = 0; regionIndex < regionCount; regionIndex++) {
                final REGION_TYPE region = pageStore.getRegion(regionIndex);
                final long regionFirstKey = (long) regionIndex << pageStore.regionMaskNumBits();
                final long regionLastKey = regionFirstKey + regionSize - 1;

                final RowSequence rs = maybeIt.getNextRowSequenceThrough(regionLastKey);
                if (rs.isEmpty()) {
                    continue;
                }

                // Create a PushdownResult restricted to the "maybe" from this region
                try (final RowSet shifted = rs.asRowSet().shift(-regionFirstKey);
                        final PushdownResult localInput = PushdownResult.allMaybeMatch(shifted)) {
                    // Perform the pushdown action on the region, accumulate the results
                    final PushdownResult localResult = region.performPushdownAction(
                            action,
                            filter,
                            shifted,
                            localInput,
                            usePrev,
                            filterContext,
                            actionContext);
                    localResult.match().shiftInPlace(regionFirstKey);
                    matchBuilder.appendRowSequence(localResult.match());
                    localResult.maybeMatch().shiftInPlace(regionFirstKey);
                    maybeBuilder.appendRowSequence(localResult.maybeMatch());
                }
            }

            // Return a new PushdownResult with the results from the subregions
            try (final RowSet maybe = maybeBuilder.build();
                    final RowSet match = matchBuilder.build();
                    final RowSet unionedMatch = match.union(input.match())) {
                return PushdownResult.of(selection, unionedMatch, maybe);
            }
        }
    }
}
