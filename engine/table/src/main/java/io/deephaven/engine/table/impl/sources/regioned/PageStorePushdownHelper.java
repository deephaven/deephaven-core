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
import io.deephaven.engine.table.impl.select.WhereFilter;

import java.util.List;

/**
 * Helper utilities for page store classes that support pushdown operations.
 */
abstract class PageStorePushdownHelper {

    private PageStorePushdownHelper() {}


    /**
     * Return the supported pushdown actions for a {@link RegionedPageStore} by delegating to its first subregion, or an
     * empty list if the page store has no subregions.
     */
    static <ATTR extends Any, REGION_TYPE extends ColumnRegion<ATTR>> List<RegionedPushdownAction> supportedActions(
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
    static <ATTR extends Any, REGION_TYPE extends ColumnRegion<ATTR>> long estimatePushdownAction(
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
    static <ATTR extends Any, REGION_TYPE extends ColumnRegion<ATTR>> PushdownResult performPushdownAction(
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
            return PushdownResult.noneMatch(selection);
        }

        final RowSetBuilderSequential maybeBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential matchBuilder = RowSetFactory.builderSequential();

        // Only testing "maybe" rows
        try (final RowSequence.Iterator maybeIt = input.maybeMatch().getRowSequenceIterator()) {
            while (maybeIt.hasMore()) {
                final long regionFirstIncludedKey = maybeIt.peekNextKey();
                final REGION_TYPE region = pageStore.lookupRegion(regionFirstIncludedKey);
                final RowSequence regionRows = maybeIt.getNextRowSequenceThrough(region.maxRow(regionFirstIncludedKey));
                final long regionFirstKey = region.firstRow(regionFirstIncludedKey);

                // Create a PushdownResult restricted to the "maybe" from this region
                try (final RowSet shifted = regionRows.asRowSet().shift(-regionFirstKey);
                        final PushdownResult localInput = PushdownResult.allMaybeMatch(shifted);
                        final PushdownResult localResult = region.performPushdownAction(
                                action,
                                filter,
                                shifted,
                                localInput,
                                usePrev,
                                filterContext,
                                actionContext)) {
                    // Perform the pushdown action on the region, accumulate the results
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
