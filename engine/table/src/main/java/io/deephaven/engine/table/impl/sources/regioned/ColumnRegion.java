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
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.MustBeInvokedByOverriders;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface ColumnRegion<ATTR extends Any> extends Page<ATTR>, Releasable, RegionedPushdownFilterMatcher {

    @Override
    @FinalDefault
    default long firstRowOffset() {
        return 0;
    }

    /**
     * Invalidate the region -- any further reads that cannot be completed consistently and correctly will fail.
     */
    void invalidate();

    abstract class Null<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegion<ATTR>, WithDefaultsForRepeatingValues<ATTR> {
        private static final RegionedPushdownAction.Region NullColumnRegion =
                new RegionedPushdownAction.Region(
                        () -> false,
                        PushdownResult.SINGLE_VALUE_REGION_COST,
                        BasePushdownFilterContext::supportsMetadataFiltering,
                        (tl) -> true,
                        (cr) -> cr instanceof Null);

        private static final List<RegionedPushdownAction> supportedActions = List.of(NullColumnRegion);

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
            return supportedActions;
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

            // We can resolve all the maybe with one test.
            final BasePushdownFilterContext.FilterNullBehavior nullBehavior = filterCtx.filterNullBehavior();
            if (nullBehavior == BasePushdownFilterContext.FilterNullBehavior.INCLUDES_NULLS) {
                return PushdownResult.of(selection, input.match().union(input.maybeMatch()), RowSetFactory.empty());
            } else if (nullBehavior == BasePushdownFilterContext.FilterNullBehavior.EXCLUDES_NULLS) {
                return PushdownResult.of(selection, input.match(), RowSetFactory.empty());
            }
            return input.copy();
        }
    }

    default RegionedPushdownAction.EstimateContext makeEstimateContext(
            final WhereFilter filter,
            final PushdownFilterContext context) {
        return RegionedPushdownAction.DEFAULT_ESTIMATE_CONTEXT;
    }

    default RegionedPushdownAction.ActionContext makeActionContext(
            final WhereFilter filter,
            final PushdownFilterContext context) {
        return RegionedPushdownAction.DEFAULT_ACTION_CONTEXT;
    }
}
