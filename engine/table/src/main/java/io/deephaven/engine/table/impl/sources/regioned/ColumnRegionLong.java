//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ColumnRegionChar and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.PagingContextHolder;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValuePushdownHelper;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Optional;

/**
 * Column region interface for regions that support fetching primitive longs.
 */
public interface ColumnRegionLong<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single long from this region.
     *
     * @param elementIndex Element row key in the table's address space
     * @return The long value at the specified element row key
     */
    long getLong(long elementIndex);

    /**
     * Get a single long from this region.
     *
     * @param context A {@link PagingContextHolder} to enable resource caching where suitable, with current region index
     *        pointing to this region
     * @param elementIndex Element row key in the table's address space
     * @return The long value at the specified element row key
     */
    default long getLong(@NotNull final FillContext context, final long elementIndex) {
        return getLong(elementIndex);
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Long;
    }

    static <ATTR extends Any> ColumnRegionLong<ATTR> createNull(final long pageMask) {
        // noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<ATTR>(pageMask);
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionLong<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionLong DEFAULT_INSTANCE =
                new ColumnRegionLong.Null(RegionedColumnSourceBase.PARAMETERS.regionMask);

        public Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public long getLong(final long elementIndex) {
            return QueryConstants.NULL_LONG;
        }
    }

    final class Constant<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegionLong<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private static final RegionedPushdownAction.Region CONSTANT_COLUMN_REGION =
                new RegionedPushdownAction.Region(
                        () -> false,
                        PushdownResult.REGION_SINGLE_VALUE_COST,
                        (ctx) -> true,
                        (tl, cr) -> cr instanceof Constant);
        private static final List<RegionedPushdownAction> SUPPORTED_ACTIONS = List.of(CONSTANT_COLUMN_REGION);

        private final long value;

        public Constant(final long pageMask, final long value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public long getLong(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableLongChunk().fillWithValue(offset, length, value);
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
            return action == CONSTANT_COLUMN_REGION ? CONSTANT_COLUMN_REGION.filterCost()
                    : PushdownResult.UNSUPPORTED_ACTION_COST;
        }

        @Override
        public PushdownResult performPushdownAction(
                final RegionedPushdownAction action,
                final WhereFilter filter,
                final RowSet selection,
                final PushdownResult input,
                final boolean usePrev,
                final PushdownFilterContext filterContext,
                final RegionedPushdownAction.ActionContext actionContext) {
            final BasePushdownFilterContext filterCtx = (BasePushdownFilterContext) filterContext;

            final boolean matches;
            if (value == QueryConstants.NULL_LONG) {
                final BasePushdownFilterContext.FilterNullBehavior nullBehavior = filterCtx.filterNullBehavior();
                if (nullBehavior == BasePushdownFilterContext.FilterNullBehavior.FAILS_ON_NULLS) {
                    // Bad-behaving filter, but not our responsibility to handle during pushdown.
                    return input.copy();
                }
                matches = nullBehavior == BasePushdownFilterContext.FilterNullBehavior.INCLUDES_NULLS;
            } else {
                if (filterCtx.supportsChunkFiltering()) {
                    matches = SingleValuePushdownHelper.chunkFilter(filterCtx,
                            () -> SingleValuePushdownHelper.makeChunk(value));
                } else {
                    final ColumnSource<?> columnSource =
                            InMemoryColumnSource.makeImmutableConstantSource(long.class, null, value);
                    matches = SingleValuePushdownHelper.tableFilter(filter, selection, false, columnSource);
                }
            }
            if (matches) {
                // Promote all maybe rows to match.
                try (final RowSet allMatch = input.match().union(input.maybeMatch())) {
                    return PushdownResult.of(selection, allMatch, RowSetFactory.empty());
                }
            }
            // None of these rows match, return the original match rows.
            return PushdownResult.of(selection, input.match(), RowSetFactory.empty());
        }
    }

    final class StaticPageStore<ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionLong<ATTR>>
            implements ColumnRegionLong<ATTR> {

        private final ColumnLocation columnLocation;

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionLong<ATTR>[] regions,
                @NotNull final ColumnLocation columnLocation) {
            super(parameters, regions);
            this.columnLocation = columnLocation;
        }

        @Override
        public Optional<ColumnLocation> getColumnLocation() {
            return Optional.of(columnLocation);
        }

        @Override
        public void invalidate() {
            for (int ii = 0; ii < getRegionCount(); ii++) {
                getRegion(ii).invalidate();
            }
        }

        @Override
        public long getLong(final long elementIndex) {
            return lookupRegion(elementIndex).getLong(elementIndex);
        }

        @Override
        public long getLong(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getLong(context, elementIndex);
        }

        // region pushdown support

        @Override
        public List<RegionedPushdownAction> supportedActions() {
            return PageStorePushdownHelper.supportedActions(this);
        }

        @Override
        public long estimatePushdownAction(
                final RegionedPushdownAction action,
                final WhereFilter filter,
                final RowSet selection,
                final boolean usePrev,
                final PushdownFilterContext filterContext,
                final RegionedPushdownAction.EstimateContext estimateContext) {
            return PageStorePushdownHelper.estimatePushdownAction(
                    this, action, filter, selection, usePrev, filterContext, estimateContext);
        }

        @Override
        public PushdownResult performPushdownAction(
                final RegionedPushdownAction action,
                final WhereFilter filter,
                final RowSet selection,
                final PushdownResult input,
                final boolean usePrev,
                final PushdownFilterContext filterContext,
                final RegionedPushdownAction.ActionContext actionContext) {
            return PageStorePushdownHelper.performPushdownAction(
                    this, action, filter, selection, input, usePrev, filterContext, actionContext);
        }

        // endregion pushdown support
    }
}
