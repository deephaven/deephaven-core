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
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.MustBeInvokedByOverriders;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Column region interface for regions that support fetching primitive floats.
 */
public interface ColumnRegionFloat<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single float from this region.
     *
     * @param elementIndex Element row key in the table's address space
     * @return The float value at the specified element row key
     */
    float getFloat(long elementIndex);

    /**
     * Get a single float from this region.
     *
     * @param context A {@link PagingContextHolder} to enable resource caching where suitable, with current region index
     *        pointing to this region
     * @param elementIndex Element row key in the table's address space
     * @return The float value at the specified element row key
     */
    default float getFloat(@NotNull final FillContext context, final long elementIndex) {
        return getFloat(elementIndex);
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Float;
    }

    static <ATTR extends Any> ColumnRegionFloat<ATTR> createNull(final long pageMask) {
        // noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<ATTR>(pageMask);
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionFloat<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionFloat DEFAULT_INSTANCE =
                new ColumnRegionFloat.Null(RegionedColumnSourceBase.PARAMETERS.regionMask);

        public Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public float getFloat(final long elementIndex) {
            return QueryConstants.NULL_FLOAT;
        }
    }

    final class Constant<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegionFloat<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        final static RegionedPushdownAction.Region ConstantColumnRegion =
                new RegionedPushdownAction.Region(
                        () -> false,
                        PushdownResult.SINGLE_VALUE_REGION_COST,
                        (ctx) -> true,
                        (tl) -> true,
                        (cr) -> cr instanceof Constant);
        private static final List<RegionedPushdownAction> supportedActions = List.of(ConstantColumnRegion);

        private final float value;

        public Constant(final long pageMask, final float value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public float getFloat(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableFloatChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }

        @Override
        public List<RegionedPushdownAction> supportedActions() {
            return supportedActions;
        }

        @Override
        @MustBeInvokedByOverriders
        public long estimatePushdownAction(
                final List<RegionedPushdownAction> actions,
                final WhereFilter filter,
                final RowSet selection,
                final boolean usePrev,
                final PushdownFilterContext filterContext,
                final RegionedPushdownAction.EstimateContext estimateContext) {
            for (RegionedPushdownAction action : actions) {
                // Only ConstantColumnRegion is supported by this class.
                if (action == ConstantColumnRegion) {
                    return ConstantColumnRegion.filterCost();
                }
            }
            return Long.MAX_VALUE;
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
            // Only ConstantColumnRegion is supported by this class.
            if (action != ConstantColumnRegion) {
                return input.copy();
            }
            final BasePushdownFilterContext filterCtx = (BasePushdownFilterContext) filterContext;

            final boolean matches;
            if (value == QueryConstants.NULL_FLOAT) {
                final BasePushdownFilterContext.FilterNullBehavior nullBehavior = filterCtx.filterNullBehavior();
                if (nullBehavior == BasePushdownFilterContext.FilterNullBehavior.FAILS_ON_NULLS) {
                    // Bad-behaving filter, but not our responsibility to handle during pushdown.
                    return input.copy();
                }
                matches = nullBehavior == BasePushdownFilterContext.FilterNullBehavior.INCLUDES_NULLS;
            } else {
                if (filterCtx.supportsChunkFiltering()) {
                    matches = SingleValuePushdownHelper.chunkFilter(selection, filterCtx,
                            () -> SingleValuePushdownHelper.makeChunk(value));
                } else {
                    final ColumnSource<?> columnSource =
                            InMemoryColumnSource.makeImmutableConstantSource(float.class, null, value);
                    matches = SingleValuePushdownHelper.tableFilter(filter, selection, false, columnSource);
                }
            }
            return matches
                    // Promote all maybe rows to match.
                    ? PushdownResult.of(selection, input.match().union(input.maybeMatch()), RowSetFactory.empty())
                    // None of these rows match, return the original match rows.
                    : PushdownResult.of(selection, input.match(), RowSetFactory.empty());
        }
    }

    final class StaticPageStore<ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionFloat<ATTR>>
            implements ColumnRegionFloat<ATTR> {

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionFloat<ATTR>[] regions) {
            super(parameters, regions);
        }

        @Override
        public void invalidate() {
            for (int ii = 0; ii < getRegionCount(); ii++) {
                getRegion(ii).invalidate();
            }
        }

        @Override
        public float getFloat(final long elementIndex) {
            return lookupRegion(elementIndex).getFloat(elementIndex);
        }

        @Override
        public float getFloat(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getFloat(context, elementIndex);
        }
    }
}
