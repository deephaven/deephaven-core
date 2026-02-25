//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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
 * Column region interface for regions that support fetching primitive chars.
 */
public interface ColumnRegionChar<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single char from this region.
     *
     * @param elementIndex Element row key in the table's address space
     * @return The char value at the specified element row key
     */
    char getChar(long elementIndex);

    /**
     * Get a single char from this region.
     *
     * @param context A {@link PagingContextHolder} to enable resource caching where suitable, with current region index
     *        pointing to this region
     * @param elementIndex Element row key in the table's address space
     * @return The char value at the specified element row key
     */
    default char getChar(@NotNull final FillContext context, final long elementIndex) {
        return getChar(elementIndex);
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Char;
    }

    static <ATTR extends Any> ColumnRegionChar<ATTR> createNull(final long pageMask) {
        // noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<ATTR>(pageMask);
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionChar<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionChar DEFAULT_INSTANCE =
                new ColumnRegionChar.Null(RegionedColumnSourceBase.PARAMETERS.regionMask);

        public Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public char getChar(final long elementIndex) {
            return QueryConstants.NULL_CHAR;
        }
    }

    final class Constant<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegionChar<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        final static RegionedPushdownAction.Region ConstantColumnRegion =
                new RegionedPushdownAction.Region(
                        () -> false,
                        PushdownResult.SINGLE_VALUE_REGION_COST,
                        (ctx) -> true,
                        (tl) -> true,
                        (cr) -> cr instanceof Constant);
        private static final List<RegionedPushdownAction> supportedActions = List.of(ConstantColumnRegion);

        private final char value;

        public Constant(final long pageMask, final char value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public char getChar(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableCharChunk().fillWithValue(offset, length, value);
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
            if (value == QueryConstants.NULL_CHAR) {
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
                            InMemoryColumnSource.makeImmutableConstantSource(char.class, null, value);
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
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionChar<ATTR>>
            implements ColumnRegionChar<ATTR> {

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionChar<ATTR>[] regions) {
            super(parameters, regions);
        }

        @Override
        public void invalidate() {
            for (int ii = 0; ii < getRegionCount(); ii++) {
                getRegion(ii).invalidate();
            }
        }

        @Override
        public char getChar(final long elementIndex) {
            return lookupRegion(elementIndex).getChar(elementIndex);
        }

        @Override
        public char getChar(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getChar(context, elementIndex);
        }
    }
}
