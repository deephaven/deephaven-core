//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.region;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectRegionBinarySearchKernel;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.chunkattributes.DictionaryKeys;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.MustBeInvokedByOverriders;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Supplier;

/**
 * {@link ColumnRegionObject} implementation for regions that support fetching objects from {@link ColumnChunkPageStore
 * column chunk page stores}.
 */
public final class ParquetColumnRegionObject<DATA_TYPE, ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionObject<DATA_TYPE, ATTR>, ParquetColumnRegion<ATTR> {

    private static final RegionedPushdownAction.Region SORTED_REGION_ACTION =
            new RegionedPushdownAction.Region(
                    () -> QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION,
                    PushdownResult.DICTIONARY_DATA_COST,
                    (ctx) -> ctx.isMatchFilter() || ctx.isRangeFilter(),
                    (tl) -> true,
                    (cr) -> true);
    private static final List<RegionedPushdownAction> SUPPORTED_ACTIONS = List.of(SORTED_REGION_ACTION);

    private volatile Supplier<ColumnRegionLong<DictionaryKeys>> dictionaryKeysRegionSupplier;
    private volatile Supplier<ColumnRegionObject<DATA_TYPE, ATTR>> dictionaryValuesRegionSupplier;

    private ColumnRegionLong<DictionaryKeys> dictionaryKeysRegion;
    private ColumnRegionObject<DATA_TYPE, ATTR> dictionaryValuesRegion;

    public ParquetColumnRegionObject(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore,
            @NotNull final Supplier<ColumnRegionLong<DictionaryKeys>> dictionaryKeysRegionSupplier,
            @NotNull final Supplier<ColumnRegionObject<DATA_TYPE, ATTR>> dictionaryValuesRegionSupplier) {
        super(columnChunkPageStore.mask(), columnChunkPageStore);
        this.dictionaryKeysRegionSupplier = dictionaryKeysRegionSupplier;
        this.dictionaryValuesRegionSupplier = dictionaryValuesRegionSupplier;
    }

    public DATA_TYPE getObject(final long rowKey) {
        final ChunkPage<ATTR> page = getChunkPageContaining(rowKey);
        try {
            return page.<DATA_TYPE>asObjectChunk().get(page.getChunkOffset(rowKey));
        } catch (Exception e) {
            throw new TableDataException(
                    "Error retrieving object at Object row key " + rowKey + " from a parquet table", e);
        }
    }

    @Override
    public RegionVisitResult supportsDictionaryFormat(@NotNull final RowSet.SearchIterator keysToVisit) {
        if (!columnChunkPageStore.usesDictionaryOnEveryPage()) {
            return RegionVisitResult.FAILED;
        }
        return advanceToNextPage(keysToVisit) ? RegionVisitResult.CONTINUE : RegionVisitResult.COMPLETE;
    }

    @Override
    public ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
        if (dictionaryKeysRegionSupplier != null) {
            synchronized (this) {
                if (dictionaryKeysRegionSupplier != null) {
                    dictionaryKeysRegion = dictionaryKeysRegionSupplier.get();
                    dictionaryKeysRegionSupplier = null;
                }
            }
        }
        return dictionaryKeysRegion;
    }

    @Override
    public ColumnRegionObject<DATA_TYPE, ATTR> getDictionaryValuesRegion() {
        if (dictionaryValuesRegionSupplier != null) {
            synchronized (this) {
                if (dictionaryValuesRegionSupplier != null) {
                    dictionaryValuesRegion = dictionaryValuesRegionSupplier.get();
                    dictionaryValuesRegionSupplier = null;
                }
            }
        }
        return dictionaryValuesRegion;
    }

    @Override
    public List<RegionedPushdownAction> supportedActions() {
        return SUPPORTED_ACTIONS;
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
        final RegionedPushdownFilterLocationContext ctx = (RegionedPushdownFilterLocationContext) filterContext;
        for (RegionedPushdownAction action : actions) {
            if (action.equals(SORTED_REGION_ACTION)) {
                if (!ctx.isMatchFilter() && !ctx.isRangeFilter()) {
                    continue;
                }

                // Only range and match filers can benefit from sorted column data.
                final SortColumn firstSortedColumn = ctx.tableLocation().getSortedColumns().isEmpty()
                        ? null
                        : ctx.tableLocation().getSortedColumns().get(0);
                if (firstSortedColumn != null && firstSortedColumn.column().name().equals(filter.getColumns().get(0))) {
                    return action.filterCost();
                }
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
        final RegionedPushdownFilterLocationContext ctx = (RegionedPushdownFilterLocationContext) filterContext;

        if (action.equals(SORTED_REGION_ACTION)) {
            final SortColumn firstSortedColumn = ctx.tableLocation().getSortedColumns().isEmpty()
                    ? null
                    : ctx.tableLocation().getSortedColumns().get(0);
            if (firstSortedColumn == null || (!ctx.isMatchFilter() && !ctx.isRangeFilter()) ||
                    !firstSortedColumn.column().name().equals(filter.getColumns().get(0))) {
                return input.copy();
            }

            // We will use the effective filter from the context, which may bypass row tracking but provides the
            // raw filter that we need to apply to the sorted column.
            final WhereFilter effectiveFilter = ctx.filter();

            if (ctx.isMatchFilter()) {
                final MatchFilter matchFilter = (MatchFilter) effectiveFilter;
                try (final RowSet matches = ObjectRegionBinarySearchKernel.binarySearchMatch(
                        this,
                        selection.firstRowKey(),
                        selection.lastRowKey(),
                        firstSortedColumn,
                        matchFilter.getValues())) {
                    return PushdownResult.of(selection, matches.intersect(selection), RowSetFactory.empty());
                }
            }

            if (ctx.isRangeFilter() && effectiveFilter instanceof RangeFilter
                    && ((RangeFilter) effectiveFilter).getRealFilter() instanceof SingleSidedComparableRangeFilter) {
                final SingleSidedComparableRangeFilter rangeFilter =
                        (SingleSidedComparableRangeFilter) ((RangeFilter) effectiveFilter).getRealFilter();
                final RowSet matches;
                if (rangeFilter.isGreaterThan()) {
                    // Only need to find the lower bound, as the upper bound includes all values.
                    matches = ObjectRegionBinarySearchKernel.binarySearchMin(
                            this,
                            selection.firstRowKey(),
                            selection.lastRowKey(),
                            firstSortedColumn,
                            rangeFilter.getPivot(),
                            rangeFilter.isLowerInclusive());
                } else {
                    // Only need to find the upper bound, as the lower bound includes all values.
                    matches = ObjectRegionBinarySearchKernel.binarySearchMax(
                            this,
                            selection.firstRowKey(),
                            selection.lastRowKey(),
                            firstSortedColumn,
                            rangeFilter.getPivot(),
                            rangeFilter.isUpperInclusive());
                }
                try (final RowSet ignored = matches) {
                    return PushdownResult.of(
                            selection,
                            selection.subSetByKeyRange(matches.firstRowKey(), matches.lastRowKey()),
                            RowSetFactory.empty());
                }
            }
            return input.copy();
        }
        return input.copy();
    }
}
