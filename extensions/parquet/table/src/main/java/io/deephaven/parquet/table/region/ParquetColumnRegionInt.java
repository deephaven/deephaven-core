//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ParquetColumnRegionChar and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.region;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.select.IntRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.RangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionInt;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPushdownAction;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPushdownFilterLocationContext;
import io.deephaven.engine.table.impl.sources.regioned.kernel.IntRegionBinarySearchKernel;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import org.jetbrains.annotations.MustBeInvokedByOverriders;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static io.deephaven.util.QueryConstants.MAX_INT;
import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * {@link ColumnRegionInt} implementation for regions that support fetching primitive ints from
 * {@link ColumnChunkPageStore column chunk page stores}.
 */
public final class ParquetColumnRegionInt<ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionInt<ATTR>, ParquetColumnRegion<ATTR> {

    private static final RegionedPushdownAction.Region SORTED_REGION_ACTION =
            new RegionedPushdownAction.Region(
                    () -> QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION,
                    PushdownResult.DICTIONARY_DATA_COST,
                    (ctx) -> ctx.isMatchFilter() || ctx.isRangeFilter(),
                    (tl) -> true,
                    (cr) -> true);
    private static final List<RegionedPushdownAction> SUPPORTED_ACTIONS = List.of(SORTED_REGION_ACTION);

    public ParquetColumnRegionInt(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(columnChunkPageStore.mask(), columnChunkPageStore);
    }

    // region getBytes
    // endregion getBytes

    @Override
    public int getInt(final long rowKey) {
        final ChunkPage<ATTR> page = getChunkPageContaining(rowKey);
        try {
            return page.asIntChunk().get(page.getChunkOffset(rowKey));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving int at row key " + rowKey + " from a parquet table", e);
        }
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
                try (final RowSet matches = IntRegionBinarySearchKernel.binarySearchMatch(
                        this,
                        selection.firstRowKey(),
                        selection.lastRowKey(),
                        firstSortedColumn,
                        matchFilter.getValues())) {
                    return PushdownResult.of(selection, matches.intersect(selection), RowSetFactory.empty());
                }
            }

            if (ctx.isRangeFilter() && effectiveFilter instanceof RangeFilter
                    && ((RangeFilter) effectiveFilter).getRealFilter() instanceof IntRangeFilter) {
                final IntRangeFilter rangeFilter = (IntRangeFilter) ((RangeFilter) effectiveFilter).getRealFilter();
                final RowSet matches;
                if (rangeFilter.getLower() == NULL_INT && rangeFilter.isLowerInclusive()) {
                    // Only need to find the upper bound, as the lower bound includes all values.
                    matches = IntRegionBinarySearchKernel.binarySearchMax(
                            this,
                            selection.firstRowKey(),
                            selection.lastRowKey(),
                            firstSortedColumn,
                            rangeFilter.getUpper(),
                            rangeFilter.isUpperInclusive());
                } else if (rangeFilter.getUpper() == MAX_INT && rangeFilter.isUpperInclusive()) {
                    // Only need to find the lower bound, as the upper bound includes all values.
                    matches = IntRegionBinarySearchKernel.binarySearchMin(
                            this,
                            selection.firstRowKey(),
                            selection.lastRowKey(),
                            firstSortedColumn,
                            rangeFilter.getLower(),
                            rangeFilter.isLowerInclusive());
                } else {
                    // Find the lower and upper bounds.
                    matches = IntRegionBinarySearchKernel.binarySearchMinMax(
                        this,
                        selection.firstRowKey(),
                        selection.lastRowKey(),
                        firstSortedColumn,
                        rangeFilter.getLower(),
                        rangeFilter.getUpper(),
                        rangeFilter.isLowerInclusive(),
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
