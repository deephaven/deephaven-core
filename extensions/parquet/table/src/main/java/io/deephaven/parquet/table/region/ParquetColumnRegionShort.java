//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ParquetColumnRegionChar and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.region;

import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.select.ShortRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionShort;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPushdownAction;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPushdownFilterContext;
import io.deephaven.engine.table.impl.sources.regioned.kernel.ShortRegionBinarySearchKernel;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static io.deephaven.util.QueryConstants.MAX_SHORT;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * {@link ColumnRegionShort} implementation for regions that support fetching primitive shorts from
 * {@link ColumnChunkPageStore column chunk page stores}.
 */
public final class ParquetColumnRegionShort<ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionShort<ATTR>, ParquetColumnRegion<ATTR> {

    private static final RegionedPushdownAction.Region SORTED_REGION_ACTION =
            new RegionedPushdownAction.Region(
                    () -> QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION,
                    PushdownResult.REGION_SORTED_DATA_COST,
                    (ctx) -> ctx.rangeFilter() != null || ctx.matchFilter() != null,
                    (tl, cr) -> true);
    private static final List<RegionedPushdownAction> SUPPORTED_ACTIONS = List.of(SORTED_REGION_ACTION);

    public ParquetColumnRegionShort(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore,
            @NotNull final ColumnLocation columnLocation) {
        super(columnChunkPageStore.mask(), columnChunkPageStore, columnLocation);
    }
    // region getBytes
    // endregion getBytes

    @Override
    public short getShort(final long rowKey) {
        final ChunkPage<ATTR> page = getChunkPageContaining(rowKey);
        try {
            return page.asShortChunk().get(page.getChunkOffset(rowKey));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving short at row key " + rowKey + " from a parquet table", e);
        }
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
        // Current implementation only supports sorted region actions.
        if (!action.equals(SORTED_REGION_ACTION)) {
            return PushdownResult.UNSUPPORTED_ACTION_COST;
        }

        final RegionedPushdownFilterContext ctx = (RegionedPushdownFilterContext) filterContext;
        final TableLocation tableLocation = getColumnLocation().map(ColumnLocation::getTableLocation).orElse(null);

        // Only range and match filters can benefit from sorted column data.
        if (tableLocation == null || (ctx.rangeFilter() == null && ctx.matchFilter() == null)) {
            return PushdownResult.UNSUPPORTED_ACTION_COST;
        }
        final SortColumn firstSortedColumn = tableLocation.getSortedColumns().isEmpty()
                ? null
                : tableLocation.getSortedColumns().get(0);

        if (firstSortedColumn != null) {
            // Need to handle column renames.
            final String col = filter.getColumns().get(0);
            final String renamedCol = ctx.filterColumnToManagerColumnName().getOrDefault(col, col);
            if (firstSortedColumn.column().name().equals(renamedCol)) {
                return action.filterCost();
            }
        }
        return PushdownResult.UNSUPPORTED_ACTION_COST;
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
        // Current implementation only supports sorted region actions.
        if (!action.equals(SORTED_REGION_ACTION)) {
            return input.copy();
        }

        final RegionedPushdownFilterContext ctx = (RegionedPushdownFilterContext) filterContext;

        final TableLocation tableLocation = getColumnLocation().map(ColumnLocation::getTableLocation).orElse(null);

        // Only range and match filters can benefit from sorted column data.
        if (tableLocation == null || (ctx.rangeFilter() == null && ctx.matchFilter() == null)) {
            return input.copy();
        }
        final SortColumn firstSortedColumn = tableLocation.getSortedColumns().isEmpty()
                ? null
                : tableLocation.getSortedColumns().get(0);
        if (firstSortedColumn == null) {
            return input.copy();
        }

        // Need to handle column renames.
        final String col = filter.getColumns().get(0);
        final String renamedCol = ctx.filterColumnToManagerColumnName().getOrDefault(col, col);
        if (!firstSortedColumn.column().name().equals(renamedCol)) {
            return input.copy();
        }

        if (ctx.matchFilter() != null) {
            final MatchFilter matchFilter = ctx.matchFilter();
            try (final RowSet matches = ShortRegionBinarySearchKernel.binarySearchMatch(
                    this,
                    selection.firstRowKey(),
                    selection.lastRowKey(),
                    firstSortedColumn,
                    matchFilter.getValues())) {
                // Handle normal / inverted match filters:
                return PushdownResult.of(selection, matchFilter.getMatchOptions().inverted()
                        ? selection.minus(matches)
                        : matches.intersect(selection), RowSetFactory.empty());
            }
        }

        if (ctx.rangeFilter() != null && ctx.rangeFilter() instanceof ShortRangeFilter) {
            final ShortRangeFilter rangeFilter = (ShortRangeFilter) ctx.rangeFilter();
            final RowSet matches;
            if (rangeFilter.getLower() == NULL_SHORT && rangeFilter.isLowerInclusive()) {
                // Only need to find the upper bound, as the lower bound includes all values.
                matches = ShortRegionBinarySearchKernel.binarySearchMax(
                        this,
                        selection.firstRowKey(),
                        selection.lastRowKey(),
                        firstSortedColumn,
                        rangeFilter.getUpper(),
                        rangeFilter.isUpperInclusive());
            } else if (rangeFilter.getUpper() == MAX_SHORT && rangeFilter.isUpperInclusive()) {
                // Only need to find the lower bound, as the upper bound includes all values.
                matches = ShortRegionBinarySearchKernel.binarySearchMin(
                        this,
                        selection.firstRowKey(),
                        selection.lastRowKey(),
                        firstSortedColumn,
                        rangeFilter.getLower(),
                        rangeFilter.isLowerInclusive());
            } else {
                // Find the lower and upper bounds.
                matches = ShortRegionBinarySearchKernel.binarySearchMinMax(
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
                        matches.intersect(selection),
                        RowSetFactory.empty());
            }
        }
        return input.copy();
    }
}
