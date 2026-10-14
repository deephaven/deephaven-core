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
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.select.IntRangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionInt;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPushdownAction;
import io.deephaven.engine.table.impl.sources.regioned.RegionedPushdownFilterContext;
import io.deephaven.engine.table.impl.sources.regioned.SortedRegionPushdownHelper;
import io.deephaven.engine.table.impl.sources.regioned.kernel.IntRegionBinarySearchKernel;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * {@link ColumnRegionInt} implementation for regions that support fetching primitive ints from
 * {@link ColumnChunkPageStore column chunk page stores}.
 */
public final class ParquetColumnRegionInt<ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionInt<ATTR>, ParquetColumnRegion<ATTR> {

    private static final RegionedPushdownAction.Region SORTED_REGION_ACTION =
            new RegionedPushdownAction.Region(
                    () -> QueryTable.DISABLE_WHERE_PUSHDOWN_SORTED_COLUMN_LOCATION,
                    PushdownResult.REGION_SORTED_DATA_COST,
                    (ctx) -> ctx.rangeFilter() != null || ctx.matchFilter() != null,
                    (tl, cr) -> true);
    private static final List<RegionedPushdownAction> SUPPORTED_ACTIONS = List.of(SORTED_REGION_ACTION);

    public ParquetColumnRegionInt(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore,
            @NotNull final ColumnLocation columnLocation) {
        super(columnChunkPageStore.mask(), columnChunkPageStore, columnLocation);
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

        final SortColumn sortColumn = SortedRegionPushdownHelper.searchableSortColumn(
                filter,
                (RegionedPushdownFilterContext) filterContext,
                getColumnLocation().map(ColumnLocation::getTableLocation).orElse(null));
        if (sortColumn == null) {
            return PushdownResult.UNSUPPORTED_ACTION_COST;
        }
        return action.filterCost();
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
        if (selection.isEmpty()) {
            // Nothing can match an empty selection.
            return PushdownResult.noneMatch(selection);
        }

        // Current implementation only supports sorted region actions.
        if (!action.equals(SORTED_REGION_ACTION)) {
            return input.copy();
        }

        final RegionedPushdownFilterContext ctx = (RegionedPushdownFilterContext) filterContext;
        final SortColumn sortColumn = SortedRegionPushdownHelper.searchableSortColumn(
                filter,
                ctx,
                getColumnLocation().map(ColumnLocation::getTableLocation).orElse(null));
        if (sortColumn == null) {
            return input.copy();
        }
        final long firstKey = selection.firstRowKey();
        final long lastKey = selection.lastRowKey();

        if (ctx.matchFilter() != null) {
            try (final RowSet matches = IntRegionBinarySearchKernel.binsearchMatchFilter(
                    this, firstKey, lastKey, sortColumn, ctx.matchFilter());
                    // Handle normal / inverted match filters:
                    final RowSet pushdownMatches = ctx.matchFilter().getMatchOptions().inverted()
                            ? selection.minus(matches)
                            : matches.intersect(selection)) {
                return PushdownResult.exactMatch(selection, pushdownMatches);
            }
        }

        if (ctx.rangeFilter() instanceof IntRangeFilter) {
            try (final RowSet matches = IntRegionBinarySearchKernel.binsearchRangeFilter(
                    this, firstKey, lastKey, sortColumn, (IntRangeFilter) ctx.rangeFilter());
                    final RowSet pushdownMatches = matches.intersect(selection)) {
                return PushdownResult.exactMatch(selection, pushdownMatches);
            }
        }
        return input.copy();
    }
}
