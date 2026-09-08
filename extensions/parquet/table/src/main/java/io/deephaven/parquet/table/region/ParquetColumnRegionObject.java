//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.region;

import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.table.impl.sources.regioned.kernel.ObjectRegionBinarySearchKernel;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.chunkattributes.DictionaryKeys;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.rowset.RowSet;
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
                    PushdownResult.REGION_SORTED_DATA_COST,
                    (ctx) -> ctx.rangeFilter() != null || ctx.matchFilter() != null,
                    (tl, cr) -> true);
    private static final List<RegionedPushdownAction> SUPPORTED_ACTIONS = List.of(SORTED_REGION_ACTION);

    private volatile Supplier<ColumnRegionLong<DictionaryKeys>> dictionaryKeysRegionSupplier;
    private volatile Supplier<ColumnRegionObject<DATA_TYPE, ATTR>> dictionaryValuesRegionSupplier;

    private ColumnRegionLong<DictionaryKeys> dictionaryKeysRegion;
    private ColumnRegionObject<DATA_TYPE, ATTR> dictionaryValuesRegion;

    public ParquetColumnRegionObject(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore,
            @NotNull final Supplier<ColumnRegionLong<DictionaryKeys>> dictionaryKeysRegionSupplier,
            @NotNull final Supplier<ColumnRegionObject<DATA_TYPE, ATTR>> dictionaryValuesRegionSupplier,
            @NotNull final ColumnLocation columnLocation) {
        super(columnChunkPageStore.mask(), columnChunkPageStore, columnLocation);
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
                // Can't push down case-insensitive match filters to binary search.
                if (ctx.matchFilter() != null && ctx.matchFilter().getMatchOptions().caseInsensitive()) {
                    return PushdownResult.UNSUPPORTED_ACTION_COST;
                }
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

        // Can't push down case-insensitive match filters to binary search.
        if (ctx.matchFilter() != null && ctx.matchFilter().getMatchOptions().caseInsensitive()) {
            return input.copy();
        }

        if (ctx.matchFilter() != null) {
            final MatchFilter matchFilter = ctx.matchFilter();
            try (final RowSet matches = ObjectRegionBinarySearchKernel.binarySearchMatch(
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

        if (ctx.rangeFilter() instanceof SingleSidedComparableRangeFilter) {
            final SingleSidedComparableRangeFilter rangeFilter =
                    (SingleSidedComparableRangeFilter) ctx.rangeFilter();
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
                        matches.intersect(selection),
                        RowSetFactory.empty());
            }
        }

        if (ctx.rangeFilter() instanceof ComparableRangeFilter) {
            final ComparableRangeFilter comparableRangeFilter = (ComparableRangeFilter) ctx.rangeFilter();
            final RowSet matches = ObjectRegionBinarySearchKernel.binarySearchMinMax(
                    this,
                    selection.firstRowKey(),
                    selection.lastRowKey(),
                    firstSortedColumn,
                    comparableRangeFilter.getLower(),
                    comparableRangeFilter.getUpper(),
                    comparableRangeFilter.isLowerInclusive(),
                    comparableRangeFilter.isUpperInclusive());
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
