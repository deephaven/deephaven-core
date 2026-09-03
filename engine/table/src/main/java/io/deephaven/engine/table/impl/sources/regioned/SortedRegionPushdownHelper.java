//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.select.WhereFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Helpers for {@link ColumnRegion} implementations that answer range and match filters by binary search over a sorted
 * column.
 *
 * <p>
 * Resolving the location's sort column against the filter is the one step of that pushdown that does not depend on the
 * region's data type. The search itself is type-specific, and normally comes from the matching
 * {@code ...RegionBinarySearchKernel}; the result is assembled with {@link PushdownResult#exactMatch}. Any
 * region-backed format -- not just Parquet -- can therefore share this step instead of restating it.
 *
 * @see io.deephaven.engine.table.impl.sort.SortedColumnPushdownManager the equivalent for in-memory column sources
 */
public final class SortedRegionPushdownHelper {

    private SortedRegionPushdownHelper() {}

    /**
     * The sort column a binary search over {@code tableLocation} can use to answer {@code filter}, or {@code null} if
     * there is none. Callers should treat {@code null} as "cannot push this filter down": report
     * {@link PushdownResult#UNSUPPORTED_ACTION_COST} when estimating, and hand the rows on unresolved when acting.
     *
     * @param filter The filter being pushed down
     * @param ctx The pushdown filter context
     * @param tableLocation The location backing the region, or {@code null} if it has none
     * @return The sort column to search, or {@code null} if binary search cannot answer {@code filter}
     */
    @Nullable
    public static SortColumn searchableSortColumn(
            @NotNull final WhereFilter filter,
            @NotNull final RegionedPushdownFilterContext ctx,
            @Nullable final TableLocation tableLocation) {
        // Only range and match filters can benefit from sorted column data.
        if (tableLocation == null || (ctx.rangeFilter() == null && ctx.matchFilter() == null)) {
            return null;
        }
        // Binary search orders by value, so it cannot answer a case-insensitive match.
        if (ctx.matchFilter() != null && ctx.matchFilter().getMatchOptions().caseInsensitive()) {
            return null;
        }
        if (tableLocation.getSortedColumns().isEmpty()) {
            return null;
        }
        // Only the first sort column is searchable; the rest are sorted within its runs.
        final SortColumn firstSortedColumn = tableLocation.getSortedColumns().get(0);
        // Need to handle column renames.
        final String col = filter.getColumns().get(0);
        final String renamedCol = ctx.filterColumnToManagerColumnName().getOrDefault(col, col);
        return firstSortedColumn.column().name().equals(renamedCol) ? firstSortedColumn : null;
    }
}
