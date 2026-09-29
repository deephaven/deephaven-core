//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.sort.SortedColumnPushdownManager;

import java.util.List;
import java.util.Map;

/**
 * A pushdown filter context for regioned column sources that handles column name mappings and definitions.
 */
public interface RegionedPushdownFilterContext extends BasePushdownFilterContext {
    /**
     * Get the column definitions for the columns involved in this filter.
     */
    List<ColumnDefinition<?>> columnDefinitions();

    /**
     * Get the mapping from column names used in the filter to the actual column names in the manager.
     */
    Map<String, String> filterColumnToManagerColumnName();

    /**
     * Whether sorted-data (binary search) filtering may be used for this filter: it is a range or match filter, and
     * sorted-column pushdown is not blocked for it (DH-23750, see
     * {@link SortedColumnPushdownManager#isKnownIncorrectForSortedPushdown}). Implementations may compute this once.
     */
    default boolean supportsSortedDataFiltering() {
        return (rangeFilter() != null || matchFilter() != null)
                && !SortedColumnPushdownManager.isKnownIncorrectForSortedPushdown(
                        columnDefinitions().get(0).getDataType(), matchFilter(), rangeFilter());
    }
}
