//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;

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

}
