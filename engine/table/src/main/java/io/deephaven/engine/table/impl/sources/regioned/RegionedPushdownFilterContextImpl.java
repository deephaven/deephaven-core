//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.BasePushdownFilterContextImpl;
import io.deephaven.engine.table.impl.select.WhereFilter;

import java.util.List;
import java.util.Map;

/**
 * A pushdown filter context for regioned column sources that handles column name mappings and definitions.
 */
public class RegionedPushdownFilterContextImpl extends BasePushdownFilterContextImpl
        implements RegionedPushdownFilterContext {
    private final List<ColumnDefinition<?>> columnDefinitions;
    private final Map<String, String> filterColumnToManagerColumnName;
    private final boolean supportsSortedDataFiltering;

    public RegionedPushdownFilterContextImpl(
            final WhereFilter filter,
            final List<ColumnSource<?>> columnSources,
            final List<ColumnDefinition<?>> columnDefinitions,
            final Map<String, String> filterColumnToManagerColumnName) {
        super(filter, columnSources);
        this.columnDefinitions = columnDefinitions;
        this.filterColumnToManagerColumnName = filterColumnToManagerColumnName;
        // Computed once per filter rather than for every region that asks.
        this.supportsSortedDataFiltering = RegionedPushdownFilterContext.super.supportsSortedDataFiltering();
    }

    public List<ColumnDefinition<?>> columnDefinitions() {
        return columnDefinitions;
    }

    public Map<String, String> filterColumnToManagerColumnName() {
        return filterColumnToManagerColumnName;
    }

    @Override
    public boolean supportsSortedDataFiltering() {
        return supportsSortedDataFiltering;
    }
}
