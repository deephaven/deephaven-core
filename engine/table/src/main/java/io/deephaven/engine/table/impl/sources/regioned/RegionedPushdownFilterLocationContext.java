//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.select.WhereFilter;

import java.util.List;
import java.util.Map;

/**
 * A wrapper class for RegionedPushdownFilterContext that additionally provides access to a table location.
 */
public class RegionedPushdownFilterLocationContext implements RegionedPushdownFilterContext {
    private final RegionedPushdownFilterContext wrapped;
    private final TableLocation tableLocation;

    public RegionedPushdownFilterLocationContext(
            final RegionedPushdownFilterContext wrapped,
            final TableLocation tableLocation) {
        this.wrapped = wrapped;
        this.tableLocation = tableLocation;
    }

    @Override
    public void close() {
        // Not releasing or modifying the wrapped context.
    }

    public TableLocation tableLocation() {
        return tableLocation;
    }

    @Override
    public WhereFilter filter() {
        return wrapped.filter();
    }

    @Override
    public List<ColumnSource<?>> columnSources() {
        return wrapped.columnSources();
    }

    @Override
    public boolean supportsChunkFiltering() {
        return wrapped.supportsChunkFiltering();
    }

    @Override
    public boolean supportsMetadataFiltering() {
        return wrapped.supportsMetadataFiltering();
    }

    @Override
    public boolean supportsInMemoryDataIndexFiltering() {
        return wrapped.supportsInMemoryDataIndexFiltering();
    }

    @Override
    public boolean supportsDeferredDataIndexFiltering() {
        return wrapped.supportsDeferredDataIndexFiltering();
    }

    @Override
    public WhereFilter filterForMetadataFiltering() {
        return wrapped.filterForMetadataFiltering();
    }

    @Override
    public FilterNullBehavior filterNullBehavior() {
        return wrapped.filterNullBehavior();
    }

    @Override
    public UnifiedChunkFilter createChunkFilter(int maxChunkSize) {
        return wrapped.createChunkFilter(maxChunkSize);
    }

    @Override
    public long executedFilterCost() {
        return wrapped.executedFilterCost();
    }

    @Override
    public void updateExecutedFilterCost(long executedFilterCost) {
        // These wrapped context are transient, should not be called.
        throw new UnsupportedOperationException("Should not update executed filter cost on wrapped context");
    }

    @Override
    public List<ColumnDefinition<?>> columnDefinitions() {
        return wrapped.columnDefinitions();
    }

    @Override
    public Map<String, String> renameMap() {
        return wrapped.renameMap();
    }

    @Override
    public RegionedPushdownFilterLocationContext withTableLocation(TableLocation tableLocation) {
        return null;
    }
}
