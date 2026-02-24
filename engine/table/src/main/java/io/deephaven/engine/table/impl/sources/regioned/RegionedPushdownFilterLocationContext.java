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
 * A pushdown filter context for regioned column sources that handles column name mappings and definitions. Additionally
 * provides access to the table location.
 */
public class RegionedPushdownFilterLocationContext extends RegionedPushdownFilterContext {
    private final TableLocation tableLocation;

    public RegionedPushdownFilterLocationContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> columnSources,
            final List<ColumnDefinition<?>> columnDefinitions,
            final Map<String, String> renameMap,
            final TableLocation tableLocation) {
        super(filter, columnSources, columnDefinitions, renameMap);
        this.tableLocation = tableLocation;
    }

    public TableLocation tableLocation() {
        return tableLocation;
    }
}
