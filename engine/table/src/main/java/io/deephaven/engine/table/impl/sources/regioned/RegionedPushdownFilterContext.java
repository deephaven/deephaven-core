//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.locations.TableLocation;

import java.util.List;
import java.util.Map;

/**
 * A pushdown filter context for regioned column sources that handles column name mappings and definitions.
 */
public interface RegionedPushdownFilterContext extends BasePushdownFilterContext {
    List<ColumnDefinition<?>> columnDefinitions();

    Map<String, String> renameMap();

    /**
     * Create a wrapper of this context with the given table location set to the supplied value.
     */
    RegionedPushdownFilterLocationContext withTableLocation(final TableLocation tableLocation);
}
