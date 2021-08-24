/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.ColumnDefinition;

/**
 * Factory for source table components.
 */
public interface SourceTableComponentFactory {

    ColumnSourceManager createColumnSourceManager(
        boolean isRefreshing,
        ColumnToCodecMappings codecMappings,
        ColumnDefinition<?>... columnDefinitions);
}
