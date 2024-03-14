//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnDefinition;

import java.util.List;

/**
 * Factory for source table components.
 */
public interface SourceTableComponentFactory {

    ColumnSourceManager createColumnSourceManager(
            boolean isRefreshing,
            ColumnToCodecMappings codecMappings,
            List<ColumnDefinition<?>> columnDefinitions);
}
