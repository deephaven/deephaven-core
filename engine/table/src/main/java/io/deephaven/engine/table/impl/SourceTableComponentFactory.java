//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.TableDefinition;

/**
 * Factory for source table components.
 */
public interface SourceTableComponentFactory {

    ColumnSourceManager createColumnSourceManager(
            boolean isRefreshing,
            boolean removeAllowed,
            ColumnToCodecMappings codecMappings,
            TableDefinition tableDefinition);
}
