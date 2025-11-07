//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.ColumnToCodecMappings;
import io.deephaven.engine.table.impl.SourceTableComponentFactory;

/**
 * Factory interface for regioned source table components.
 */
public interface RegionedTableComponentFactory extends SourceTableComponentFactory {

    @FunctionalInterface
    interface RegionedColumnSourceFactory {
        RegionedColumnSource<?> make(RegionedColumnSourceManager manager);
    }

    <DATA_TYPE> RegionedColumnSource<DATA_TYPE> createRegionedColumnSource(
            RegionedColumnSourceManager manager,
            ColumnDefinition<DATA_TYPE> columnDefinition,
            ColumnToCodecMappings codecMappings);
}
