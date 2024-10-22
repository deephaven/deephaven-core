//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

/**
 * Instructions for reading a {@link Table table} containing the {@link TableDefinition definition} corresponding to an
 * Iceberg table.
 */
@Value.Immutable
@BuildableStyle
public abstract class IcebergDefinitionTable extends IcebergReadOperationsBase {

    public static Builder builder() {
        return ImmutableIcebergDefinitionTable.builder();
    }

    public interface Builder
            extends IcebergReadOperationsBase.Builder<IcebergDefinitionTable, IcebergDefinitionTable.Builder> {
    }
}
