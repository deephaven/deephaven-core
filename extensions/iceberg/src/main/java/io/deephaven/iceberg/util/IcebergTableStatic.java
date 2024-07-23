//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.SourceTableComponentFactory;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.iceberg.layout.IcebergTableLocationProviderBase;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import org.apache.iceberg.Snapshot;
import org.jetbrains.annotations.NotNull;

/**
 * Table wrapper for refreshing Iceberg tables.
 */
public class IcebergTableStatic extends PartitionAwareSourceTable implements IcebergTable {
    /**
     *
     *
     * @param tableDefinition A TableDefinition
     * @param description A human-readable description for this table
     * @param componentFactory A component factory for creating column source managers
     * @param locationProvider A TableLocationProvider, for use in discovering the locations that compose this table
     */
    IcebergTableStatic(
            @NotNull TableDefinition tableDefinition,
            @NotNull String description,
            @NotNull SourceTableComponentFactory componentFactory,
            @NotNull IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> locationProvider) {
        super(tableDefinition, description, componentFactory, locationProvider, null);
    }

    @Override
    public void update() {
        throw new IllegalStateException("Static Iceberg tables cannot be updated, table: " + description);
    }

    @Override
    public void update(final long snapshotId) {
        throw new IllegalStateException("Static Iceberg tables cannot be updated, table: " + description);
    }

    @Override
    public void update(final @NotNull Snapshot snapshot) {
        throw new IllegalStateException("Static Iceberg tables cannot be updated, table: " + description);
    }
}
