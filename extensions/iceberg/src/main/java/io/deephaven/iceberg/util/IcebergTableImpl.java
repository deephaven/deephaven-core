//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.SourceTableComponentFactory;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.iceberg.layout.IcebergTableLocationProviderBase;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import org.apache.iceberg.Snapshot;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table implementation for static and refreshing Iceberg tables.
 */
public class IcebergTableImpl extends PartitionAwareSourceTable implements IcebergTable {
    /**
     * Location discovery.
     */
    final IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> locationProvider;

    /**
     * Create an instance of the class with the provided parameters.
     *
     * @param tableDefinition The {@link TableDefinition} describing the table schema
     * @param description A human-readable description for this table
     * @param componentFactory A component factory for creating column source managers
     * @param locationProvider A {@link IcebergTableLocationProviderBase}, for use in discovering the locations that
     *        compose this table
     * @param updateSourceRegistrar Callback for registering live tables for refreshes, null if this table is not live
     */
    IcebergTableImpl(
            @NotNull TableDefinition tableDefinition,
            @NotNull String description,
            @NotNull SourceTableComponentFactory componentFactory,
            @NotNull IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> locationProvider,
            @Nullable UpdateSourceRegistrar updateSourceRegistrar) {
        super(tableDefinition, description, componentFactory, locationProvider, updateSourceRegistrar);
        this.locationProvider = locationProvider;
    }

    @Override
    public void update() {
        locationProvider.update();
    }

    @Override
    public void update(final long snapshotId) {
        locationProvider.update(snapshotId);
    }

    @Override
    public void update(final @NotNull Snapshot snapshot) {
        locationProvider.update(snapshot);
    }
}
