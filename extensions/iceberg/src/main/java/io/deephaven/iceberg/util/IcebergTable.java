//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.SourceTableComponentFactory;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.iceberg.layout.IcebergRefreshingTableLocationProvider;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Table wrapper for refreshing Iceberg tables.
 */
public class IcebergTable extends PartitionAwareSourceTable {
    private final TableIdentifier tableIdentifier;
    private final IcebergCatalogAdapter adapter;
    /**
     * Location discovery.
     */
    final IcebergRefreshingTableLocationProvider<TableKey, IcebergTableLocationKey> locationProvider;

    /**
     *
     *
     * @param tableDefinition A TableDefinition
     * @param description A human-readable description for this table
     * @param componentFactory A component factory for creating column source managers
     * @param locationProvider A TableLocationProvider, for use in discovering the locations that compose this table
     * @param updateSourceRegistrar Callback for registering live tables for refreshes, null if this table is not live
     */
    IcebergTable(
            @NotNull TableIdentifier tableIdentifier,
            @NotNull IcebergCatalogAdapter adapter,
            @NotNull TableDefinition tableDefinition,
            @NotNull String description,
            @NotNull SourceTableComponentFactory componentFactory,
            @NotNull IcebergRefreshingTableLocationProvider<TableKey, IcebergTableLocationKey> locationProvider,
            @Nullable UpdateSourceRegistrar updateSourceRegistrar) {
        super(tableDefinition, description, componentFactory, locationProvider, updateSourceRegistrar);
        this.tableIdentifier = tableIdentifier;
        this.adapter = adapter;
        this.locationProvider = locationProvider;
    }

    public void update() {
        // Find the latest snapshot.
        final List<Snapshot> snapshots = adapter.listSnapshots(tableIdentifier);
        update(snapshots.get(snapshots.size() - 1));
    }

    public void update(final long snapshotId) {
        // Find the snapshot with the given snapshot id
        final Snapshot tableSnapshot = adapter.getSnapshot(tableIdentifier, snapshotId);
        if (tableSnapshot == null) {
            throw new IllegalArgumentException("Snapshot with id " + snapshotId + " not found");
        }

        update(tableSnapshot);
    }

    public void update(final @NotNull Snapshot snapshot) {
        // Call the update function, this
        locationProvider.update(snapshot);
    }
}
