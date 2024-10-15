//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.TableLocationFactory;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;

public abstract class IcebergTableLocationProviderBase<TK extends TableKey, TLK extends TableLocationKey>
        extends AbstractTableLocationProvider {

    final IcebergBaseLayout locationKeyFinder;
    final TableLocationFactory<TK, TLK> locationFactory;
    final IcebergTableAdapter adapter;
    final TableIdentifier tableIdentifier;

    public IcebergTableLocationProviderBase(
            @NotNull final TK tableKey,
            @NotNull final IcebergBaseLayout locationKeyFinder,
            @NotNull final TableLocationFactory<TK, TLK> locationFactory,
            final boolean isRefreshing,
            @Nullable final IcebergTableAdapter adapter,
            @NotNull final TableIdentifier tableIdentifier,
            final TableUpdateMode updateMode,
            final TableUpdateMode locationUpdateMode) {
        super(tableKey, isRefreshing, updateMode, locationUpdateMode);
        this.locationKeyFinder = locationKeyFinder;
        this.locationFactory = locationFactory;
        this.adapter = adapter;
        this.tableIdentifier = tableIdentifier;
    }

    /**
     * Update a manually refreshing table location provider with the latest snapshot from the catalog. This will throw
     * an {@link UnsupportedOperationException} if the table is not manually refreshing.
     */
    public abstract void update();

    /**
     * Update a manually refreshing table location provider with a specific snapshot from the catalog. If the
     * {@code snapshotId} is not found in the list of snapshots for the table, an {@link IllegalArgumentException} is
     * thrown. The input snapshot must also be newer (higher in sequence number) than the current snapshot or an
     * {@link IllegalArgumentException} is thrown. This will throw an {@link UnsupportedOperationException} if the table
     * is not manually refreshing.
     *
     * @param snapshotId The identifier of the snapshot to use when updating the table.
     */
    public abstract void update(final long snapshotId);

    /**
     * Update a manually refreshing table location provider with a specific snapshot from the catalog. The input
     * snapshot must be newer (higher in sequence number) than the current snapshot or an
     * {@link IllegalArgumentException} is thrown. This will throw an {@link UnsupportedOperationException} if the table
     * is not manually refreshing.
     * 
     * @param snapshot The snapshot to use when updating the table.
     */
    public abstract void update(final Snapshot snapshot);

    @Override
    @NotNull
    protected TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
        // noinspection unchecked
        return locationFactory.makeLocation((TK) getKey(), (TLK) locationKey, null);
    }

    /**
     * Refresh the table location provider with the latest snapshot from the catalog. This method will identify new
     * locations and removed locations.
     */
    protected void refreshLocations() {
        final Object token = new Object();
        beginTransaction(token);
        final Set<ImmutableTableLocationKey> missedKeys = new HashSet<>();
        getTableLocationKeys(ttlk -> missedKeys.add(ttlk.get()));
        locationKeyFinder.findKeys(tlk -> {
            missedKeys.remove(tlk);
            handleTableLocationKeyAdded(tlk, token);
        });
        missedKeys.forEach(tlk -> handleTableLocationKeyRemoved(tlk, token));
        endTransaction(token);
        setInitialized();
    }
}
