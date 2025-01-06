//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.TableLocationFactory;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;

/**
 * <p>
 * Manually refreshing {@link TableLocationProvider} implementation that delegates {@link TableLocationKey location key}
 * discovery to a {@link TableLocationKeyFinder} and {@link TableLocation location} creation to a
 * {@link TableLocationFactory}.
 * </p>
 */
public class IcebergManualRefreshTableLocationProvider<TK extends TableKey, TLK extends TableLocationKey>
        extends IcebergTableLocationProviderBase<TK, TLK> {

    private static final String IMPLEMENTATION_NAME = IcebergManualRefreshTableLocationProvider.class.getSimpleName();

    public IcebergManualRefreshTableLocationProvider(
            @NotNull final TK tableKey,
            @NotNull final IcebergBaseLayout locationKeyFinder,
            @NotNull final TableLocationFactory<TK, TLK> locationFactory,
            @NotNull final IcebergTableAdapter adapter,
            @NotNull final TableIdentifier tableIdentifier) {
        super(tableKey,
                locationKeyFinder,
                locationFactory,
                true,
                adapter,
                tableIdentifier,
                TableUpdateMode.ADD_REMOVE, // New locations can be added and removed
                TableUpdateMode.STATIC // Individual locations cannot add or remove rows
        );
    }

    // ------------------------------------------------------------------------------------------------------------------
    // AbstractTableLocationProvider implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public void refresh() {
        ensureInitialized();
    }

    @Override
    public synchronized void update() {
        if (locationKeyFinder.maybeUpdateSnapshot()) {
            refreshLocations();
        }
    }

    @Override
    public synchronized void update(final long snapshotId) {
        // delegate to the locationKeyFinder to update the snapshot
        locationKeyFinder.updateSnapshot(snapshotId);
        refreshLocations();
    }

    @Override
    public synchronized void update(final Snapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("Input snapshot cannot be null");
        }
        // Update the snapshot.
        locationKeyFinder.updateSnapshot(snapshot);
        refreshLocations();
    }

    @Override
    protected synchronized void doInitialization() {
        if (!isInitialized()) {
            refreshLocations();
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // SubscriptionAggregator implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    protected void activateUnderlyingDataSource() {
        ensureInitialized();
        activationSuccessful(this);
    }

    @Override
    protected void deactivateUnderlyingDataSource() {
        // NOP for manually refreshing Iceberg table location provider.
    }

    @Override
    protected <T> boolean matchSubscriptionToken(T token) {
        return token == this;
    }
}
