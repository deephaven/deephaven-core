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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    private boolean initialized = false;

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
    public synchronized void refresh() {
        // There should be no refresh service for this provider.
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void update() {
        adapter.refresh();
        update(adapter.currentSnapshot());
    }

    @Override
    public synchronized void update(final long snapshotId) {
        adapter.refresh();
        final List<Snapshot> snapshots = adapter.listSnapshots();

        final Snapshot snapshot = snapshots.stream()
                .filter(s -> s.snapshotId() == snapshotId).findFirst()
                .orElse(null);

        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "Snapshot " + snapshotId + " was not found in the list of snapshots for table " + tableIdentifier
                            + ". Snapshots: " + snapshots);
        }
        update(snapshot);
    }

    @Override
    public synchronized void update(final Snapshot snapshot) {
        // Verify that the input snapshot is newer (higher in sequence number) than the current snapshot.
        if (snapshot.sequenceNumber() <= locationKeyFinder.snapshot.sequenceNumber()) {
            throw new IllegalArgumentException(
                    "Update snapshot sequence number (" + snapshot.sequenceNumber()
                            + ") must be higher than the current snapshot sequence number ("
                            + locationKeyFinder.snapshot.sequenceNumber() + ") for table " + tableIdentifier);
        }
        // Update the snapshot.
        locationKeyFinder.snapshot = snapshot;
        refreshSnapshot();
    }

    /**
     * Refresh the table location provider with the latest snapshot from the catalog. This method will identify new
     * locations and removed locations.
     */
    private void refreshSnapshot() {
        beginTransaction(this);
        final Set<ImmutableTableLocationKey> missedKeys = new HashSet<>();
        getTableLocationKeys(ttlk -> missedKeys.add(ttlk.get()));
        locationKeyFinder.findKeys(tlk -> {
            missedKeys.remove(tlk);
            handleTableLocationKeyAdded(tlk, this);
        });
        missedKeys.forEach(tlk -> handleTableLocationKeyRemoved(tlk, this));
        endTransaction(this);
        setInitialized();
    }

    // ------------------------------------------------------------------------------------------------------------------
    // SubscriptionAggregator implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    protected void activateUnderlyingDataSource() {
        if (!initialized) {
            refreshSnapshot();
            activationSuccessful(this);
            initialized = true;
        }
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
