//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.TableLocationFactory;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <p>
 * Refreshing {@link TableLocationProvider} implementation that delegates {@link TableLocationKey location key}
 * discovery to a {@link TableLocationKeyFinder} and {@link TableLocation location} creation to a
 * {@link TableLocationFactory}.
 * </p>
 * <p>
 * Supports both automatic and manual refreshing cases, distinguished by the {@code autoRefresh} parameter.
 * </p>
 */
public class IcebergRefreshingTableLocationProvider<TK extends TableKey, TLK extends TableLocationKey>
        extends AbstractTableLocationProvider {

    private static final String IMPLEMENTATION_NAME = IcebergRefreshingTableLocationProvider.class.getSimpleName();

    private final IcebergBaseLayout locationKeyFinder;
    private final TableLocationFactory<TK, TLK> locationFactory;
    private final TableDataRefreshService refreshService;
    private final IcebergCatalogAdapter adapter;
    private final TableIdentifier tableIdentifier;
    private final boolean autoRefresh;

    private TableDataRefreshService.CancellableSubscriptionToken subscriptionToken;

    public IcebergRefreshingTableLocationProvider(
            @NotNull final TK tableKey,
            @NotNull final IcebergBaseLayout locationKeyFinder,
            @NotNull final TableLocationFactory<TK, TLK> locationFactory,
            @Nullable final TableDataRefreshService refreshService,
            @NotNull final IcebergCatalogAdapter adapter,
            @NotNull final TableIdentifier tableIdentifier,
            final boolean autoRefresh) {
        super(tableKey, refreshService != null);
        this.locationKeyFinder = locationKeyFinder;
        this.locationFactory = locationFactory;
        this.refreshService = refreshService;
        this.adapter = adapter;
        this.tableIdentifier = tableIdentifier;
        this.autoRefresh = autoRefresh;
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
        if (autoRefresh) {
            final Snapshot latestSnapshot = adapter.getCurrentSnapshot(tableIdentifier);
            if (latestSnapshot.sequenceNumber() > locationKeyFinder.snapshot.sequenceNumber()) {
                locationKeyFinder.snapshot = latestSnapshot;
            }
        }
        refreshSnapshot();
    }

    /**
     * Update the table location provider with the latest snapshot from the catalog.
     */
    public synchronized void update() {
        update(adapter.getCurrentSnapshot(tableIdentifier));
    }

    /**
     * Update the table location provider with a specific snapshot from the catalog. If the {@code snapshotId} is not
     * found in the list of snapshots for the table, an {@link IllegalArgumentException} is thrown. The input snapshot
     * must also be newer (higher in sequence number) than the current snapshot or an {@link IllegalArgumentException}
     * is thrown.
     */
    public synchronized void update(final long snapshotId) {
        final List<Snapshot> snapshots = adapter.listSnapshots(tableIdentifier);

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

    /**
     * Update the table location provider with a specific snapshot from the catalog. The input snapshot must be newer
     * (higher in sequence number) than the current snapshot or an {@link IllegalArgumentException} is thrown.
     * 
     * @param snapshot
     */
    public synchronized void update(final Snapshot snapshot) {
        // Verify that the input snapshot is newer (higher in sequence number) than the current snapshot.
        if (snapshot.sequenceNumber() <= locationKeyFinder.snapshot.sequenceNumber()) {
            throw new IllegalArgumentException(
                    "Snapshot sequence number " + snapshot.sequenceNumber()
                            + " is older than the current snapshot sequence number "
                            + locationKeyFinder.snapshot.sequenceNumber() + " for table " + tableIdentifier);
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
        beginTransaction();
        final Set<ImmutableTableLocationKey> missedKeys = new HashSet<>(getTableLocationKeys());
        locationKeyFinder.findKeys(tableLocationKey -> {
            // noinspection SuspiciousMethodCalls
            missedKeys.remove(tableLocationKey);
            handleTableLocationKey(tableLocationKey);
        });
        missedKeys.forEach(this::handleTableLocationKeyRemoved);
        endTransaction();
        setInitialized();
    }

    @Override
    @NotNull
    protected TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
        // noinspection unchecked
        return locationFactory.makeLocation((TK) getKey(), (TLK) locationKey, refreshService);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // SubscriptionAggregator implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    protected final void activateUnderlyingDataSource() {
        subscriptionToken = refreshService.scheduleTableLocationProviderRefresh(this);
    }

    @Override
    protected final void deactivateUnderlyingDataSource() {
        if (subscriptionToken != null) {
            subscriptionToken.cancel();
            subscriptionToken = null;
        }
    }

    @Override
    protected final <T> boolean matchSubscriptionToken(final T token) {
        return token == subscriptionToken;
    }
}
