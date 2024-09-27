//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.TableLocationFactory;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * Automatically refreshing {@link TableLocationProvider} implementation that delegates {@link TableLocationKey location
 * key} discovery to a {@link TableLocationKeyFinder} and {@link TableLocation location} creation to a
 * {@link TableLocationFactory}.
 * </p>
 */
public class IcebergAutoRefreshTableLocationProvider<TK extends TableKey, TLK extends TableLocationKey>
        extends IcebergTableLocationProviderBase<TK, TLK> {

    private static final String IMPLEMENTATION_NAME = IcebergAutoRefreshTableLocationProvider.class.getSimpleName();

    private final TableDataRefreshService refreshService;
    private final long refreshIntervalMs;

    private TableDataRefreshService.CancellableSubscriptionToken subscriptionToken;

    public IcebergAutoRefreshTableLocationProvider(
            @NotNull final TK tableKey,
            @NotNull final IcebergBaseLayout locationKeyFinder,
            @NotNull final TableLocationFactory<TK, TLK> locationFactory,
            @NotNull final TableDataRefreshService refreshService,
            final long refreshIntervalMs,
            @NotNull final IcebergTableAdapter adapter,
            @NotNull final TableIdentifier tableIdentifier) {
        super(tableKey,
                locationKeyFinder,
                locationFactory,
                true,
                adapter,
                tableIdentifier,
                UpdateMode.ADD_REMOVE, // New locations can be added and removed
                UpdateMode.STATIC // Individual locations cannot add or remove rows
        );

        Assert.neqNull(refreshService, "refreshService");
        this.refreshService = refreshService;
        this.refreshIntervalMs = refreshIntervalMs;
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
        adapter.refresh();
        final Snapshot latestSnapshot = adapter.currentSnapshot();
        if (latestSnapshot.sequenceNumber() > locationKeyFinder.snapshot.sequenceNumber()) {
            locationKeyFinder.snapshot = latestSnapshot;
            refreshSnapshot();
        }
    }

    @Override
    public void update() {
        throw new IllegalStateException("An automatically refreshing Iceberg table cannot be manually updated");
    }

    @Override
    public void update(long snapshotId) {
        throw new IllegalStateException("An automatically refreshing Iceberg table cannot be manually updated");
    }

    @Override
    public void update(Snapshot snapshot) {
        throw new IllegalStateException("An automatically refreshing Iceberg table cannot be manually updated");
    }

    /**
     * Refresh the table location provider with the latest snapshot from the catalog. This method will identify new
     * locations and removed locations.
     */
    private void refreshSnapshot() {
        beginTransaction(this);
        final Set<ImmutableTableLocationKey> missedKeys = new HashSet<>();
        getTableLocationKeys(ttlk -> missedKeys.add(ttlk.get()));
        locationKeyFinder.findKeys(tableLocationKey -> {
            missedKeys.remove(tableLocationKey);
            handleTableLocationKeyAdded(tableLocationKey, this);
        });
        missedKeys.forEach(tlk -> handleTableLocationKeyRemoved(tlk, this));
        endTransaction(this);
        setInitialized();
    }

    // ------------------------------------------------------------------------------------------------------------------
    // SubscriptionAggregator implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    protected final void activateUnderlyingDataSource() {
        refreshSnapshot();
        subscriptionToken = refreshService.scheduleTableLocationProviderRefresh(this, refreshIntervalMs);
    }

    @Override
    protected final void deactivateUnderlyingDataSource() {
        if (subscriptionToken != null) {
            subscriptionToken.cancel();
            subscriptionToken = null;
        }
    }
}
