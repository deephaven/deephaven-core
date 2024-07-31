//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;

/**
 * Polling-driven {@link TableLocationProvider} implementation that delegates {@link TableLocationKey location key}
 * discovery to a {@link TableLocationKeyFinder} and {@link TableLocation location} creation to a
 * {@link TableLocationFactory}.
 */
public class PollingTableLocationProvider<TK extends TableKey, TLK extends TableLocationKey>
        extends AbstractTableLocationProvider {

    private static final String IMPLEMENTATION_NAME = PollingTableLocationProvider.class.getSimpleName();

    private final TableLocationKeyFinder<TLK> locationKeyFinder;
    private final TableLocationFactory<TK, TLK> locationFactory;
    private final TableDataRefreshService refreshService;

    private TableDataRefreshService.CancellableSubscriptionToken subscriptionToken;

    public PollingTableLocationProvider(@NotNull final TK tableKey,
            @NotNull final TableLocationKeyFinder<TLK> locationKeyFinder,
            @NotNull final TableLocationFactory<TK, TLK> locationFactory,
            @Nullable final TableDataRefreshService refreshService) {
        super(tableKey, refreshService != null);
        this.locationKeyFinder = locationKeyFinder;
        this.locationFactory = locationFactory;
        this.refreshService = refreshService;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // AbstractTableLocationProvider implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    // The simplest way to support "push" of new data availability is to provide a callback to the user that just calls
    // `refresh`, which would need to become synchronized. Alternatively, we could make an Iceberg-specific aTLP
    // implementation that exposes a more specific callback, e.g. with a snapshot ID, as well as the option to disable
    // polling. We do need a mechanism to avoid going backwards, probably.
    @Override
    public void refresh() {
        final Set<ImmutableTableLocationKey> missedKeys = new HashSet<>(getTableLocationKeys());
        locationKeyFinder.findKeys(tableLocationKey -> {
            // noinspection SuspiciousMethodCalls
            missedKeys.remove(tableLocationKey);
            handleTableLocationKeyAdded(tableLocationKey);
        });
        missedKeys.forEach(this::handleTableLocationKeyRemoved);
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
