package io.deephaven.db.v2.locations.impl;

import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    @Override
    public void refresh() {
        locationKeyFinder.findKeys(this::handleTableLocationKey);
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
