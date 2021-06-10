package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.AbstractTableLocationProvider;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import org.jetbrains.annotations.NotNull;

public abstract class LocalTableLocationProvider extends AbstractTableLocationProvider {
    private final TableDataRefreshService refreshService;
    private TableDataRefreshService.CancellableSubscriptionToken subscriptionToken;

    LocalTableLocationProvider(@NotNull final TableKey tableKey,
                               final boolean supportsSubscriptions,
                               @NotNull final TableDataRefreshService refreshService) {
        super(tableKey, supportsSubscriptions);
        this.refreshService = refreshService;
    }

    @Override
    protected void activateUnderlyingDataSource() {
        subscriptionToken = refreshService.scheduleTableLocationProviderRefresh(this);
    }

    @Override
    protected void deactivateUnderlyingDataSource() {
        if (subscriptionToken != null) {
            subscriptionToken.cancel();
            subscriptionToken = null;
        }
    }

    @Override
    protected <T> boolean matchSubscriptionToken(final T token) {
        return token == subscriptionToken;
    }


}
