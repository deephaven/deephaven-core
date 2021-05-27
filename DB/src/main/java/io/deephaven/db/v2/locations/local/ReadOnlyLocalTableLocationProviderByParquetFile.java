package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.*;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import org.jetbrains.annotations.NotNull;

import java.io.File;

public class ReadOnlyLocalTableLocationProviderByParquetFile extends AbstractTableLocationProvider {
    private final TableDataRefreshService refreshService;

    private TableDataRefreshService.CancellableSubscriptionToken subscriptionToken;
    private final File fileLocation;

    public ReadOnlyLocalTableLocationProviderByParquetFile(
            @NotNull final File fileLocation,
            @NotNull final TableDataRefreshService refreshService) {
        super(StandaloneTableKey.getInstance(), false);
        this.refreshService = refreshService;
        this.fileLocation = fileLocation;
    }

    @Override
    public String getImplementationName() {
        return "ReadOnlyLocalTableLocationProvider";
    }

    @Override
    public void refresh() { }

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

    @Override
    @NotNull
    protected final TableLocation<?> makeTableLocation(@NotNull final TableLocationKey locationKey) {
        return new DeferredTableLocation.DataDriven<>(getTableKey(), locationKey, this::makeLocation);
    }

    private TableLocation<?> makeLocation(@NotNull final TableKey tableKey, @NotNull final TableLocationKey tableLocationKey) {
        return new ReadOnlyParquetTableLocation(tableKey, tableLocationKey, fileLocation, false);
    }
}
