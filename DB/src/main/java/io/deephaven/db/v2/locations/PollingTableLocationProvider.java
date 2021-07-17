package io.deephaven.db.v2.locations;

import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Polling-driven {@link TableLocationProvider} implementation that delegates location discovery and creation to a
 * {@link Scanner} instance.
 */
public class PollingTableLocationProvider extends AbstractTableLocationProvider {

    /**
     * Scanner to handle location discover and creation.
     */
    public interface Scanner {

        /**
         * Scan for available {@link TableLocationKey}s and deliver them to the observer.
         *
         * @param locationKeyObserver Callback for key delivery
         */
        void scanAll(@NotNull Consumer<TableLocationKey> locationKeyObserver);

        /**
         * Manufacture a {@link TableLocation} from the supplied key, which must have come from this scanner
         *
         * @param tableKey    The {@link TableKey} for the provider using this scanner instance
         * @param locationKey The {@link TableLocationKey} (or an immutable equivalent), previously discovered via
         *                    {@link #scanAll(Consumer)}
         * @return A new or cached {@link TableLocation} identified by the supplied {@link TableLocationKey}
         */
        TableLocation makeLocation(@NotNull final TableKey tableKey, @NotNull TableLocationKey locationKey);
    }

    private final Scanner scanner;
    private final TableDataRefreshService refreshService;

    private TableDataRefreshService.CancellableSubscriptionToken subscriptionToken;

    public PollingTableLocationProvider(@NotNull final TableKey tableKey,
                                        @NotNull final Scanner scanner,
                                        @Nullable final TableDataRefreshService refreshService) {
        super(tableKey, refreshService != null);
        this.scanner = scanner;
        this.refreshService = refreshService;
    }

    //------------------------------------------------------------------------------------------------------------------
    // AbstractTableLocationProvider implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public String getImplementationName() {
        return PollingTableLocationProvider.class.getSimpleName();
    }

    @Override
    public void refresh() {
        scanner.scanAll(this::handleTableLocationKey);
        setInitialized();
    }

    @Override
    @NotNull
    protected TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
        return scanner.makeLocation(getKey(), locationKey);
    }

    //------------------------------------------------------------------------------------------------------------------
    // SubscriptionAggregator implementation
    //------------------------------------------------------------------------------------------------------------------

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
