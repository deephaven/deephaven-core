package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.PollingTableLocationProvider;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Base class for local table location providers.
 */
abstract class PollingTableLocationProviderByScanner extends PollingTableLocationProvider {

    public interface Scanner {
        void scanAll(@NotNull Consumer<TableLocationKey> locationKeyObserver);
        TableLocation makeLocation(@NotNull TableLocationKey locationKey);
    }

    final Scanner scanner;

    PollingTableLocationProviderByScanner(@NotNull final TableKey tableKey,
                                          @NotNull final Scanner scanner,
                                          final boolean supportsSubscriptions,
                                          @NotNull final TableDataRefreshService refreshService) {
        super(tableKey, supportsSubscriptions, refreshService);
        this.scanner = scanner;
    }
}
