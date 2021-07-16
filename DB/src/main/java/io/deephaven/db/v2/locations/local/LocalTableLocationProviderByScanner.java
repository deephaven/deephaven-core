package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * Base class for local table location providers.
 */
abstract class LocalTableLocationProviderByScanner extends LocalTableLocationProvider {

    public interface Scanner {
        void scanAll(@NotNull Consumer<TableLocationKey> locationKeyObserver);
        String computeLocationBasePath(@NotNull TableKey tableKey, @NotNull TableLocationKey locationKey);
        default TableLocationMetadataIndex getMetadataIndex() {
            return null;
        }
    }

    final Scanner scanner;

    LocalTableLocationProviderByScanner(@NotNull final TableKey tableKey,
                                        @NotNull final Scanner scanner,
                                        final boolean supportsSubscriptions,
                                        @NotNull final TableDataRefreshService refreshService) {
        super(tableKey, supportsSubscriptions, refreshService);
        this.scanner = scanner;
    }
}
