package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.AbstractTableLocationProvider;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * Base class for local table location providers.
 */
abstract class LocalTableLocationProvider extends AbstractTableLocationProvider {

    public interface Scanner {
        void scanAll(@NotNull Consumer<TableLocationKey> locationKeyObserver);
        File computeLocationDirectory(@NotNull TableKey tableKey, @NotNull TableLocationKey locationKey);
        default TableLocationMetadataIndex getMetadataIndex() {
            return null;
        }
    }

    final Scanner scanner;

    LocalTableLocationProvider(@NotNull final TableKey tableKey,
                               @NotNull final Scanner scanner,
                               final boolean supportsSubscriptions) {
        super(tableKey, supportsSubscriptions);
        this.scanner = scanner;
    }
}
