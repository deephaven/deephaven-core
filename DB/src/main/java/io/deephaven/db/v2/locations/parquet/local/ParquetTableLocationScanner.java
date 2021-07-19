package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.db.v2.locations.*;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * Location scanner for parquet locations.
 */
public final class ParquetTableLocationScanner implements PollingTableLocationProvider.Scanner {

    /**
     * Interface for key-discovery callbacks.
     */
    @FunctionalInterface
    public interface LocationKeyFinder {
        void findKeys(@NotNull final Consumer<TableLocationKey> locationKeyObserver);
    }

    private final LocationKeyFinder locationKeyFinder;
    private final ParquetInstructions readInstructions;

    public ParquetTableLocationScanner(@NotNull final LocationKeyFinder locationKeyFinder,
                                       @NotNull final ParquetInstructions readInstructions) {
        this.locationKeyFinder = locationKeyFinder;
        this.readInstructions = readInstructions;
    }

    @Override
    public void scanAll(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        locationKeyFinder.findKeys(locationKeyObserver);
    }

    @Override
    @NotNull
    public TableLocation makeLocation(@NotNull final TableKey tableKey, @NotNull final TableLocationKey locationKey) {
        if (!(locationKey instanceof ParquetTableLocationKey)) {
            throw new IllegalArgumentException("Unexpected table location key: " + locationKey + " is not a ParquetTableLocationKey");
        }
        final File parquetFile = ((ParquetTableLocationKey) locationKey).getFile();
        if (Utils.fileExistsPrivileged(parquetFile)) {
            return new ParquetTableLocation(tableKey, locationKey, parquetFile, readInstructions);
        } else {
            return new NonexistentTableLocation(tableKey, locationKey);
        }
    }
}
