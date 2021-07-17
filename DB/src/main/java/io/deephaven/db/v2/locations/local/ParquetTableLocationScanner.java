package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.*;
import io.deephaven.db.v2.locations.parquet.ParquetTableLocationKey;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Location scanner for parquet locations.
 */
public final class ParquetTableLocationScanner implements PollingTableLocationProvider.Scanner {

    private final Supplier<Stream<ParquetTableLocationKey>> keySupplier;
    private final ParquetInstructions readInstructions;

    public ParquetTableLocationScanner(@NotNull final Supplier<Stream<ParquetTableLocationKey>> keySupplier,
                                       @NotNull final ParquetInstructions readInstructions) {
        this.keySupplier = keySupplier;
        this.readInstructions = readInstructions;
    }

    @Override
    public void scanAll(@NotNull final Consumer<TableLocationKey> locationKeyObserver) {
        keySupplier.get().forEach(locationKeyObserver);
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

    public static TableLocationProvider makeSingleFileProvider(@NotNull final ParquetInstructions readInstructions, @NotNull final File file) {
        return new PollingTableLocationProvider(
                StandaloneTableKey.getInstance(),
                new ParquetTableLocationScanner(() -> Stream.of(new ParquetTableLocationKey(file, null)), readInstructions),
                null);
    }
}
