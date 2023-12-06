/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.layout;

import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.function.Consumer;

/**
 * Parquet {@link TableLocationKeyFinder location finder} that will discover a single file.
 */
public final class ParquetSingleFileLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private final File parquetFile;
    private final ParquetInstructions readInstructions;

    /**
     * @param parquetFile The single parquet file to find
     * @param readInstructions the instructions for customizations while reading
     */
    public ParquetSingleFileLayout(@NotNull final File parquetFile,
            @NotNull final ParquetInstructions readInstructions) {
        this.parquetFile = parquetFile;
        this.readInstructions = readInstructions;
    }

    public String toString() {
        return ParquetSingleFileLayout.class.getSimpleName() + '[' + parquetFile + ']';
    }

    @Override
    public void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        locationKeyObserver.accept(new ParquetTableLocationKey(parquetFile, 0, null, readInstructions));
    }
}
