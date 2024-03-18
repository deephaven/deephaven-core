//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.function.Consumer;

/**
 * Parquet {@link TableLocationKeyFinder location finder} that will discover a single file.
 */
public final class ParquetSingleFileLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {
    private final URI parquetFileURI;
    private final ParquetInstructions readInstructions;

    /**
     * @param parquetFileURI URI of single parquet file to find
     * @param readInstructions the instructions for customizations while reading
     */
    public ParquetSingleFileLayout(@NotNull final URI parquetFileURI,
            @NotNull final ParquetInstructions readInstructions) {
        this.parquetFileURI = parquetFileURI;
        this.readInstructions = readInstructions;
    }

    public String toString() {
        return ParquetSingleFileLayout.class.getSimpleName() + '[' + parquetFileURI + ']';
    }

    @Override
    public void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        locationKeyObserver.accept(new ParquetTableLocationKey(parquetFileURI, 0, null, readInstructions));
    }
}
