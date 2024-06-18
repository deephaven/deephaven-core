//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.Map;

/**
 * {@link TableLocationKey} implementation for use with data stored in Iceberg tables in the parquet format.
 */
public class IcebergTableParquetLocationKey extends ParquetTableLocationKey implements IcebergTableLocationKey {
    private static final String IMPLEMENTATION_NAME = IcebergTableParquetLocationKey.class.getSimpleName();

    private final ParquetInstructions readInstructions;

    /**
     * Construct a new IcebergTableParquetLocationKey for the supplied {@code fileUri} and {@code partitions}.
     *
     * @param fileUri The file that backs the keyed location
     * @param order Explicit ordering index, taking precedence over other fields
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call
     * @param readInstructions the instructions for customizations while reading
     */
    public IcebergTableParquetLocationKey(
            @NotNull final URI fileUri,
            final int order,
            @Nullable final Map<String, Comparable<?>> partitions,
            @NotNull final ParquetInstructions readInstructions) {
        super(fileUri, order, partitions, readInstructions);
        this.readInstructions = readInstructions;
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public ParquetInstructions readInstructions() {
        return readInstructions;
    }
}
