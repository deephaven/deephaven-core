//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.local.URITableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.apache.iceberg.FileFormat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.Map;

/**
 * {@link TableLocationKey} implementation for use with data stored in the parquet format.
 */
public class IcebergTableLocationKey extends URITableLocationKey {

    private static final String IMPLEMENTATION_NAME = IcebergTableLocationKey.class.getSimpleName();

    final FileFormat format;
    final URITableLocationKey internalTableLocationKey;

    /**
     * Construct a new IcebergTableLocationKey for the supplied {@code fileUri} and {@code partitions}.
     *
     * @param fileUri The file that backs the keyed location.
     * @param order Explicit ordering index, taking precedence over other fields
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call
     * @param readInstructions the instructions for customizations while reading
     */
    public IcebergTableLocationKey(
            final FileFormat format,
            @NotNull final URI fileUri,
            final int order,
            @Nullable final Map<String, Comparable<?>> partitions,
            @NotNull final Object readInstructions) {
        super(fileUri, order, partitions);
        this.format = format;
        if (format == FileFormat.PARQUET) {
            // This constructor will perform validation of the parquet file
            final ParquetInstructions parquetInstructions = (ParquetInstructions) readInstructions;
            this.internalTableLocationKey =
                    new ParquetTableLocationKey(fileUri, order, partitions, parquetInstructions);
        } else {
            throw new IllegalArgumentException("Unsupported file format: " + format);
        }
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    /**
     * See {@link ParquetTableLocationKey#verifyFileReader()}.
     */
    public synchronized boolean verifyFileReader() {
        if (format == FileFormat.PARQUET) {
            return ((ParquetTableLocationKey) internalTableLocationKey).verifyFileReader();
        } else {
            throw new IllegalArgumentException("Unsupported file format: " + format);
        }
    }
}
