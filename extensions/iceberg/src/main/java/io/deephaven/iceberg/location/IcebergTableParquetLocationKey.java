//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.apache.iceberg.DataFile;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.Map;

/**
 * {@link TableLocationKey} implementation for use with data stored in Iceberg tables in the parquet format.
 */
public class IcebergTableParquetLocationKey extends ParquetTableLocationKey implements IcebergTableLocationKey {
    private static final String IMPLEMENTATION_NAME = IcebergTableParquetLocationKey.class.getSimpleName();

    @Nullable
    private final Long dataSequenceNumber;
    @Nullable
    private final Long fileSequenceNumber;
    @Nullable
    private final Long pos;

    private final ParquetInstructions readInstructions;

    /**
     * Construct a new IcebergTableParquetLocationKey for the supplied {@code fileUri} and {@code partitions}.
     *
     * @param dataFile The data file that backs the keyed location
     * @param fileUri The {@link URI} for the file that backs the keyed location
     * @param order Explicit ordering index, taking precedence over other fields
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call
     * @param readInstructions the instructions for customizations while reading
     */
    public IcebergTableParquetLocationKey(
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            final int order,
            @Nullable final Map<String, Comparable<?>> partitions,
            @NotNull final ParquetInstructions readInstructions) {
        super(fileUri, order, partitions, readInstructions);
        this.dataSequenceNumber = dataFile.dataSequenceNumber();
        this.fileSequenceNumber = dataFile.fileSequenceNumber();
        this.pos = dataFile.pos();
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

    /**
     * Precedence-wise this implementation compares {@code order}, then {@code dataSequenceNumber}, then
     * {@code fileSequenceNumber}, then {@code pos}, then applies a {@link PartitionsComparator} to {@code partitions}
     * and finally {@code uri}.
     *
     * @inheritDoc
     */
    @Override
    public int compareTo(@NotNull final TableLocationKey other) {
        if (other instanceof IcebergTableParquetLocationKey) {
            final IcebergTableParquetLocationKey otherTyped = (IcebergTableParquetLocationKey) other;
            int comparisonResult;
            if ((comparisonResult = Integer.compare(order, otherTyped.order)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = compareNullable(dataSequenceNumber, otherTyped.dataSequenceNumber)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = compareNullable(fileSequenceNumber, otherTyped.fileSequenceNumber)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = compareNullable(pos, otherTyped.pos)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = PartitionsComparator.INSTANCE.compare(partitions, otherTyped.partitions)) != 0) {
                return comparisonResult;
            }
            return uri.compareTo(otherTyped.uri);
        }
        throw new ClassCastException("Cannot compare " + getClass() + " to " + other.getClass());
    }

    private static int compareNullable(final Long first, final Long second) {
        if (first == null || second == null) {
            // Cannot compare, treat them as equal, so we can continue to the next comparison
            return 0;
        } else {
            return first.compareTo(second);
        }
    }
}
