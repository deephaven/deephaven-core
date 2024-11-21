//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * {@link TableLocationKey} implementation for use with data stored in Iceberg tables in the parquet format.
 */
public class IcebergTableParquetLocationKey extends ParquetTableLocationKey implements IcebergTableLocationKey {

    private static final String IMPLEMENTATION_NAME = IcebergTableParquetLocationKey.class.getSimpleName();

    private static final Comparator<String> CATALONG_NAME_COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());
    private static final Comparator<UUID> UUID_COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());
    private static final Comparator<TableIdentifier> TABLE_IDENTIFIER_COMPARATOR =
            Comparator.nullsFirst(TableIdentifierComparator.INSTANCE);

    @Nullable
    private final String catalogName;

    @Nullable
    private final UUID tableUuid;

    @Nullable
    private final TableIdentifier tableIdentifier;

    /**
     * The {@link DataFile#dataSequenceNumber()} of the data file backing this keyed location.
     */
    private final long dataSequenceNumber;

    /**
     * The {@link DataFile#fileSequenceNumber()} of the data file backing this keyed location.
     */
    private final long fileSequenceNumber;

    /**
     * The {@link DataFile#pos()} of data file backing this keyed location.
     */
    private final long dataFilePos;

    /**
     * The {@link ManifestFile#sequenceNumber()} of the manifest file from which the data file was discovered.
     */
    private final long manifestSequenceNumber;

    @NotNull
    private final ParquetInstructions readInstructions;

    private int cachedHashCode;

    /**
     * Construct a new IcebergTableParquetLocationKey for the supplied {@code fileUri} and {@code partitions}.
     *
     * @param catalogName The name of the catalog using which the table is accessed
     * @param tableUuid The UUID of the table, or {@code null} if not available
     * @param tableIdentifier The table identifier used to access the table
     * @param manifestFile The manifest file from which the data file was discovered
     * @param dataFile The data file that backs the keyed location
     * @param fileUri The {@link URI} for the file that backs the keyed location
     * @param order Explicit ordering index, taking precedence over other fields
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call
     * @param readInstructions the instructions for customizations while reading
     * @param channelsProvider the provider for reading the file
     */
    public IcebergTableParquetLocationKey(
            @Nullable final String catalogName,
            @Nullable final UUID tableUuid,
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final ManifestFile manifestFile,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            final int order,
            @Nullable final Map<String, Comparable<?>> partitions,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        super(fileUri, order, partitions, channelsProvider);

        this.catalogName = catalogName;
        this.tableUuid = tableUuid;

        // We don't save tableIdentifier (and thus don't use it in comparisons/equality-testing/hashCode) unless
        // tableUUID was null
        this.tableIdentifier = tableUuid != null ? null : tableIdentifier;

        // Files with unknown sequence numbers should be ordered first
        dataSequenceNumber = dataFile.dataSequenceNumber() != null ? dataFile.dataSequenceNumber() : Long.MIN_VALUE;
        fileSequenceNumber = dataFile.fileSequenceNumber() != null ? dataFile.fileSequenceNumber() : Long.MIN_VALUE;

        // This should never be null because we are discovering this data file through a non-null manifest file
        dataFilePos = Require.neqNull(dataFile.pos(), "dataFile.pos()");

        manifestSequenceNumber = manifestFile.sequenceNumber();

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
     * When comparing with another {@link IcebergTableParquetLocationKey}, precedence-wise this implementation compares:
     * <ul>
     * <li>{@code order}</li>
     * <li>{@code catalogName}</li>
     * <li>{@code uuid}</li>
     * <li>{@code tableIdentifier}</li>
     * <li>{@code dataSequenceNumber}</li>
     * <li>{@code fileSequenceNumber}</li>
     * <li>{@code manifestSequenceNumber}</li>
     * <li>{@code dataFilePos}</li>
     * <li>{@code uri}</li>
     * </ul>
     * Otherwise, it delegates to the parent class.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public int compareTo(@NotNull final TableLocationKey other) {
        if (other instanceof IcebergTableParquetLocationKey) {
            // TODO(deephaven-core#5989): Add unit tests for the ordering of data files
            final IcebergTableParquetLocationKey otherTyped = (IcebergTableParquetLocationKey) other;
            int comparisonResult;
            if ((comparisonResult = Integer.compare(order, otherTyped.order)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = CATALONG_NAME_COMPARATOR.compare(catalogName, otherTyped.catalogName)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = UUID_COMPARATOR.compare(tableUuid, otherTyped.tableUuid)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult =
                    TABLE_IDENTIFIER_COMPARATOR.compare(tableIdentifier, otherTyped.tableIdentifier)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = Long.compare(dataSequenceNumber, otherTyped.dataSequenceNumber)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = Long.compare(fileSequenceNumber, otherTyped.fileSequenceNumber)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = Long.compare(manifestSequenceNumber, otherTyped.manifestSequenceNumber)) != 0) {
                return comparisonResult;
            }
            if ((comparisonResult = Long.compare(dataFilePos, otherTyped.dataFilePos)) != 0) {
                return comparisonResult;
            }
            return uri.compareTo(otherTyped.uri);
        }
        // When comparing with non-iceberg location key, we want to compare both partitions and URI
        return super.compareTo(other);
    }

    @Override
    public boolean equals(@Nullable final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof IcebergTableParquetLocationKey)) {
            return false;
        }
        // Iceberg devs have confirmed that uri's are supposed to be unique across data files, but Iceberg doesn't have
        // enough checks to enforce that. So for safety, we are comparing all fields and not just URI.
        // https://apache-iceberg.slack.com/archives/C03LG1D563F/p1731352244907559
        final IcebergTableParquetLocationKey otherTyped = (IcebergTableParquetLocationKey) other;
        return Objects.equals(catalogName, otherTyped.catalogName)
                && Objects.equals(tableUuid, otherTyped.tableUuid)
                && Objects.equals(tableIdentifier, otherTyped.tableIdentifier)
                && dataSequenceNumber == otherTyped.dataSequenceNumber
                && fileSequenceNumber == otherTyped.fileSequenceNumber
                && dataFilePos == otherTyped.dataFilePos
                && manifestSequenceNumber == otherTyped.manifestSequenceNumber
                && uri.equals(otherTyped.uri);
    }

    @Override
    public int hashCode() {
        if (cachedHashCode == 0) {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(catalogName);
            result = prime * result + Objects.hashCode(tableUuid);
            result = prime * result + Objects.hashCode(tableIdentifier);
            result = prime * result + Long.hashCode(dataSequenceNumber);
            result = prime * result + Long.hashCode(fileSequenceNumber);
            result = prime * result + Long.hashCode(dataFilePos);
            result = prime * result + Long.hashCode(manifestSequenceNumber);
            result = prime * result + uri.hashCode();
            // Don't use 0; that's used by StandaloneTableLocationKey, and also our sentinel for the need to compute
            if (result == 0) {
                final int fallbackHashCode = IcebergTableParquetLocationKey.class.hashCode();
                cachedHashCode = fallbackHashCode == 0 ? 1 : fallbackHashCode;
            } else {
                cachedHashCode = result;
            }
        }
        return cachedHashCode;
    }
}
