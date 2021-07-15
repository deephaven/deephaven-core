package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.DeferredTableLocation;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * Location provider for read-only table locations.
 */
public class ReadOnlyLocalTableLocationProvider extends LocalTableLocationProviderByScanner {

    private static final String PARQUET_FILE_EXTENSION = ParquetTableWriter.PARQUET_FILE_EXTENSION;

    private final ParquetInstructions readInstructions;

    public ReadOnlyLocalTableLocationProvider(
            @NotNull final TableKey tableKey,
            @NotNull final Scanner scanner,
            final boolean supportsSubscriptions,
            @NotNull final TableDataRefreshService refreshService,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableKey, scanner, supportsSubscriptions, refreshService);
        this.readInstructions = readInstructions;
    }

    @Override
    public String getImplementationName() {
        return ReadOnlyLocalTableLocationProvider.class.getSimpleName();
    }

    @Override
    public void refresh() {
        scanner.scanAll(this::handleTableLocationKey);
        setInitialized();
    }

    @Override
    @NotNull
    @SuppressWarnings("unchecked")
    protected final TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey) {
        final TableLocationMetadataIndex.TableLocationSnapshot snapshot = maybeGetSnapshot(locationKey);
        if (snapshot == null) {
            return new DeferredTableLocation.DataDriven<>(getTableKey(), locationKey, this::makeDataDrivenLocation);
        }
        if (snapshot.getFormat() == TableLocation.Format.PARQUET) {
            return new DeferredTableLocation.SnapshotDriven<>(getTableKey(), snapshot, this::makeSnapshottedParquetLocation);
        }
        throw new UnsupportedOperationException(this + ": Unrecognized format " + snapshot.getFormat() + " for snapshotted location " + locationKey);
    }

    private TableLocation makeSnapshottedParquetLocation(@NotNull final TableKey tableKey, @NotNull final TableLocationKey tableLocationKey) {
        return new ReadOnlyParquetTableLocation(
                tableKey,
                tableLocationKey,
                new File(scanner.computeLocationBasePath(tableKey, tableLocationKey) + PARQUET_FILE_EXTENSION),
                false,
                readInstructions);
    }

    private TableLocation makeDataDrivenLocation(@NotNull final TableKey tableKey, @NotNull final TableLocationKey tableLocationKey) {
        final File parquetFile = new File(scanner.computeLocationBasePath(tableKey, tableLocationKey) + PARQUET_FILE_EXTENSION);
        if (Utils.fileExistsPrivileged(parquetFile)) {
            return new ReadOnlyParquetTableLocation(tableKey, tableLocationKey, parquetFile, supportsSubscriptions(), readInstructions);
        } else {
            throw new UnsupportedOperationException(this + ": Unrecognized data format in location " + tableLocationKey);
        }
    }

    private TableLocationMetadataIndex.TableLocationSnapshot maybeGetSnapshot(@NotNull final TableLocationKey tableLocationKey) {
        if (supportsSubscriptions()) {
            return null;
        }
        final TableLocationMetadataIndex metadataIndex = scanner.getMetadataIndex();
        if (metadataIndex == null) {
            return null;
        }
        return metadataIndex.getTableLocationSnapshot(tableLocationKey);
    }
}
