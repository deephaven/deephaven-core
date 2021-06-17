package io.deephaven.db.v2.locations.local;

import io.deephaven.db.v2.locations.DeferredTableLocation;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Map;

/**
 * Location provider for read-only table locations.
 */
public class ReadOnlyLocalTableLocationProvider extends LocalTableLocationProviderByScanner {

    private static final String PARQUET_FILE_NAME = "table.parquet";

    private final Map<String, String> columnNameMappings;

    public ReadOnlyLocalTableLocationProvider(
            @NotNull final TableKey tableKey,
            @NotNull final Scanner scanner,
            final boolean supportsSubscriptions,
            @NotNull final TableDataRefreshService refreshService,
            @NotNull final Map<String, String> columnNameMappings) {
        super(tableKey, scanner, supportsSubscriptions, refreshService);
        this.columnNameMappings = columnNameMappings;
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
                new File(scanner.computeLocationDirectory(tableKey, tableLocationKey), PARQUET_FILE_NAME),
                false,
                columnNameMappings);
    }

    private TableLocation makeDataDrivenLocation(@NotNull final TableKey tableKey, @NotNull final TableLocationKey tableLocationKey) {
        final File directory = scanner.computeLocationDirectory(tableKey, tableLocationKey);
        final File parquetFile = new File(directory, PARQUET_FILE_NAME);
        if (Utils.fileExistsPrivileged(parquetFile)) {
            return new ReadOnlyParquetTableLocation(tableKey, tableLocationKey, parquetFile, supportsSubscriptions(), columnNameMappings);
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
