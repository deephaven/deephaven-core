//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables without partitions that will discover data files
 * from a {@link org.apache.iceberg.Snapshot}
 */
public final class IcebergFlatLayout implements TableLocationKeyFinder<IcebergTableLocationKey> {
    private final Snapshot tableSnapshot;
    private final FileIO fileIO;
    private final Map<URI, IcebergTableLocationKey> cache;
    private final Object readInstructions;

    private static IcebergTableLocationKey locationKey(
            final FileFormat format,
            final URI fileUri,
            @NotNull final Object readInstructions) {
        return new IcebergTableLocationKey(format, fileUri, 0, null, readInstructions);
    }

    /**
     * @param tableSnapshot The {@link Snapshot} from which to discover data files.
     * @param readInstructions the instructions for customizations while reading.
     */
    public IcebergFlatLayout(
            @NotNull final Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final Object readInstructions) {
        this.tableSnapshot = tableSnapshot;
        this.fileIO = fileIO;
        this.readInstructions = readInstructions;

        this.cache = new HashMap<>();
    }

    public String toString() {
        return IcebergFlatLayout.class.getSimpleName() + '[' + tableSnapshot + ']';
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<IcebergTableLocationKey> locationKeyObserver) {
        try {
            // Retrieve the manifest files from the snapshot
            final List<ManifestFile> manifestFiles = tableSnapshot.allManifests(fileIO);
            for (final ManifestFile manifestFile : manifestFiles) {
                final ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, fileIO);
                for (DataFile df : reader) {
                    final URI fileUri = URI.create(df.path().toString());
                    IcebergTableLocationKey locationKey = cache.get(fileUri);
                    if (locationKey == null) {
                        locationKey = locationKey(df.format(), fileUri, readInstructions);
                        if (!locationKey.verifyFileReader()) {
                            continue;
                        }
                        cache.put(fileUri, locationKey);
                    }
                    locationKeyObserver.accept(locationKey);
                }
            }
        } catch (final Exception e) {
            throw new TableDataException("Error finding Iceberg locations under " + tableSnapshot, e);
        }
    }
}
