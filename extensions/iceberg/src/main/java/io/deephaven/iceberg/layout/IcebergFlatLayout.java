//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.location.IcebergTableParquetLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
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
    /**
     * The Iceberg {@link Table} to discover locations for.
     */
    private final Table table;

    /**
     * The {@link FileIO} to use for passing to the catalog reading manifest data files.
     */
    private final FileIO fileIO;

    /**
     * A cache of {@link IcebergTableLocationKey}s keyed by the URI of the file they represent.
     */
    private final Map<URI, IcebergTableLocationKey> cache;

    /**
     * The instructions for customizations while reading. Could be a {@link ParquetInstructions} or similar.
     */
    private final Object readInstructions;

    /**
     * The current {@link Snapshot} to discover locations for.
     */
    private Snapshot currentSnapshot;

    private static IcebergTableLocationKey locationKey(
            final FileFormat format,
            final URI fileUri,
            @NotNull final Object readInstructions) {
        if (format == FileFormat.PARQUET) {
            return new IcebergTableParquetLocationKey(fileUri, 0, null, (ParquetInstructions) readInstructions);
        }
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }

    /**
     * @param table The {@link Table} to discover locations for.
     * @param tableSnapshot The {@link Snapshot} from which to discover data files.
     * @param fileIO The file IO to use for reading manifest data files.
     * @param readInstructions The instructions for customizations while reading.
     */
    public IcebergFlatLayout(
            @NotNull final Table table,
            @NotNull final Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final Object readInstructions) {
        this.table = table;
        this.currentSnapshot = tableSnapshot;
        this.fileIO = fileIO;
        this.readInstructions = readInstructions;

        this.cache = new HashMap<>();
    }

    public String toString() {
        return IcebergFlatLayout.class.getSimpleName() + '[' + table.name() + ']';
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<IcebergTableLocationKey> locationKeyObserver) {
        try {
            // Retrieve the manifest files from the snapshot
            final List<ManifestFile> manifestFiles = currentSnapshot.allManifests(fileIO);
            for (final ManifestFile manifestFile : manifestFiles) {
                // Currently only can process manifest files with DATA content type.
                Assert.eq(manifestFile.content(), "manifestFile.content()",
                        ManifestContent.DATA, "ManifestContent.DATA");
                final ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, fileIO);
                for (DataFile df : reader) {
                    final URI fileUri = FileUtils.convertToURI(df.path().toString(), false);
                    final IcebergTableLocationKey locationKey = cache.computeIfAbsent(fileUri, uri -> {
                        final IcebergTableLocationKey key = locationKey(df.format(), fileUri, readInstructions);
                        // Verify before caching.
                        return key.verifyFileReader() ? key : null;
                    });
                    if (locationKey != null) {
                        locationKeyObserver.accept(locationKey);
                    }
                }
            }
        } catch (final Exception e) {
            throw new TableDataException("Error finding Iceberg locations under " + currentSnapshot, e);
        }
    }
}
