//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.location.IcebergTableParquetLocationKey;
import io.deephaven.iceberg.util.IcebergInstructions;
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
 * from a {@link Snapshot}
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
     * A cache of {@link IcebergTableLocationKey IcebergTableLocationKeys} keyed by the URI of the file they represent.
     */
    private final Map<URI, IcebergTableLocationKey> cache;

    /**
     * The instructions for customizations while reading.
     */
    private final IcebergInstructions instructions;

    /**
     * The {@link ParquetInstructions} object that will be used to read any Parquet data files in this table.
     */
    private ParquetInstructions parquetInstructions;

    /**
     * The {@link Snapshot} to discover locations for.
     */
    private final Snapshot snapshot;

    private IcebergTableLocationKey locationKey(
            final FileFormat format,
            final URI fileUri) {
        if (format == org.apache.iceberg.FileFormat.PARQUET) {
            if (parquetInstructions == null) {
                // Start with user-supplied instructions (if provided).
                parquetInstructions = instructions.parquetInstructions().isPresent()
                        ? instructions.parquetInstructions().get()
                        : ParquetInstructions.EMPTY;

                // Use the ParquetInstructions overrides to propagate the Iceberg instructions.
                if (instructions.columnRenameMap() != null) {
                    parquetInstructions = parquetInstructions.withColumnRenameMap(instructions.columnRenameMap());
                }
                if (instructions.s3Instructions().isPresent()) {
                    parquetInstructions =
                            parquetInstructions.withSpecialInstructions(instructions.s3Instructions().get());
                }
            }
            return new IcebergTableParquetLocationKey(fileUri, 0, null, parquetInstructions);
        }
        throw new UnsupportedOperationException(String.format("%s:%d - an unsupported file format %s for URI '%s'",
                table, snapshot.snapshotId(), format, fileUri));
    }

    /**
     * @param table The {@link Table} to discover locations for.
     * @param tableSnapshot The {@link Snapshot} from which to discover data files.
     * @param fileIO The file IO to use for reading manifest data files.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergFlatLayout(
            @NotNull final Table table,
            @NotNull final Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final IcebergInstructions instructions) {
        this.table = table;
        this.snapshot = tableSnapshot;
        this.fileIO = fileIO;
        this.instructions = instructions;

        this.cache = new HashMap<>();
    }

    public String toString() {
        return IcebergFlatLayout.class.getSimpleName() + '[' + table.name() + ']';
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<IcebergTableLocationKey> locationKeyObserver) {
        try {
            // Retrieve the manifest files from the snapshot
            final List<ManifestFile> manifestFiles = snapshot.allManifests(fileIO);
            for (final ManifestFile manifestFile : manifestFiles) {
                // Currently only can process manifest files with DATA content type.
                if (manifestFile.content() != ManifestContent.DATA) {
                    throw new TableDataException(
                            String.format("%s:%d - only DATA manifest files are currently supported, encountered %s",
                                    table, snapshot.snapshotId(), manifestFile.content()));
                }
                final ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, fileIO);
                for (DataFile df : reader) {
                    final URI fileUri = FileUtils.convertToURI(df.path().toString(), false);
                    final IcebergTableLocationKey locationKey = cache.computeIfAbsent(fileUri, uri -> {
                        final IcebergTableLocationKey key = locationKey(df.format(), fileUri);
                        // Verify before caching.
                        return key.verifyFileReader() ? key : null;
                    });
                    if (locationKey != null) {
                        locationKeyObserver.accept(locationKey);
                    }
                }
            }
        } catch (final Exception e) {
            throw new TableDataException(
                    String.format("%s:%d - error finding Iceberg locations", table, snapshot.snapshotId()), e);
        }
    }
}
