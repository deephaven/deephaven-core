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
import io.deephaven.iceberg.util.IcebergInstructions;
import io.deephaven.parquet.table.ParquetInstructions;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables with partitions that will discover data files from
 * a {@link org.apache.iceberg.Snapshot}
 */
public final class IcebergKeyValuePartitionedLayout implements TableLocationKeyFinder<IcebergTableLocationKey> {
    /**
     * The Iceberg {@link Table} to discover locations for.
     */
    private final Table table;

    /**
     * The {@link FileIO} to use for passing to the catalog reading manifest data files.
     */
    private final FileIO fileIO;

    /**
     * The columns to use for partitioning.
     */
    private final String[] partitionColumns;

    /**
     * A cache of {@link IcebergTableLocationKey}s keyed by the URI of the file they represent.
     */
    private final Map<URI, IcebergTableLocationKey> cache;

    /**
     * The instructions for customizations while reading. Could be a {@link ParquetInstructions} or similar.
     */
    private final IcebergInstructions instructions;

    /**
     * The current {@link Snapshot} to discover locations for.
     */
    private Snapshot currentSnapshot;

    private static IcebergTableLocationKey locationKey(
            final FileFormat format,
            final URI fileUri,
            final Map<String, Comparable<?>> partitions,
            @NotNull final IcebergInstructions instructions) {

        if (format == FileFormat.PARQUET) {
            final ParquetInstructions parquetInstructions;
            if (instructions.parquetInstructions().isPresent()) {
                // Accept the user supplied instructions without change
                parquetInstructions = instructions.parquetInstructions().get();
            } else if (instructions.s3Instructions().isPresent()) {
                // Create a default Parquet instruction object from the S3 instructions
                parquetInstructions = ParquetInstructions.builder()
                        .setSpecialInstructions(instructions.s3Instructions().get())
                        .build();
            } else {
                // Create a default Parquet instruction object
                parquetInstructions = ParquetInstructions.builder().build();
            }
            return new IcebergTableParquetLocationKey(fileUri, 0, partitions, parquetInstructions);
        }
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }

    /**
     * @param table The {@link Table} to discover locations for.
     * @param tableSnapshot The {@link Snapshot} from which to discover data files.
     * @param fileIO The file IO to use for reading manifest data files.
     * @param partitionColumns The columns to use for partitioning.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergKeyValuePartitionedLayout(
            @NotNull final Table table,
            @NotNull final Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final String[] partitionColumns,
            @NotNull final IcebergInstructions instructions) {
        this.table = table;
        this.currentSnapshot = tableSnapshot;
        this.fileIO = fileIO;
        this.partitionColumns = partitionColumns;
        this.instructions = instructions;

        this.cache = new HashMap<>();
    }

    public String toString() {
        return IcebergFlatLayout.class.getSimpleName() + '[' + table.name() + ']';
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<IcebergTableLocationKey> locationKeyObserver) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
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
                        final PartitionData partitionData = (PartitionData) df.partition();
                        for (int ii = 0; ii < partitionColumns.length; ++ii) {
                            partitions.put(partitionColumns[ii], (Comparable<?>) partitionData.get(ii));
                        }
                        final IcebergTableLocationKey key =
                                locationKey(df.format(), fileUri, partitions, instructions);
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
