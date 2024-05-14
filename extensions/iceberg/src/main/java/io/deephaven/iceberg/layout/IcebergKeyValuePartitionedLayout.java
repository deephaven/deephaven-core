//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.location.IcebergTableParquetLocationKey;
import io.deephaven.iceberg.util.IcebergInstructions;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.type.TypeUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.*;
import java.util.function.Consumer;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables with partitions that will discover data files from
 * a {@link Snapshot}
 */
public final class IcebergKeyValuePartitionedLayout implements TableLocationKeyFinder<IcebergTableLocationKey> {
    /**
     * The {@link TableDefinition} that will be used for the table.
     */
    final TableDefinition tableDef;
    /**
     * The Iceberg {@link org.apache.iceberg.Table} to discover locations for.
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
            final org.apache.iceberg.FileFormat format,
            final URI fileUri,
            final Map<String, Comparable<?>> partitions) {

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
            return new IcebergTableParquetLocationKey(fileUri, 0, partitions, parquetInstructions);
        }
        throw new UnsupportedOperationException(String.format("%s:%d - an unsupported file format %s for URI '%s'",
                table, snapshot.snapshotId(), format, fileUri));
    }

    /**
     * @param table The {@link org.apache.iceberg.Table} to discover locations for.
     * @param tableSnapshot The {@link org.apache.iceberg.Snapshot} from which to discover data files.
     * @param fileIO The file IO to use for reading manifest data files.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergKeyValuePartitionedLayout(
            @NotNull final TableDefinition tableDef,
            @NotNull final org.apache.iceberg.Table table,
            @NotNull final org.apache.iceberg.Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final IcebergInstructions instructions) {
        this.tableDef = tableDef;
        this.table = table;
        this.snapshot = tableSnapshot;
        this.fileIO = fileIO;
        this.instructions = instructions;

        this.cache = new HashMap<>();
    }

    public String toString() {
        return IcebergKeyValuePartitionedLayout.class.getSimpleName() + '[' + table.name() + ']';
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<IcebergTableLocationKey> locationKeyObserver) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
        try {
            final String[] partitionColumns =
                    tableDef.getPartitioningColumns().stream().map(ColumnDefinition::getName).toArray(String[]::new);
            final Class<?>[] partitionColumnTypes = Arrays.stream(partitionColumns)
                    .map(colName -> TypeUtils.getBoxedType(tableDef.getColumn(colName).getDataType()))
                    .toArray(Class<?>[]::new);

            // Retrieve the manifest files from the snapshot
            final List<ManifestFile> manifestFiles = snapshot.allManifests(fileIO);
            for (final ManifestFile manifestFile : manifestFiles) {
                // Currently only can process manifest files with DATA content type.
                if (manifestFile.content() != ManifestContent.DATA) {
                    throw new TableDataException(
                            String.format("%s:%d - only DATA manifest files are currently supported, encountered %s",
                                    table, snapshot.snapshotId(), manifestFile.content()));
                }
                try (final ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, fileIO)) {
                    for (DataFile df : reader) {
                        final URI fileUri = FileUtils.convertToURI(df.path().toString(), false);
                        final IcebergTableLocationKey locationKey = cache.computeIfAbsent(fileUri, uri -> {
                            final PartitionData partitionData = (PartitionData) df.partition();
                            for (int ii = 0; ii < partitionColumns.length; ++ii) {
                                final Object value = partitionData.get(ii);
                                if (value != null && !value.getClass().isAssignableFrom(partitionColumnTypes[ii])) {
                                    throw new TableDataException("Partitioning column " + partitionColumns[ii]
                                            + " has type " + value.getClass().getName()
                                            + " but expected " + partitionColumnTypes[ii].getName());
                                }
                                partitions.put(partitionColumns[ii], (Comparable<?>) value);
                            }
                            final IcebergTableLocationKey key =
                                    locationKey(df.format(), fileUri, partitions);
                            // Verify before caching.
                            return key.verifyFileReader() ? key : null;
                        });
                        if (locationKey != null) {
                            locationKeyObserver.accept(locationKey);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            throw new TableDataException(
                    String.format("%s:%d - error finding Iceberg locations", table, snapshot.snapshotId()), e);
        }
    }
}
