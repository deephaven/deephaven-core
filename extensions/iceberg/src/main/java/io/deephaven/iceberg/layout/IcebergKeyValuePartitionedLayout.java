//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
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
 * a {@link org.apache.iceberg.Snapshot}
 */
public final class IcebergKeyValuePartitionedLayout implements TableLocationKeyFinder<IcebergTableLocationKey> {
    /**
     * The {@link TableDefinition} that will be used for the table.
     */
    final TableDefinition tableDef;
    /**
     * The Iceberg {@link org.apache.iceberg.Table} to discover locations for.
     */
    private final org.apache.iceberg.Table table;

    /**
     * The {@link FileIO} to use for passing to the catalog reading manifest data files.
     */
    private final FileIO fileIO;

    /**
     * The columns to use for partitioning.
     */
    private final String[] partitionColumns;

    /**
     * The data types of the partitioning columns.
     */
    private final Class<?>[] partitionColumnTypes;

    /**
     * A cache of {@link IcebergTableLocationKey}s keyed by the URI of the file they represent.
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
     * The current {@link org.apache.iceberg.Snapshot} to discover locations for.
     */
    private org.apache.iceberg.Snapshot currentSnapshot;

    private IcebergTableLocationKey locationKey(
            final org.apache.iceberg.FileFormat format,
            final URI fileUri,
            final Map<String, Comparable<?>> partitions) {

        if (format == org.apache.iceberg.FileFormat.PARQUET) {
            if (parquetInstructions == null) {
                // Start with user-supplied instructions (if provided).
                parquetInstructions = instructions.parquetInstructions().isPresent()
                        ? instructions.parquetInstructions().get()
                        : ParquetInstructions.builder().build();

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
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }

    /**
     * @param table The {@link org.apache.iceberg.Table} to discover locations for.
     * @param tableSnapshot The {@link org.apache.iceberg.Snapshot} from which to discover data files.
     * @param fileIO The file IO to use for reading manifest data files.
     * @param partitionColumns The columns to use for partitioning.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergKeyValuePartitionedLayout(
            @NotNull final TableDefinition tableDef,
            @NotNull final org.apache.iceberg.Table table,
            @NotNull final org.apache.iceberg.Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final String[] partitionColumns,
            @NotNull final IcebergInstructions instructions) {
        this.tableDef = tableDef;
        this.table = table;
        this.currentSnapshot = tableSnapshot;
        this.fileIO = fileIO;
        this.partitionColumns = partitionColumns;
        this.instructions = instructions;

        // Compute and store the data types of the partitioning columns.
        partitionColumnTypes = Arrays.stream(partitionColumns)
                .map(colName -> TypeUtils.getBoxedType(tableDef.getColumn(colName).getDataType()))
                .toArray(Class<?>[]::new);

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
        } catch (final Exception e) {
            throw new TableDataException("Error finding Iceberg locations under " + currentSnapshot, e);
        }
    }
}
