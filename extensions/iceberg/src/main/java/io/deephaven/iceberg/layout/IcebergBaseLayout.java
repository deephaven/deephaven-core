//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.location.IcebergTableParquetLocationKey;
import io.deephaven.iceberg.relative.RelativeFileIO;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.deephaven.iceberg.base.IcebergUtils.allDataFiles;

public abstract class IcebergBaseLayout implements TableLocationKeyFinder<IcebergTableLocationKey> {
    /**
     * The {@link TableDefinition} that will be used for the table.
     */
    final TableDefinition tableDef;

    /**
     * The Iceberg {@link Table} to discover locations for.
     */
    final Table table;

    /**
     * The {@link Snapshot} to discover locations for.
     */
    final Snapshot snapshot;

    /**
     * The {@link FileIO} to use for passing to the catalog reading manifest data files.
     */
    final FileIO fileIO;

    /**
     * The instructions for customizations while reading.
     */
    final IcebergReadInstructions instructions;

    /**
     * A cache of {@link IcebergTableLocationKey IcebergTableLocationKeys} keyed by the URI of the file they represent.
     */
    final Map<URI, IcebergTableLocationKey> cache;

    /**
     * The data instructions provider for creating instructions from URI and user-supplied properties.
     */
    final DataInstructionsProviderLoader dataInstructionsProvider;

    /**
     * The {@link ParquetInstructions} object that will be used to read any Parquet data files in this table. Only
     * accessed while synchronized on {@code this}.
     */
    ParquetInstructions parquetInstructions;

    /**
     * The number of files discovered, used for ordering the keys.
     */
    private int fileCount;

    protected IcebergTableLocationKey locationKey(
            final org.apache.iceberg.FileFormat format,
            final URI fileUri,
            @Nullable final Map<String, Comparable<?>> partitions) {

        if (format == org.apache.iceberg.FileFormat.PARQUET) {
            if (parquetInstructions == null) {
                // Start with user-supplied instructions (if provided).
                final ParquetInstructions.Builder builder = new ParquetInstructions.Builder();

                // Add the table definition.
                builder.setTableDefinition(tableDef);

                // Add any column rename mappings.
                if (!instructions.columnRenames().isEmpty()) {
                    for (Map.Entry<String, String> entry : instructions.columnRenames().entrySet()) {
                        builder.addColumnNameMapping(entry.getKey(), entry.getValue());
                    }
                }

                // Add the data instructions if provided as part of the iceberg instructions.
                if (instructions.dataInstructions().isPresent()) {
                    builder.setSpecialInstructions(instructions.dataInstructions().get());
                } else {
                    // Attempt to create data instructions from the properties collection and URI.
                    final Object dataInstructions = dataInstructionsProvider.fromServiceLoader(fileUri);
                    if (dataInstructions != null) {
                        builder.setSpecialInstructions(dataInstructions);
                    }
                }

                parquetInstructions = builder.build();
            }
            return new IcebergTableParquetLocationKey(fileUri, fileCount++, partitions, parquetInstructions);
        }
        throw new UnsupportedOperationException(String.format("%s:%d - an unsupported file format %s for URI '%s'",
                table, snapshot.snapshotId(), format, fileUri));
    }

    /**
     * @param tableDef The {@link TableDefinition} that will be used for the table.
     * @param table The {@link Table} to discover locations for.
     * @param tableSnapshot The {@link Snapshot} from which to discover data files.
     * @param fileIO The file IO to use for reading manifest data files.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergBaseLayout(
            @NotNull final TableDefinition tableDef,
            @NotNull final Table table,
            @NotNull final Snapshot tableSnapshot,
            @NotNull final FileIO fileIO,
            @NotNull final IcebergReadInstructions instructions,
            @NotNull final DataInstructionsProviderLoader dataInstructionsProvider) {
        this.tableDef = tableDef;
        this.table = table;
        this.snapshot = tableSnapshot;
        this.fileIO = fileIO;
        this.instructions = instructions;
        this.dataInstructionsProvider = dataInstructionsProvider;

        this.cache = new HashMap<>();
    }

    abstract IcebergTableLocationKey keyFromDataFile(DataFile df, URI fileUri);

    @NotNull
    private URI dataFileUri(@NotNull DataFile df) {
        String path = df.path().toString();
        if (fileIO instanceof RelativeFileIO) {
            path = ((RelativeFileIO) fileIO).absoluteLocation(path);
        }
        return FileUtils.convertToURI(path, false);
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<IcebergTableLocationKey> locationKeyObserver) {
        try (final Stream<DataFile> dataFiles = allDataFiles(table, snapshot)) {
            dataFiles
                    .map(df -> {
                        final URI fileUri = dataFileUri(df);
                        return cache.computeIfAbsent(fileUri, uri -> keyFromDataFile(df, fileUri));
                    })
                    .filter(Objects::nonNull)
                    .forEach(locationKeyObserver);
        } catch (final RuntimeException e) {
            throw new TableDataException(
                    String.format("%s:%d - error finding Iceberg locations", table, snapshot.snapshotId()), e);
        }
    }
}
