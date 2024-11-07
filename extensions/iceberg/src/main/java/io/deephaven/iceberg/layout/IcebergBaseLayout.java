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
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class IcebergBaseLayout implements TableLocationKeyFinder<IcebergTableLocationKey> {
    /**
     * The {@link IcebergTableAdapter} that will be used to access the table.
     */
    final IcebergTableAdapter tableAdapter;

    /**
     * The {@link TableDefinition} that will be used for life of this table. Although Iceberg table schema may change,
     * schema changes are not supported in Deephaven.
     */
    final TableDefinition tableDef;

    /**
     * The instructions for customizations while reading.
     */
    final IcebergReadInstructions instructions;

    /**
     * A cache of {@link IcebergTableLocationKey IcebergTableLocationKeys} keyed by the URI of the file they represent.
     */
    final Map<URI, IcebergTableLocationKey> cache;

    /**
     * The {@link Snapshot} from which to discover data files.
     */
    Snapshot snapshot;

    /**
     * The data instructions provider for creating instructions from URI and user-supplied properties.
     */
    final DataInstructionsProviderLoader dataInstructionsProvider;

    /**
     * The {@link ParquetInstructions} object that will be used to read any Parquet data files in this table. Only
     * accessed while synchronized on {@code this}.
     */
    ParquetInstructions parquetInstructions;

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

                // Add the data instructions if provided as part of the IcebergReadInstructions.
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
            return new IcebergTableParquetLocationKey(fileUri, 0, partitions, parquetInstructions);
        }
        throw new UnsupportedOperationException(String.format("%s:%d - an unsupported file format %s for URI '%s'",
                tableAdapter, snapshot.snapshotId(), format, fileUri));
    }

    /**
     * @param tableAdapter The {@link IcebergTableAdapter} that will be used to access the table.
     * @param instructions The instructions for customizations while reading.
     */
    public IcebergBaseLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final IcebergReadInstructions instructions,
            @NotNull final DataInstructionsProviderLoader dataInstructionsProvider) {
        this.tableAdapter = tableAdapter;
        this.snapshot = tableAdapter.getSnapshot(instructions);
        this.instructions = instructions;
        this.dataInstructionsProvider = dataInstructionsProvider;
        this.tableDef = tableAdapter.definition(instructions);

        this.cache = new HashMap<>();
    }

    abstract IcebergTableLocationKey keyFromDataFile(DataFile df, URI fileUri);

    @NotNull
    private URI dataFileUri(@NotNull DataFile df) {
        String path = df.path().toString();
        final FileIO fileIO = tableAdapter.icebergTable().io();
        if (fileIO instanceof RelativeFileIO) {
            path = ((RelativeFileIO) fileIO).absoluteLocation(path);
        }
        return FileUtils.convertToURI(path, false);
    }

    @Override
    public synchronized void findKeys(@NotNull final Consumer<IcebergTableLocationKey> locationKeyObserver) {
        if (snapshot == null) {
            return;
        }
        final Table table = tableAdapter.icebergTable();
        try {
            // Retrieve the manifest files from the snapshot
            final List<ManifestFile> manifestFiles = snapshot.allManifests(table.io());
            for (final ManifestFile manifestFile : manifestFiles) {
                // Currently only can process manifest files with DATA content type.
                if (manifestFile.content() != ManifestContent.DATA) {
                    throw new TableDataException(
                            String.format("%s:%d - only DATA manifest files are currently supported, encountered %s",
                                    table, snapshot.snapshotId(), manifestFile.content()));
                }
                try (final ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, table.io())) {
                    for (DataFile df : reader) {
                        final URI fileUri = dataFileUri(df);
                        final IcebergTableLocationKey locationKey =
                                cache.computeIfAbsent(fileUri, uri -> keyFromDataFile(df, fileUri));
                        if (locationKey != null) {
                            locationKeyObserver.accept(locationKey);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            throw new TableDataException(
                    String.format("%s:%d - error finding Iceberg locations", tableAdapter, snapshot.snapshotId()), e);
        }
    }

    /**
     * Update the snapshot to the latest snapshot from the catalog if
     */
    protected synchronized boolean maybeUpdateSnapshot() {
        final Snapshot latestSnapshot = tableAdapter.currentSnapshot();
        if (latestSnapshot == null) {
            return false;
        }
        if (snapshot == null || latestSnapshot.sequenceNumber() > snapshot.sequenceNumber()) {
            snapshot = latestSnapshot;
            return true;
        }
        return false;
    }

    /**
     * Update the snapshot to the user specified snapshot. See
     * {@link io.deephaven.iceberg.util.IcebergTable#update(long)} for more details.
     */
    protected void updateSnapshot(long snapshotId) {
        final List<Snapshot> snapshots = tableAdapter.listSnapshots();

        final Snapshot snapshot = snapshots.stream()
                .filter(s -> s.snapshotId() == snapshotId).findFirst()
                .orElse(null);

        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "Snapshot " + snapshotId + " was not found in the list of snapshots for table " + tableAdapter
                            + ". Snapshots: " + snapshots);
        }
        updateSnapshot(snapshot);
    }

    /**
     * Update the snapshot to the user specified snapshot. See
     * {@link io.deephaven.iceberg.util.IcebergTable#update(Snapshot)} for more details.
     */
    protected void updateSnapshot(@NotNull final Snapshot updateSnapshot) {
        // Validate that we are not trying to update to an older snapshot.
        if (snapshot != null && updateSnapshot.sequenceNumber() <= snapshot.sequenceNumber()) {
            throw new IllegalArgumentException(
                    "Update snapshot sequence number (" + updateSnapshot.sequenceNumber()
                            + ") must be higher than the current snapshot sequence number ("
                            + snapshot.sequenceNumber() + ") for table " + tableAdapter);
        }

        snapshot = updateSnapshot;
    }
}
