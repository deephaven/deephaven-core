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
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
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
     * The URI scheme from the Table {@link Table#location() location}.
     */
    private final String uriScheme;

    /**
     * A cache of {@link IcebergTableLocationKey IcebergTableLocationKeys} keyed by the URI of the file they represent.
     */
    private final Map<URI, IcebergTableLocationKey> cache;

    /**
     * The {@link ParquetInstructions} object that will be used to read any Parquet data files in this table.
     */
    private final ParquetInstructions parquetInstructions;

    /**
     * The {@link SeekableChannelsProvider} object that will be used for {@link IcebergTableParquetLocationKey}
     * creation.
     */
    private final SeekableChannelsProvider channelsProvider;

    /**
     * The {@link Snapshot} from which to discover data files.
     */
    Snapshot snapshot;

    protected IcebergTableLocationKey locationKey(
            final org.apache.iceberg.FileFormat format,
            final URI fileUri,
            @Nullable final Map<String, Comparable<?>> partitions) {
        if (format == org.apache.iceberg.FileFormat.PARQUET) {
            return new IcebergTableParquetLocationKey(fileUri, 0, partitions, parquetInstructions, channelsProvider);
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
        this.tableDef = tableAdapter.definition(instructions);
        this.uriScheme = locationUri(tableAdapter.icebergTable()).getScheme();
        // Add the data instructions if provided as part of the IcebergReadInstructions, or else attempt to create
        // data instructions from the properties collection and URI scheme.
        final Object specialInstructions = instructions.dataInstructions()
                .orElseGet(() -> dataInstructionsProvider.load(uriScheme));
        {
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
            if (specialInstructions != null) {
                builder.setSpecialInstructions(specialInstructions);
            }
            this.parquetInstructions = builder.build();
        }
        this.channelsProvider = SeekableChannelsProviderLoader.getInstance().load(uriScheme, specialInstructions);
        this.cache = new HashMap<>();
    }

    abstract IcebergTableLocationKey keyFromDataFile(DataFile df, URI fileUri);

    private static String path(String path, FileIO io) {
        return io instanceof RelativeFileIO ? ((RelativeFileIO) io).absoluteLocation(path) : path;
    }

    private static URI locationUri(Table table) {
        return FileUtils.convertToURI(path(table.location(), table.io()), true);
    }

    private static URI dataFileUri(Table table, DataFile dataFile) {
        return FileUtils.convertToURI(path(dataFile.path().toString(), table.io()), false);
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
                        final URI fileUri = dataFileUri(table, df);
                        if (!uriScheme.equals(fileUri.getScheme())) {
                            throw new TableDataException(String.format(
                                    "%s:%d - multiple URI schemes are not currently supported. uriScheme=%s, fileUri=%s",
                                    table, snapshot.snapshotId(), uriScheme, fileUri));
                        }
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
