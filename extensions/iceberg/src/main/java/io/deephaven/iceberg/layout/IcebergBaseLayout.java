//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.location.IcebergTableParquetLocationKey;
import io.deephaven.iceberg.relative.RelativeFileIO;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.deephaven.iceberg.base.IcebergUtils.allManifestFiles;

public abstract class IcebergBaseLayout implements TableLocationKeyFinder<IcebergTableLocationKey> {
    /**
     * The {@link IcebergTableAdapter} that will be used to access the table.
     */
    final IcebergTableAdapter tableAdapter;

    /**
     * The UUID of the table, if available.
     */
    @Nullable
    private final UUID tableUuid;

    /**
     * The URI scheme from the Table {@link Table#location() location}.
     */
    private final String uriScheme;

    /**
     * The {@link Snapshot} from which to discover data files.
     */
    Snapshot snapshot;

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
     * Create a new {@link IcebergTableLocationKey} for the given {@link ManifestFile}, {@link DataFile} and
     * {@link URI}.
     *
     * @param manifestFile The manifest file from which the data file was discovered
     * @param dataFile The data file that backs the keyed location
     * @param fileUri The {@link URI} for the file that backs the keyed location
     * @param partitions The table partitions enclosing the table location keyed by the returned key. If {@code null},
     *        the location will be a member of no partitions.
     *
     * @return A new {@link IcebergTableLocationKey}
     */
    protected IcebergTableLocationKey locationKey(
            @NotNull final ManifestFile manifestFile,
            @NotNull final PartitionSpec spec,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            @Nullable final Map<String, Comparable<?>> partitions) {
        final org.apache.iceberg.FileFormat format = dataFile.format();
        if (format == org.apache.iceberg.FileFormat.PARQUET) {
            return new IcebergTableParquetLocationKey(
                    tableAdapter.catalog().name(),
                    tableUuid,
                    tableAdapter.tableIdentifier(),
                    manifestFile,
                    spec,
                    dataFile,
                    fileUri,
                    0,
                    partitions,
                    parquetInstructions,
                    channelsProvider);
        }
        throw new UnsupportedOperationException(String.format("%s:%d - an unsupported file format %s for URI '%s'",
                tableAdapter, snapshot.snapshotId(), format, fileUri));
    }

    IcebergBaseLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @Nullable final Snapshot snapshot,
            @NotNull final ParquetInstructions pi,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        this.tableAdapter = tableAdapter;
        {
            UUID uuid;
            try {
                uuid = tableAdapter.icebergTable().uuid();
            } catch (final RuntimeException e) {
                // The UUID method is unsupported for v1 Iceberg tables since uuid is optional for v1 tables.
                uuid = null;
            }
            this.tableUuid = uuid;
        }
        this.snapshot = snapshot;
        this.uriScheme = locationUri(tableAdapter.icebergTable()).getScheme();
        // Add the data instructions if provided as part of the IcebergReadInstructions, or else attempt to create
        // data instructions from the properties collection and URI scheme.
        // final Object specialInstructions = instructions.dataInstructions()
        // .orElseGet(() -> dataInstructionsProvider.load(uriScheme));
        this.parquetInstructions = Objects.requireNonNull(pi);
        this.channelsProvider = Objects.requireNonNull(channelsProvider);
    }

    abstract IcebergTableLocationKey keyFromDataFile(ManifestFile manifestFile, PartitionSpec spec, DataFile dataFile,
            URI fileUri);

    private static String path(String path, FileIO io) {
        return io instanceof RelativeFileIO ? ((RelativeFileIO) io).absoluteLocation(path) : path;
    }

    // todo
    public static URI locationUri(Table table) {
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
        try (final Stream<ManifestFile> manifestFiles = allManifestFiles(table, snapshot)) {
            manifestFiles.forEach(manifestFile -> {
                final ManifestReader<DataFile> reader = ManifestFiles.read(manifestFile, table.io());

                // Note: this spec contains the *latest* Schema at the time the ManifestFile was produced, and not the
                // actual Schema the producer may have used to write the DataFiles.
                // https://lists.apache.org/thread/98m6d7b08fzxkbxlm78c5tnx5zp93mgc
                final PartitionSpec spec = reader.spec();

                IcebergUtils.toStream(reader)
                        .map(dataFile -> {
                            final URI fileUri = dataFileUri(table, dataFile);
                            if (!uriScheme.equals(fileUri.getScheme())) {
                                throw new TableDataException(String.format(
                                        "%s:%d - multiple URI schemes are not currently supported. uriScheme=%s, " +
                                                "fileUri=%s",
                                        table, snapshot.snapshotId(), uriScheme, fileUri));
                            }
                            return keyFromDataFile(manifestFile, spec, dataFile, fileUri);
                        })
                        .forEach(locationKeyObserver);
            });
        } catch (final RuntimeException e) {
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
