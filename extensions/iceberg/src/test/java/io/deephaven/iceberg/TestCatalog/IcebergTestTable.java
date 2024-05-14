//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.TestCatalog;

import org.apache.iceberg.*;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IcebergTestTable implements Table {
    private final TableMetadata metadata;
    private final FileIO fileIO;

    private IcebergTestTable(@NotNull final String path, @NotNull final FileIO fileIO) {
        final File metadataRoot = new File(path, "metadata");
        this.fileIO = fileIO;

        final List<String> metadataFiles = new ArrayList<>();

        // Get a list of the JSON files.
        for (final File file : metadataRoot.listFiles()) {
            if (!file.isDirectory() && file.getName().endsWith(".json")) {
                metadataFiles.add(file.getAbsolutePath());
            }
        }

        // The last entry after sorting will be the newest / current.
        metadataFiles.sort(String::compareTo);
        final Path tablePath = Path.of(metadataFiles.get(metadataFiles.size() - 1));
        try {
            final String tableJson = new String(java.nio.file.Files.readAllBytes(tablePath));
            metadata = TableMetadataParser.fromJson(tableJson);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read table file: " + tablePath, e);
        }
    }

    public static IcebergTestTable loadFromMetadata(@NotNull final String path, @NotNull final FileIO fileIO) {
        return new IcebergTestTable(path, fileIO);
    }

    @Override
    public void refresh() {}

    @Override
    public TableScan newScan() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Schema schema() {
        return metadata.schema();
    }

    @Override
    public Map<Integer, Schema> schemas() {
        final Map<Integer, Schema> schemaMap = new java.util.HashMap<>();
        final List<Schema> schemas = metadata.schemas();
        for (int i = 0; i < schemas.size(); i++) {
            schemaMap.put(i, schemas.get(i));
        }
        return schemaMap;
    }

    @Override
    public PartitionSpec spec() {
        return metadata.spec();
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        final List<PartitionSpec> partitionSpecs = metadata.specs();
        final Map<Integer, PartitionSpec> specMap = new java.util.HashMap<>();
        for (int i = 0; i < partitionSpecs.size(); i++) {
            specMap.put(i, partitionSpecs.get(i));
        }
        return specMap;
    }

    @Override
    public SortOrder sortOrder() {
        return metadata.sortOrder();
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
        final List<SortOrder> sortOrders = metadata.sortOrders();
        final Map<Integer, SortOrder> sortOrderMap = new java.util.HashMap<>();
        for (int i = 0; i < sortOrders.size(); i++) {
            sortOrderMap.put(i, sortOrders.get(i));
        }
        return sortOrderMap;
    }

    @Override
    public Map<String, String> properties() {
        return metadata.properties();
    }

    @Override
    public String location() {
        return metadata.location();
    }

    @Override
    public Snapshot currentSnapshot() {
        return metadata.currentSnapshot();
    }

    @Override
    public Snapshot snapshot(long l) {
        final List<Snapshot> snapshots = metadata.snapshots();
        for (final Snapshot snapshot : snapshots) {
            if (snapshot.snapshotId() == l) {
                return snapshot;
            }
        }
        return null;
    }

    @Override
    public Iterable<Snapshot> snapshots() {
        return metadata.snapshots();
    }

    @Override
    public List<HistoryEntry> history() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public UpdateSchema updateSchema() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public UpdateProperties updateProperties() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public UpdateLocation updateLocation() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public AppendFiles newAppend() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public RewriteFiles newRewrite() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public RewriteManifests rewriteManifests() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public OverwriteFiles newOverwrite() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public RowDelta newRowDelta() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public DeleteFiles newDelete() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public ManageSnapshots manageSnapshots() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Transaction newTransaction() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    public EncryptionManager encryption() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public LocationProvider locationProvider() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public List<StatisticsFile> statisticsFiles() {
        return metadata.statisticsFiles();
    }

    @Override
    public Map<String, SnapshotRef> refs() {
        throw new NotImplementedException("Not implemented");
    }
}
