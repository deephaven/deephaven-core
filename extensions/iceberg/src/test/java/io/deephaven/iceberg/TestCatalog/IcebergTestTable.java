package io.deephaven.iceberg.TestCatalog;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IcebergTestTable implements Table {
    private final TableIdentifier tableIdentifier;

    private final List<Schema> schemas;
    private final List<PartitionSpec> partitionSpecs;
    private final List<Snapshot> snapshots;

    private IcebergTestTable(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Schema initialSchema,
            @NotNull final PartitionSpec initialPartitionSpec,
            @NotNull final Snapshot initialSnapshot) {
        this.tableIdentifier = tableIdentifier;

        schemas = new ArrayList<>(1);
        schemas.add(initialSchema);

        partitionSpecs = new ArrayList<>(1);
        partitionSpecs.add(initialPartitionSpec);

        snapshots = new ArrayList<>(1);
        snapshots.add(initialSnapshot);
    }

    public static IcebergTestTable create(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Schema initialSchema,
            @NotNull final PartitionSpec initialPartitionSpec,
            @NotNull final IcebergTestSnapshot initialSnapshot) {
        return new IcebergTestTable(tableIdentifier, initialSchema, initialPartitionSpec, initialSnapshot);
    }

    public static IcebergTestTable create(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Schema initialSchema,
            @NotNull final IcebergTestSnapshot initialSnapshot) {
        final PartitionSpec spec = PartitionSpec.builderFor(initialSchema).build();
        return new IcebergTestTable(tableIdentifier, initialSchema, spec, initialSnapshot);
    }

    @Override
    public void refresh() {
    }

    @Override
    public TableScan newScan() {
        throw new NotImplementedException("Not implemented");

    }

    @Override
    public Schema schema() {
        // return newest
        return schemas.get(schemas.size() - 1);
    }

    @Override
    public Map<Integer, Schema> schemas() {
        final Map<Integer, Schema> schemaMap = new java.util.HashMap<>();
        for (int i = 0; i < schemas.size(); i++) {
            schemaMap.put(i, schemas.get(i));
        }
        return schemaMap;
    }

    @Override
    public PartitionSpec spec() {
        // return newest
        return partitionSpecs.get(partitionSpecs.size() - 1);
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        final Map<Integer, PartitionSpec> specMap = new java.util.HashMap<>();
        for (int i = 0; i < partitionSpecs.size(); i++) {
            specMap.put(i, partitionSpecs.get(i));
        }
        return specMap;
    }

    @Override
    public SortOrder sortOrder() {
        throw new NotImplementedException("Not implemented");

    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Map<String, String> properties() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public String location() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Snapshot currentSnapshot() {
        return snapshots.get(snapshots.size() - 1);
    }

    @Override
    public Snapshot snapshot(long l) {
        for (final Snapshot snapshot : snapshots) {
            if (snapshot.snapshotId() == l) {
                return snapshot;
            }
        }
        return null;
    }

    @Override
    public Iterable<Snapshot> snapshots() {
        return snapshots;
    }

    @Override
    public List<HistoryEntry> history() {
        return List.of();
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
        throw new NotImplementedException("Not implemented");

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
        return List.of();
    }

    @Override
    public Map<String, SnapshotRef> refs() {
        return Map.of();
    }
}
