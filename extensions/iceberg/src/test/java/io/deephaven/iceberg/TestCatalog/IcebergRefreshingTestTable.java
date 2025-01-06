//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.TestCatalog;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.*;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Test table wrapper that restricts available snapshots to a given sequence number which the user can advance using
 * {@link #advanceSequenceNumber()}.
 */
public class IcebergRefreshingTestTable implements Table {
    private final IcebergTestTable testTable;

    private long currentSnapshotSequenceNumber;

    private IcebergRefreshingTestTable(final IcebergTestTable testTable) {
        this.testTable = testTable;
        currentSnapshotSequenceNumber = 1;
    }

    public static IcebergRefreshingTestTable fromTestTable(final IcebergTestTable testTable) {
        return new IcebergRefreshingTestTable(testTable);
    }

    public void advanceSequenceNumber() {
        currentSnapshotSequenceNumber++;
    }

    @Override
    public void refresh() {}

    @Override
    public TableScan newScan() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Schema schema() {
        return testTable.schema();
    }

    @Override
    public Map<Integer, Schema> schemas() {
        return testTable.schemas();
    }

    @Override
    public PartitionSpec spec() {
        return testTable.spec();
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        return testTable.specs();

    }

    @Override
    public SortOrder sortOrder() {
        return testTable.sortOrder();
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
        return testTable.sortOrders();

    }

    @Override
    public Map<String, String> properties() {
        return testTable.properties();
    }

    @Override
    public String location() {
        return testTable.location();
    }

    @Override
    public Snapshot currentSnapshot() {
        Snapshot snapshot = null;
        for (final Snapshot s : snapshots()) {
            snapshot = s; // grab the last snapshot
        }
        return snapshot;
    }

    @Override
    public Snapshot snapshot(long l) {
        for (final Snapshot snapshot : snapshots()) {
            if (snapshot.snapshotId() == l) {
                return snapshot;
            }
        }
        return null;
    }

    @Override
    public Iterable<Snapshot> snapshots() {
        final List<Snapshot> snapshots = new ArrayList<>();
        for (final Snapshot s : testTable.snapshots()) {
            if (s.sequenceNumber() <= currentSnapshotSequenceNumber) {
                snapshots.add(s);
            }
        }
        return snapshots;
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
        return testTable.io();
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
        return testTable.statisticsFiles();
    }

    @Override
    public Map<String, SnapshotRef> refs() {
        throw new NotImplementedException("Not implemented");
    }
}
