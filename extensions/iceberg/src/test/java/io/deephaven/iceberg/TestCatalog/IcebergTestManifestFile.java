package io.deephaven.iceberg.TestCatalog;

import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IcebergTestManifestFile implements ManifestFile {
    private static String path;
    private final ManifestContent content;
    private final List<String> files;
    private final IcebergTestSnapshot snapshot;
    private final long sequenceNumber;
    private final long minSequenceNumber;

    private IcebergTestManifestFile(
            final ManifestContent content,
            final IcebergTestSnapshot snapshot,
            final long sequenceNumber,
            final long minSequenceNumber) {
        this.content = content;
        this.snapshot = snapshot;
        this.partitionSpec = partitionSpec;
        this.sequenceNumber = sequenceNumber;
        this.minSequenceNumber = minSequenceNumber;

        path = UUID.randomUUID() + ".manifest";
        files = new ArrayList<>();
    }

    public static IcebergTestManifestFile create(
            final ManifestContent content,
            final IcebergTestSnapshot snapshot,
            final PartitionSpec partitionSpec) {
        return new IcebergTestManifestFile(content, snapshot, partitionSpec, 0,0);
    }

    public static IcebergTestManifestFile create(
            final ManifestContent content,
            final IcebergTestSnapshot snapshot,
            final PartitionSpec partitionSpec,
            final long sequenceNumber,
            final long minSequenceNumber) {
        return new IcebergTestManifestFile(content, snapshot, partitionSpec, sequenceNumber, minSequenceNumber);
    }


    public void addFile(String dataFilePath) {
        files.add(dataFilePath);
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public long length() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int partitionSpecId() {
        return snapshot.partitionSpecId;
    }

    @Override
    public ManifestContent content() {
        return content;
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public long minSequenceNumber() {
        return minSequenceNumber;
    }

    @Override
    public Long snapshotId() {
        return snapshot.snapshotId();
    }

    @Override
    public Integer addedFilesCount() {
        return files.size();
    }

    @Override
    public Long addedRowsCount() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Integer existingFilesCount() {
        return files.size();
    }

    @Override
    public Long existingRowsCount() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Integer deletedFilesCount() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Long deletedRowsCount() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ManifestFile copy() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
