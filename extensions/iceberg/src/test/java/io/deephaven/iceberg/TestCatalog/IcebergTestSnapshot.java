package io.deephaven.iceberg.TestCatalog;

import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class IcebergTestSnapshot implements Snapshot {
    private long sequenceNumber;
    private long snapshotId;
    private final PartitionSpec partitionSpec;

    int schemaId;
    int partitionSpecId;

    private List<ManifestFile> dataManifests;
    private List<ManifestFile> deleteManifests;

    private IcebergTestSnapshot(
            final long sequenceNumber,
            final int schemaId,
            final PartitionSpec partitionSpec,
            final int partitionSpecId) {
        this.sequenceNumber = sequenceNumber;
        this.snapshotId = ThreadLocalRandom.current().nextLong(1_000_000_000);
        this.partitionSpec = partitionSpec;
        this.schemaId = schemaId;
        this.partitionSpecId = partitionSpecId;

        dataManifests = new ArrayList<>();
        deleteManifests = new ArrayList<>();
    }

    public static IcebergTestSnapshot create(final long sequenceNumber) {

        return new IcebergTestSnapshot(sequenceNumber, 0, 0);
    }

    public static IcebergTestSnapshot create(
            final long sequenceNumber,
            final int schemaId,
            final PartitionSpec partitionSpec,
            final int partitionSpecId) {

        return new IcebergTestSnapshot(sequenceNumber, schemaId, partitionSpec, partitionSpecId);
    }


    public void addDataManifest(final IcebergTestFileIO fileIO, final String... dataFilePaths) {
        final IcebergTestManifestFile manifest =
                IcebergTestManifestFile.create(ManifestContent.DATA, this);
        for (String dataFilePath : dataFilePaths) {
            manifest.addFile(dataFilePath);
        }
        addDataManifest(fileIO, manifest);
    }

    public void addDataManifest(final IcebergTestFileIO fileIO, final ManifestFile manifest) {
        dataManifests.add(manifest);
        fileIO.addManifestFile(manifest.path(), (IcebergTestManifestFile) manifest);
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public long snapshotId() {
        return snapshotId;
    }

    @Override
    public Long parentId() {
        return (long)0;
    }

    @Override
    public Integer schemaId() {
        return schemaId;
    }

    @Override
    public long timestampMillis() {
        return 0;
    }

    @Override
    public List<ManifestFile> allManifests(FileIO fileIO) {
        final List<ManifestFile> allManifests = new ArrayList<>(dataManifests.size() + deleteManifests.size());
        allManifests.addAll(dataManifests);
        allManifests.addAll(deleteManifests);
        return allManifests;
    }

    @Override
    public List<ManifestFile> dataManifests(FileIO fileIO) {
        return dataManifests;
    }

    @Override
    public List<ManifestFile> deleteManifests(FileIO fileIO) {
        return deleteManifests;
    }

    @Override
    public String operation() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Map<String, String> summary() {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Iterable<DataFile> addedDataFiles(FileIO fileIO) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Iterable<DataFile> removedDataFiles(FileIO fileIO) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public String manifestListLocation() {
        throw new NotImplementedException("Not implemented");
    }
}
