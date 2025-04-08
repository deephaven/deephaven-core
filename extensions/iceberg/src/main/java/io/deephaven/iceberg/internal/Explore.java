//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.api.Selectable;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * <pre>
 * import jpy
 * from deephaven.table import Table
 * from deephaven.experimental.iceberg import IcebergTableAdapter
 *
 * _JExplore = jpy.get_type("io.deephaven.iceberg.internal.Explore")
 *
 *
 * def snapshots(ita: IcebergTableAdapter) -> Table:
 *     return Table(_JExplore.snapshots(ita.j_object.icebergTable()))
 *
 *
 * def manifest_files(ita: IcebergTableAdapter) -> Table:
 *     return Table(_JExplore.manifestFiles(ita.j_object.icebergTable()))
 *
 *
 * def data_files(ita: IcebergTableAdapter) -> Table:
 *     return Table(_JExplore.dataFiles(ita.j_object.icebergTable()))
 *
 *
 * def refs(ita: IcebergTableAdapter) -> Table:
 *     return Table(_JExplore.refs(ita.j_object.icebergTable()))
 *
 *
 * def properties(ita: IcebergTableAdapter) -> Table:
 *     return Table(_JExplore.properties(ita.j_object.icebergTable()))
 *
 *
 * def statistics_files(ita: IcebergTableAdapter) -> Table:
 *     return Table(_JExplore.statisticsFiles(ita.j_object.icebergTable()))
 *
 *
 * def partition_statistics_files(ita: IcebergTableAdapter) -> Table:
 *     return Table(_JExplore.partitionStatisticsFiles(ita.j_object.icebergTable()))
 * </pre>
 */
@InternalUseOnly
public final class Explore {

    public static Table snapshots(org.apache.iceberg.Table table) {
        final List<Snapshot> snapshots = new ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        return snapshots(snapshots);
    }

    public static Table snapshots(Collection<Snapshot> snapshots) {
        return new TableBuilder<>("SnapshotObject", Snapshot.class)
                .add("SnapshotId", long.class, Snapshot::snapshotId)
                .add("SequenceNumber", long.class, Snapshot::sequenceNumber)
                .add("Timestamp", Instant.class, x -> Instant.ofEpochMilli(x.timestampMillis()))
                .add("Operation", String.class, Snapshot::operation)
                .add("Summary", Map.class, Snapshot::summary)
                .add("ParentId", long.class, Snapshot::parentId)
                .add("ManifestListLocation", String.class, Snapshot::manifestListLocation)
                .add("FirstRowId", long.class, Snapshot::firstRowId)
                .add("AddedRows", long.class, Snapshot::addedRows)
                .view(snapshots);
    }

    public static Table manifestFiles(org.apache.iceberg.Table table) {
        return manifestFiles(manifestFilesDeduped(table.snapshots(), table.io()));
    }

    public static Table manifestFiles(Snapshot snapshot, FileIO io) {
        return manifestFiles(snapshot.allManifests(io));
    }

    public static Table manifestFiles(Collection<ManifestFile> manifestFiles) {
        return new TableBuilder<>("ManifestFile", ManifestFile.class)
                .add("Path", String.class, ManifestFile::path)
                .add("Length", long.class, ManifestFile::length)
                .add("PartitionSpecId", int.class, ManifestFile::partitionSpecId)
                .add("Content", ManifestContent.class, ManifestFile::content)
                .add("SequenceNumber", long.class, ManifestFile::sequenceNumber)
                .add("MinSequenceNumber", long.class, ManifestFile::minSequenceNumber)
                .add("OrigSnapshotId", long.class, ManifestFile::snapshotId)
                .add("AddedFilesCount", int.class, ManifestFile::addedFilesCount)
                .add("AddedRowsCount", long.class, ManifestFile::addedRowsCount)
                .add("ExistingFilesCount", int.class, ManifestFile::existingFilesCount)
                .add("ExistingRowsCount", long.class, ManifestFile::existingRowsCount)
                .add("DeletedFilesCount", int.class, ManifestFile::deletedFilesCount)
                .add("DeletedRowsCount", long.class, ManifestFile::deletedRowsCount)
                .view(manifestFiles);
    }

    public static Table dataFiles(org.apache.iceberg.Table table) throws IOException {
        return dataFiles(table.snapshots(), table.io());
    }

    public static Table dataFiles(Iterable<Snapshot> snapshots, FileIO io) throws IOException {
        final List<DataFile> dataFiles = new ArrayList<>();
        for (final ManifestFile manifestFile : manifestFilesDeduped(snapshots, io)) {
            dataFiles.addAll(readDataFiles(manifestFile, io));
        }
        return dataFiles(dataFiles);
    }

    public static Table dataFiles(Snapshot snapshot, FileIO io) throws IOException {
        final List<DataFile> dataFiles = new ArrayList<>();
        // We don't expect any duplicated ManifestFiles from a single snapshot
        for (final ManifestFile allManifest : snapshot.allManifests(io)) {
            dataFiles.addAll(readDataFiles(allManifest, io));
        }
        return dataFiles(dataFiles);
    }

    public static Table dataFiles(ManifestFile manifestFile, FileIO io) throws IOException {
        return dataFiles(readDataFiles(manifestFile, io));
    }

    public static Table dataFiles(Collection<DataFile> dataFiles) {
        return new TableBuilder<>("DataFile", DataFile.class)
                .add("Path", String.class, ContentFile::location)
                .add("Pos", long.class, ContentFile::pos)
                .add("SpecId", int.class, ContentFile::specId)
                .add("Format", FileFormat.class, ContentFile::format)
                .add("Partition", StructLike.class, ContentFile::partition)
                .add("RecordCount", long.class, ContentFile::recordCount)
                .add("FileSize", long.class, ContentFile::fileSizeInBytes)
                // todo bunch of stuff
                .add("SortOrderId", int.class, ContentFile::sortOrderId)
                .add("DataSequenceNumber", long.class, ContentFile::dataSequenceNumber)
                .add("FileSequenceNumber", long.class, ContentFile::fileSequenceNumber)
                .view(dataFiles);
    }

    public static Table refs(org.apache.iceberg.Table table) {
        return refs(table.refs());
    }

    public static Table refs(Map<String, SnapshotRef> refs) {
        return new TableBuilder<>("Entry", (Class<Map.Entry<String, SnapshotRef>>) (Class) Map.Entry.class)
                .add("Ref", String.class, Map.Entry::getKey)
                .add("RefType", String.class, e -> e.getValue().isBranch() ? "BRANCH" : "TAG")
                .add("SnapshotId", long.class, e -> e.getValue().snapshotId())
                .add("MinSnapshotsToKeep", int.class, e -> e.getValue().minSnapshotsToKeep())
                .add("MaxSnapshotAgeMs", long.class, e -> e.getValue().maxSnapshotAgeMs())
                .add("MaxRefAgeMs", long.class, e -> e.getValue().maxRefAgeMs())
                .view(refs.entrySet());
    }

    public static Table statisticsFiles(org.apache.iceberg.Table table) {
        return statisticsFiles(table.statisticsFiles());
    }

    public static Table statisticsFiles(Collection<StatisticsFile> values) {
        return new TableBuilder<>("StatisticsFile", StatisticsFile.class)
                .add("SnapshotId", long.class, StatisticsFile::snapshotId)
                .add("Path", String.class, StatisticsFile::path)
                .add("FileSize", long.class, StatisticsFile::fileSizeInBytes)
                .add("FileFooterSize", long.class, StatisticsFile::fileFooterSizeInBytes)
                .view(values);
    }

    public static Table partitionStatisticsFiles(org.apache.iceberg.Table table) {
        return partitionStatisticsFiles(table.partitionStatisticsFiles());
    }

    public static Table partitionStatisticsFiles(Collection<PartitionStatisticsFile> values) {
        return new TableBuilder<>("PartitionStatisticsFile", PartitionStatisticsFile.class)
                .add("SnapshotId", long.class, PartitionStatisticsFile::snapshotId)
                .add("Path", String.class, PartitionStatisticsFile::path)
                .add("FileSize", long.class, PartitionStatisticsFile::fileSizeInBytes)
                .view(values);
    }

    public static Table properties(org.apache.iceberg.Table table) {
        return properties(table.properties());
    }

    public static Table properties(Map<String, String> properties) {
        return new TableBuilder<>("Entry", (Class<Map.Entry<String, String>>) (Class) Map.Entry.class)
                .add("Key", String.class, Map.Entry::getKey)
                .add("Value", String.class, Map.Entry::getValue)
                .view(properties.entrySet());
    }

    public static Collection<ManifestFile> manifestFilesDeduped(Iterable<Snapshot> snapshots, FileIO io) {
        final Map<String, ManifestFile> deduped = new LinkedHashMap<>();
        for (final Snapshot snapshot : snapshots) {
            final List<ManifestFile> files = snapshot.allManifests(io);
            for (final ManifestFile manifestFile : files) {
                deduped.putIfAbsent(manifestFile.path(), manifestFile);
            }
        }
        return deduped.values();
    }

    public static List<DataFile> readDataFiles(ManifestFile manifestFile, FileIO io) throws IOException {
        final List<DataFile> dataFiles = new ArrayList<>();
        try (final ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifestFile, io)) {
            for (final DataFile dataFile : manifestReader) {
                dataFiles.add(dataFile);
            }
        }
        return dataFiles;
    }

    private static class TableBuilder<S> {
        private final String name;
        private final Class<S> dataType;
        private final List<Selectable> views;

        TableBuilder(String name, Class<S> dataType) {
            if (dataType.isPrimitive()) {
                throw new IllegalArgumentException();
            }
            this.name = Objects.requireNonNull(name);
            this.dataType = Objects.requireNonNull(dataType);
            this.views = new ArrayList<>();
        }

        private Table table(Collection<S> values) {
            return TableTools.newTable(
                    TableDefinition.of(ColumnDefinition.of(name, io.deephaven.qst.type.Type.find(dataType))),
                    holder(values));
        }

        private ColumnHolder<S> holder(Collection<S> values) {
            // We know S is not primitive type, so cast is ok
            // noinspection unchecked
            return new ColumnHolder<>(name, dataType, null, false,
                    values.toArray(x -> (S[]) Array.newInstance(dataType, x)));
        }

        public <D> TableBuilder<S> add(String name, Class<D> type, Function<S, D> f) {
            views.add(new FunctionalColumn<>(this.name, dataType, name, type, f));
            return this;
        }

        public Table view(Collection<S> values) {
            return table(values).view(views);
        }
    }
}
