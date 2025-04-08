//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.api.Selectable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.iceberg.internal.Inference;
import io.deephaven.iceberg.internal.Shim;
import io.deephaven.iceberg.layout.IcebergAutoRefreshTableLocationProvider;
import io.deephaven.iceberg.layout.IcebergBaseLayout;
import io.deephaven.iceberg.layout.IcebergFlatLayout;
import io.deephaven.iceberg.layout.IcebergManualRefreshTableLocationProvider;
import io.deephaven.iceberg.layout.IcebergStaticTableLocationProvider;
import io.deephaven.iceberg.layout.IcebergTableLocationProviderBase;
import io.deephaven.iceberg.location.IcebergTableLocationFactory;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.iceberg.base.IcebergUtils.convertToDHType;

/**
 * This class manages an Iceberg {@link org.apache.iceberg.Table table} and provides methods to interact with it.
 */
public class IcebergTableAdapter {
    @VisibleForTesting
    static final TableDefinition SNAPSHOT_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("Id"),
            ColumnDefinition.ofTime("Timestamp"),
            ColumnDefinition.ofString("Operation"),
            ColumnDefinition.fromGenericType("Summary", Map.class),
            ColumnDefinition.fromGenericType("SnapshotObject", Snapshot.class));

    private static final Set<String> S3_SCHEMES = Set.of("s3", "s3a", "s3n");

    private final Catalog catalog;
    private final org.apache.iceberg.Table table;
    private final TableIdentifier tableIdentifier;
    private final DataInstructionsProviderLoader dataInstructionsProviderLoader;

    private final URI locationUri;

    public IcebergTableAdapter(
            final Catalog catalog,
            final TableIdentifier tableIdentifier,
            final org.apache.iceberg.Table table,
            final DataInstructionsProviderLoader dataInstructionsProviderLoader) {
        this.catalog = catalog;
        this.table = table;
        this.tableIdentifier = tableIdentifier;
        this.dataInstructionsProviderLoader = dataInstructionsProviderLoader;
        this.locationUri = IcebergUtils.locationUri(table);
    }

    /**
     * {@link Catalog} used to access this table.
     */
    public Catalog catalog() {
        // TODO: this should go
        return catalog;
    }

    /**
     * Get the Iceberg {@link TableIdentifier table identifier}.
     */
    public TableIdentifier tableIdentifier() {
        return tableIdentifier;
    }

    /**
     * Get the current {@link Snapshot snapshot} of a given Iceberg table or {@code null} if there are no snapshots.
     *
     * @return The current snapshot of the table or {@code null} if there are no snapshots.
     */
    public synchronized Snapshot currentSnapshot() {
        // Refresh the table to update the current snapshot.
        refresh();
        return table.currentSnapshot();
    }

    /**
     * Get the current list of all {@link Snapshot snapshots} of the Iceberg table.
     *
     * @return A list of all snapshots of the given table.
     */
    public synchronized List<Snapshot> listSnapshots() {
        // Refresh the table to update the snapshot list.
        refresh();
        return getSnapshots();
    }

    /**
     * Get a list of all {@link Snapshot snapshots} of the Iceberg table (without refreshing).
     *
     * @return A list of all snapshots of the given table.
     */
    private List<Snapshot> getSnapshots() {
        final List<Snapshot> snapshots = new ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        return snapshots;
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the following columns:
     * <table>
     * <caption></caption>
     * <tr>
     * <th>Column Name</th>
     * <th>Description</th>
     * </tr>
     * <tr>
     * <td>Id</td>
     * <td>The snapshot identifier (can be used for updating the table or loading a specific snapshot)</td>
     * </tr>
     * <tr>
     * <td>Timestamp</td>
     * <td>The timestamp of the snapshot</td>
     * </tr>
     * <tr>
     * <td>Operation</td>
     * <td>The data operation that created this snapshot</td>
     * </tr>
     * <tr>
     * <td>Summary</td>
     * <td>Additional information about the snapshot from the Iceberg metadata</td>
     * </tr>
     * <tr>
     * <td>SnapshotObject</td>
     * <td>A Java object containing the Iceberg API snapshot</td>
     * </tr>
     * <tr>
     * </tr>
     * </table>
     *
     * @return A Table containing a list of all tables in the given namespace.
     */
    public Table snapshots() {
        // Retrieve the current list of snapshots
        final List<Snapshot> snapshots = listSnapshots();
        final long size = snapshots.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final long[] idArr = new long[(int) size];
        columnSourceMap.put("Id", InMemoryColumnSource.getImmutableMemoryColumnSource(idArr, long.class, null));

        final long[] timestampArr = new long[(int) size];
        columnSourceMap.put("Timestamp",
                InMemoryColumnSource.getImmutableMemoryColumnSource(timestampArr, Instant.class, null));

        final String[] operatorArr = new String[(int) size];
        columnSourceMap.put("Operation",
                InMemoryColumnSource.getImmutableMemoryColumnSource(operatorArr, String.class, null));

        final Map<String, String>[] summaryArr = new Map[(int) size];
        columnSourceMap.put("Summary",
                InMemoryColumnSource.getImmutableMemoryColumnSource(summaryArr, Map.class, null));

        final Snapshot[] snapshotArr = new Snapshot[(int) size];
        columnSourceMap.put("SnapshotObject",
                InMemoryColumnSource.getImmutableMemoryColumnSource(snapshotArr, Snapshot.class, null));

        // Populate the column source(s)
        for (int i = 0; i < size; i++) {
            final Snapshot snapshot = snapshots.get(i);
            idArr[i] = snapshot.snapshotId();
            // Provided as millis from epoch, convert to nanos
            timestampArr[i] = DateTimeUtils.millisToNanos(snapshot.timestampMillis());
            operatorArr[i] = snapshot.operation();
            summaryArr[i] = snapshot.summary();
            snapshotArr[i] = snapshot;
        }

        // Create and return the table
        return new QueryTable(SNAPSHOT_DEFINITION, RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    public Table snapshots2() {
        final List<Snapshot> l = new ArrayList<>();
        table.snapshots().forEach(l::add);
        final Snapshot[] snapshots = l.toArray(new Snapshot[0]);
        final String snapshot = "SnapshotObject";
        final Class<Snapshot> dataType = Snapshot.class;
        final TableDefinition td =
                TableDefinition.of(ColumnDefinition.of(snapshot, io.deephaven.qst.type.Type.find(dataType)));
        final Table snapshotsTable =
                TableTools.newTable(td, new ColumnHolder<>(snapshot, dataType, null, false, snapshots));
        return snapshotsTable.updateView(List.of(
                new FunctionalColumn<>(snapshot, dataType, "SequenceNumber", long.class, Snapshot::sequenceNumber),
                new FunctionalColumn<>(snapshot, dataType, "Id", long.class, Snapshot::snapshotId),
                new FunctionalColumn<>(snapshot, dataType, "Timestamp", Instant.class,
                        x -> Instant.ofEpochMilli(x.timestampMillis())),
                new FunctionalColumn<>(snapshot, dataType, "Operation", String.class, Snapshot::operation),
                new FunctionalColumn<>(snapshot, dataType, "Summary", Map.class, Snapshot::summary),
                new FunctionalColumn<>(snapshot, dataType, "ParentId", long.class, Snapshot::parentId),
                new FunctionalColumn<>(snapshot, dataType, "ManifestListLocation", String.class,
                        Snapshot::manifestListLocation),
                new FunctionalColumn<>(snapshot, dataType, "FirstRowId", long.class, Snapshot::firstRowId),
                new FunctionalColumn<>(snapshot, dataType, "AddedRows", long.class, Snapshot::addedRows)));
    }

    public Table statisticsFile() {
        final StatisticsFile[] files = table.statisticsFiles().toArray(new StatisticsFile[0]);
        final String file = "StatisticsFile";
        final Class<StatisticsFile> dataType = StatisticsFile.class;
        final TableDefinition td =
                TableDefinition.of(ColumnDefinition.of(file, io.deephaven.qst.type.Type.find(dataType)));
        final Table table = TableTools.newTable(td, new ColumnHolder<>(file, dataType, null, false, files));
        return table.updateView(List.of(
                new FunctionalColumn<>(file, dataType, "SnapshotId", long.class, StatisticsFile::snapshotId),
                new FunctionalColumn<>(file, dataType, "Path", String.class, StatisticsFile::path),
                new FunctionalColumn<>(file, dataType, "FileSize", long.class, StatisticsFile::fileSizeInBytes),
                new FunctionalColumn<>(file, dataType, "FileFooterSize", long.class,
                        StatisticsFile::fileFooterSizeInBytes)));
    }

    public Table refs() {
        final Map.Entry[] entries = table.refs().entrySet().toArray(new Map.Entry[0]);
        final String entry = "Entry";
        final Class<Map.Entry> dataType = Map.Entry.class;
        final TableDefinition td =
                TableDefinition.of(ColumnDefinition.of(entry, io.deephaven.qst.type.Type.find(dataType)));
        final Table table = TableTools.newTable(td, new ColumnHolder<>(entry, dataType, null, false, entries));
        return table.view(List.of(
                new FunctionalColumn<>(entry, dataType, "Ref", String.class, e -> (String) e.getKey()),
                new FunctionalColumn<>(entry, dataType, "RefType", String.class,
                        e -> ((SnapshotRef) e.getValue()).isBranch() ? "BRANCH" : "TAG"),
                new FunctionalColumn<>(entry, dataType, "SnapshotId", long.class,
                        e -> ((SnapshotRef) e.getValue()).snapshotId()),
                new FunctionalColumn<>(entry, dataType, "MinSnapshotsToKeep", int.class,
                        e -> ((SnapshotRef) e.getValue()).minSnapshotsToKeep()),
                new FunctionalColumn<>(entry, dataType, "maxSnapshotAgeMs", long.class,
                        e -> ((SnapshotRef) e.getValue()).maxSnapshotAgeMs()),
                new FunctionalColumn<>(entry, dataType, "MaxRefAgeMs", long.class,
                        e -> ((SnapshotRef) e.getValue()).maxRefAgeMs())));
    }

    public Table manifestFiles() {
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
                .view(manifestFilesMap().values());
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

        public Table updateView(Collection<S> values) {
            return table(values).updateView(views);
        }

        public Table view(Collection<S> values) {
            return table(values).view(views);
        }
    }

    private Map<String, ManifestFile> manifestFilesMap() {
        final Map<String, ManifestFile> deduped = new LinkedHashMap<>();
        for (final Snapshot snapshot : table.snapshots()) {
            final List<ManifestFile> files = snapshot.allManifests(table.io());
            for (final ManifestFile manifestFile : files) {
                deduped.putIfAbsent(manifestFile.path(), manifestFile);
            }
        }
        return deduped;
    }

    public Table dataFiles() throws IOException {
        final List<DataFile> dataFiles = new ArrayList<>();
        for (final ManifestFile manifestFile : manifestFilesMap().values()) {
            try (final ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifestFile, table.io())) {
                for (final DataFile dataFile : manifestReader) {
                    dataFiles.add(dataFile);
                }
            }
        }
        final DataFile[] files = dataFiles.toArray(new DataFile[0]);
        final String file = "DataFile";
        final Class<DataFile> dataType = DataFile.class;
        final TableDefinition td =
                TableDefinition.of(ColumnDefinition.of(file, io.deephaven.qst.type.Type.find(dataType)));
        final Table table = TableTools.newTable(td, new ColumnHolder<>(file, dataType, null, false, files));
        return table.updateView(List.of(
                new FunctionalColumn<>(file, dataType, "Path", String.class, ContentFile::location),
                new FunctionalColumn<>(file, dataType, "Pos", long.class, ContentFile::pos),
                new FunctionalColumn<>(file, dataType, "SpecId", int.class, ContentFile::specId),
                new FunctionalColumn<>(file, dataType, "Format", FileFormat.class, ContentFile::format),
                new FunctionalColumn<>(file, dataType, "Partition", StructLike.class, ContentFile::partition),
                new FunctionalColumn<>(file, dataType, "RecordCount", long.class, ContentFile::recordCount),
                new FunctionalColumn<>(file, dataType, "FileSize", long.class, ContentFile::fileSizeInBytes),
                // todo bunch of stuff
                new FunctionalColumn<>(file, dataType, "SortOrderId", int.class, ContentFile::sortOrderId),
                new FunctionalColumn<>(file, dataType, "DataSequenceNumber", long.class,
                        ContentFile::dataSequenceNumber),
                new FunctionalColumn<>(file, dataType, "FileSequenceNumber", long.class,
                        ContentFile::fileSequenceNumber)));
    }

    /**
     * Retrieve a specific {@link Snapshot snapshot} of an Iceberg table.
     *
     * @param snapshotId The identifier of the snapshot to load.
     *
     * @return An {@link Optional} containing the requested {@link Snapshot} if it exists.
     */
    private Optional<Snapshot> snapshot(final long snapshotId) {
        Optional<Snapshot> found = getSnapshots().stream()
                .filter(snapshot -> snapshot.snapshotId() == snapshotId)
                .findFirst();
        if (found.isEmpty()) {
            // Refresh the table to update the snapshot list, then try again.
            refresh();
            found = getSnapshots().stream()
                    .filter(snapshot -> snapshot.snapshotId() == snapshotId)
                    .findFirst();
        }
        return found;
    }

    /**
     * Retrieve the current {@link Schema schema} of an Iceberg table.
     */
    public synchronized Schema currentSchema() {
        refresh();
        return table.schema();
    }

    /**
     * Retrieve the current {@link Schema schema} of an Iceberg table.
     */
    public synchronized Map<Integer, Schema> schemas() {
        refresh();
        return Map.copyOf(table.schemas());
    }

    /**
     * Retrieve a specific {@link Schema schema} of an Iceberg table.
     *
     * @param schemaId The identifier of the schema to load.
     */
    public synchronized Optional<Schema> schema(final int schemaId) {
        Schema found = table.schemas().get(schemaId);
        if (found == null) {
            // Refresh the table to update the snapshot list, then try again.
            refresh();
            found = table.schemas().get(schemaId);
        }
        return Optional.ofNullable(found);
    }

    /**
     * Retrieves the appropriate {@link Snapshot} based on the provided {@link IcebergReadInstructions}, or {@code null}
     * if no {@link IcebergReadInstructions#snapshot() snapshot} or {@link IcebergReadInstructions#snapshotId()
     * snapshotId} is provided.
     */
    @InternalUseOnly
    @Nullable
    public Snapshot getSnapshot(@NotNull final IcebergReadInstructions readInstructions) {
        if (readInstructions.snapshot().isPresent()) {
            return readInstructions.snapshot().get();
        } else if (readInstructions.snapshotId().isPresent()) {
            return snapshot(readInstructions.snapshotId().getAsLong())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Snapshot with id " + readInstructions.snapshotId().getAsLong() + " not found for " +
                                    "table " + tableIdentifier));
        }
        return null;
    }

    private ResolverAndSnapshot resolverAndSnapshot(@NotNull final IcebergReadInstructions readInstructions) {
        final Resolver explicitResolver = readInstructions.resolver().orElse(null);
        final Snapshot explicitSnapshot = getSnapshot(readInstructions);
        if (explicitResolver != null && explicitSnapshot != null) {
            return new ResolverAndSnapshot(explicitResolver, explicitSnapshot);
        }
        final Resolver resolver;
        final Snapshot snapshot;
        if (explicitResolver == null) {
            final InferenceInstructions ii;
            if (explicitSnapshot == null) {
                // todo: not sure why we were sync and refreshing before?
                ii = InferenceInstructions.of(table.schema(), table.spec());
                snapshot = table.currentSnapshot();
            } else {
                ii = InferenceInstructions.of(table.schemas().get(explicitSnapshot.schemaId()), table.spec());
                snapshot = explicitSnapshot;
            }
            try {
                resolver = Resolver.infer(ii);
            } catch (Inference.UnsupportedType e) {
                throw new RuntimeException(e);
            }

            // todo:


        } else {
            Assert.eqNull(explicitSnapshot, "explicitSnapshot");
            resolver = explicitResolver;
            snapshot = table.currentSnapshot();
        }
        return new ResolverAndSnapshot(resolver, snapshot);
    }

    /**
     * Return {@link TableDefinition table definition} corresponding to this iceberg table
     *
     * @return The table definition
     */
    public TableDefinition definition() {
        return definition(IcebergReadInstructions.DEFAULT);
    }

    /**
     * Return {@link TableDefinition table definition} corresponding to this iceberg table
     *
     * @param readInstructions The instructions for customizations while reading the table.
     * @return The table definition
     */
    public TableDefinition definition(@NotNull final IcebergReadInstructions readInstructions) {
        return resolverAndSnapshot(readInstructions).resolver().definition();
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of this Iceberg table.
     *
     * @return The table definition as a Deephaven table
     */
    public Table definitionTable() {
        return definitionTable(IcebergReadInstructions.DEFAULT);
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of this Iceberg table.
     *
     * @param readInstructions The instructions for customizations while reading the table.
     * @return The table definition as a Deephaven table
     */
    public Table definitionTable(final IcebergReadInstructions readInstructions) {
        return TableTools.metaTable(definition(readInstructions));
    }

    /**
     * Read the latest snapshot of this Iceberg table from the Iceberg catalog as a Deephaven {@link Table table}.
     *
     * @return The loaded table
     */
    public IcebergTable table() {
        return table(IcebergReadInstructions.DEFAULT);
    }

    /**
     * Read a snapshot of this Iceberg table from the Iceberg catalog as a Deephaven {@link Table table}.
     *
     * @param readInstructions The instructions for customizations while reading the table.
     * @return The loaded table
     */
    public IcebergTable table(@NotNull final IcebergReadInstructions readInstructions) {
        final ResolverAndSnapshot ras = resolverAndSnapshot(readInstructions);
        final Resolver resolver = ras.resolver();
        final TableDefinition definition = resolver.definition();
        final IcebergBaseLayout keyFinder =
                keyFinder(resolver, ras.snapshot().orElse(null), readInstructions.dataInstructions().orElse(null));
        if (readInstructions.updateMode().updateType() == IcebergUpdateMode.IcebergUpdateType.STATIC) {
            final IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> locationProvider =
                    new IcebergStaticTableLocationProvider<>(
                            StandaloneTableKey.getInstance(),
                            keyFinder,
                            new IcebergTableLocationFactory(),
                            tableIdentifier);
            return new IcebergTableImpl(
                    definition,
                    tableIdentifier.toString(),
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    locationProvider,
                    null);
        }

        final UpdateSourceRegistrar updateSourceRegistrar = ExecutionContext.getContext().getUpdateGraph();
        final IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> locationProvider;

        if (readInstructions.updateMode().updateType() == IcebergUpdateMode.IcebergUpdateType.MANUAL_REFRESHING) {
            locationProvider = new IcebergManualRefreshTableLocationProvider<>(
                    StandaloneTableKey.getInstance(),
                    keyFinder,
                    new IcebergTableLocationFactory(),
                    this,
                    tableIdentifier);
        } else {
            locationProvider = new IcebergAutoRefreshTableLocationProvider<>(
                    StandaloneTableKey.getInstance(),
                    keyFinder,
                    new IcebergTableLocationFactory(),
                    TableDataRefreshService.getSharedRefreshService(),
                    readInstructions.updateMode().autoRefreshMs(),
                    this,
                    tableIdentifier);
        }

        return new IcebergTableImpl(
                definition,
                tableIdentifier.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                locationProvider,
                updateSourceRegistrar);
    }

    private @NotNull IcebergBaseLayout keyFinder(
            @NotNull final Resolver resolver,
            @Nullable final Snapshot snapshot,
            @Nullable final Object dataInstructions) {
        final Object specialInstructions;
        final SeekableChannelsProvider channelsProvider;
        {
            final String uriScheme = locationUri.getScheme();
            specialInstructions = dataInstructions == null
                    ? dataInstructionsProviderLoader.load(uriScheme)
                    : dataInstructions;
            channelsProvider = seekableChannelsProvider(uriScheme, specialInstructions);
        }
        final ParquetInstructions parquetInstructions = ParquetInstructions.builder()
                .setTableDefinition(resolver.definition())
                .setColumnResolverFactory(Shim.factory(resolver))
                .setSpecialInstructions(specialInstructions)
                .build();
        // todo: we should probably be checking TD instead
        final PartitionSpec spec = resolver.spec();
        if (spec.isUnpartitioned()) {
            // Create the flat layout location key finder
            return new IcebergFlatLayout(this, parquetInstructions, channelsProvider, snapshot);
        } else {
            throw new UnsupportedOperationException("TODO");
            // Create the partitioning column location key finder
            // return new IcebergKeyValuePartitionedLayout(this, parquetInstructions, channelsProvider, ss.snapshot,
            // identityPartitioningColumns());
        }
    }

    private static SeekableChannelsProvider seekableChannelsProvider(
            final String uriScheme,
            final Object specialInstructions) {
        final SeekableChannelsProviderLoader loader = SeekableChannelsProviderLoader.getInstance();
        return S3_SCHEMES.contains(uriScheme)
                ? loader.load(S3_SCHEMES, specialInstructions)
                : loader.load(uriScheme, specialInstructions);
    }

    // private static List<IcebergKeyValuePartitionedLayout.IdentityPartitioningColData> identityPartitioningColumns(
    // final PartitionSpec partitionSpec,
    // final Map<String, String> legalizedColumnRenames,
    // final TableDefinition tableDef) {
    // final List<IcebergKeyValuePartitionedLayout.IdentityPartitioningColData> identityPartitioningColumns;
    // // We can assume due to upstream validation that there are no duplicate names (after renaming) that are included
    // // in the output definition, so we can ignore duplicates.
    // final List<PartitionField> partitionFields = partitionSpec.fields();
    // final int numPartitionFields = partitionFields.size();
    // identityPartitioningColumns = new ArrayList<>(numPartitionFields);
    // for (int fieldId = 0; fieldId < numPartitionFields; ++fieldId) {
    // final PartitionField partitionField = partitionFields.get(fieldId);
    // if (!partitionField.transform().isIdentity()) {
    // // TODO (DH-18160): Improve support for handling non-identity transforms
    // continue;
    // }
    // final String icebergColName = partitionField.name();
    // final String dhColName = legalizedColumnRenames.getOrDefault(icebergColName, icebergColName);
    // final ColumnDefinition<?> columnDef = tableDef.getColumn(dhColName);
    // if (columnDef == null) {
    // // Table definition provided by the user doesn't have this column, so skip.
    // continue;
    // }
    // identityPartitioningColumns.add(new IcebergKeyValuePartitionedLayout.IdentityPartitioningColData(
    // dhColName, TypeUtils.getBoxedType(columnDef.getDataType()), fieldId));
    // }
    // return identityPartitioningColumns;
    // }

    /**
     * Refresh the table with the latest information from the Iceberg catalog, including new snapshots and schema.
     */
    public synchronized void refresh() {
        table.refresh();
    }

    /**
     * Return the underlying Iceberg table.
     */
    public org.apache.iceberg.Table icebergTable() {
        return table;
    }

    @Override
    public String toString() {
        return table.toString();
    }

    /**
     * Create a single {@link TableDefinition} from a given Schema, PartitionSpec, and TableDefinition. Takes into
     * account {@link Map<> column rename instructions}
     *
     * @param schema The schema of the table.
     * @param partitionSpec The partition specification of the table.
     * @param userTableDef The table definition.
     * @param columnRenameMap The map for renaming columns.
     * @return The generated TableDefinition.
     */
    private static TableDefinition fromSchema(
            @NotNull final Schema schema,
            @NotNull final PartitionSpec partitionSpec,
            @Nullable final TableDefinition userTableDef,
            @NotNull final Map<String, String> columnRenameMap) {

        final Set<String> columnNames = userTableDef != null
                ? userTableDef.getColumnNameSet()
                : null;

        final Set<String> partitionNames =
                partitionSpec.fields().stream()
                        .map(PartitionField::name)
                        .map(colName -> columnRenameMap.getOrDefault(colName, colName))
                        .collect(Collectors.toSet());

        final List<ColumnDefinition<?>> columns = new ArrayList<>();

        for (final Types.NestedField field : schema.columns()) {
            final String name = columnRenameMap.getOrDefault(field.name(), field.name());
            // Skip columns that are not in the provided table definition.
            if (columnNames != null && !columnNames.contains(name)) {
                continue;
            }
            final Type type = field.type();
            final io.deephaven.qst.type.Type<?> qstType = convertToDHType(type);
            final ColumnDefinition<?> column;
            if (partitionNames.contains(name)) {
                column = ColumnDefinition.of(name, qstType).withPartitioning();
            } else {
                column = ColumnDefinition.of(name, qstType);
            }
            columns.add(column);
        }

        final TableDefinition icebergTableDef = TableDefinition.of(columns);
        if (userTableDef == null) {
            return icebergTableDef;
        }

        // If the user supplied a table definition, make sure it's fully compatible.
        final TableDefinition tableDef = icebergTableDef.checkCompatibility(userTableDef);

        // Ensure that the user has not marked non-partitioned columns as partitioned.
        final Set<String> userPartitionColumns = userTableDef.getPartitioningColumns().stream()
                .map(ColumnDefinition::getName)
                .collect(Collectors.toSet());
        final Set<String> partitionColumns = tableDef.getPartitioningColumns().stream()
                .map(ColumnDefinition::getName)
                .collect(Collectors.toSet());

        // The working partitioning column set must be a super-set of the user-supplied set.
        if (!partitionColumns.containsAll(userPartitionColumns)) {
            final Set<String> invalidColumns = new HashSet<>(userPartitionColumns);
            invalidColumns.removeAll(partitionColumns);

            throw new TableDataException("The following columns are not partitioned in the Iceberg table: " +
                    invalidColumns);
        }
        return tableDef;
    }

    /**
     * Create a new {@link IcebergTableWriter} for this Iceberg table using the provided {@link TableWriterOptions}.
     * <p>
     * This method will perform schema validation to ensure that the provided
     * {@link TableWriterOptions#tableDefinition()} is compatible with the Iceberg table schema. All further writes
     * performed by the returned writer will not be validated against the table's schema, and thus will be faster.
     *
     * @param tableWriterOptions The options to configure the table writer.
     * @return A new instance of {@link IcebergTableWriter} configured with the provided options.
     */
    public IcebergTableWriter tableWriter(final TableWriterOptions tableWriterOptions) {
        return new IcebergTableWriter(tableWriterOptions, this, dataInstructionsProviderLoader);
    }

    /**
     * Get the location URI of the Iceberg table.
     */
    public URI locationUri() {
        return locationUri;
    }
}
