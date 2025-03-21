//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

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
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.internal.Inference;
import io.deephaven.iceberg.internal.SpecAndSchema2;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.iceberg.layout.*;
import io.deephaven.iceberg.location.IcebergTableLocationFactory;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.iceberg.base.IcebergUtils.convertToDHType;
import static io.deephaven.iceberg.layout.IcebergBaseLayout.locationUri;

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

    private final Catalog catalog;
    private final org.apache.iceberg.Table table;
    private final TableIdentifier tableIdentifier;
    private final DataInstructionsProviderLoader dataInstructionsProviderLoader;

    public IcebergTableAdapter(
            final Catalog catalog,
            final TableIdentifier tableIdentifier,
            final org.apache.iceberg.Table table,
            final DataInstructionsProviderLoader dataInstructionsProviderLoader) {
        this.catalog = catalog;
        this.table = table;
        this.tableIdentifier = tableIdentifier;
        this.dataInstructionsProviderLoader = dataInstructionsProviderLoader;
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

    /**
     * Retrieve the schema and partition spec for the table based on the provided read instructions. Also, populate the
     * read instructions with the requested snapshot, or the latest snapshot if none is requested.
     */
    private SpecAndSchema2 getSpecAndSchema(@NotNull final IcebergReadInstructions readInstructions) {
        // TODO: this is a change in behavior, unless w/ definitionInstructions, we will infer based on the latest
        // schema
        final Snapshot snapshot;
        {
            final Snapshot snapshotFromInstructions = getSnapshot(readInstructions);
            if (snapshotFromInstructions == null) {
                // todo: not sure why we were sync and refreshing before?
                snapshot = table.currentSnapshot();
            } else {
                // Use the schema from the snapshot
                snapshot = snapshotFromInstructions;
            }
        }
        final Resolver di;
        if (readInstructions.resolver().isPresent()) {
            di = readInstructions.resolver().orElseThrow();
        } else {
            try {
                // note: using the latest schema, even if specific snapshot is set
                di = Resolver.infer(InferenceInstructions.of(table.schema(), table.spec()));
            } catch (Inference.Exception e) {
                throw new RuntimeException(e);
            }
        }
        return new SpecAndSchema2(di, snapshot);
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
        return getSpecAndSchema(readInstructions).di.definition();
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
        final SpecAndSchema2 ss = getSpecAndSchema(readInstructions);
        final IcebergBaseLayout keyFinder = keyFinder(ss, readInstructions);
        if (readInstructions.updateMode().updateType() == IcebergUpdateMode.IcebergUpdateType.STATIC) {
            final IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> locationProvider =
                    new IcebergStaticTableLocationProvider<>(
                            StandaloneTableKey.getInstance(),
                            keyFinder,
                            new IcebergTableLocationFactory(),
                            tableIdentifier);
            return new IcebergTableImpl(
                    ss.di.definition(),
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
                ss.di.definition(),
                tableIdentifier.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                locationProvider,
                updateSourceRegistrar);
    }

    private @NotNull IcebergBaseLayout keyFinder(SpecAndSchema2 ss, IcebergReadInstructions ri) {
        final String uriScheme = locationUri(table).getScheme();
        final Object specialInstructions = ri.dataInstructions()
                .orElseGet(() -> dataInstructionsProviderLoader.load(uriScheme));
        final SeekableChannelsProvider channelsProvider =
                SeekableChannelsProviderLoader.getInstance().load(uriScheme, specialInstructions);
        final ParquetInstructions parquetInstructions = ParquetInstructions.builder()
                .setTableDefinition(ss.di.definition())
                .setColumnResolverFactory(ss.di.factory())
                .setSpecialInstructions(specialInstructions)
                .build();
        final PartitionSpec spec = ss.di.spec();
        if (spec.isUnpartitioned()) {
            // Create the flat layout location key finder
            return new IcebergFlatLayout(this, ss.snapshot, parquetInstructions, channelsProvider);
        } else {
            // Create the partitioning column location key finder
            return new IcebergKeyValuePartitionedLayout(this, ss.snapshot, parquetInstructions, channelsProvider, spec);
        }
    }

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
        return new IcebergTableWriter(tableWriterOptions, this);
    }
}
