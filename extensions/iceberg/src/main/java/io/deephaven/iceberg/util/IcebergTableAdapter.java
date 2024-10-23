//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.api.util.NameValidator;
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
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.iceberg.layout.*;
import io.deephaven.iceberg.location.IcebergTableLocationFactory;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.iceberg.base.IcebergUtils.allDataFiles;
import static io.deephaven.iceberg.base.IcebergUtils.convertToDHType;
import static io.deephaven.iceberg.base.IcebergUtils.createSpecAndSchema;
import static io.deephaven.iceberg.base.IcebergUtils.partitionDataFromPaths;
import static io.deephaven.iceberg.base.IcebergUtils.verifyAppendCompatibility;
import static io.deephaven.iceberg.base.IcebergUtils.verifyOverwriteCompatibility;

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

    private final org.apache.iceberg.Table table;
    private final TableIdentifier tableIdentifier;
    private final DataInstructionsProviderLoader dataInstructionsProviderLoader;

    public IcebergTableAdapter(
            final TableIdentifier tableIdentifier,
            final org.apache.iceberg.Table table,
            final DataInstructionsProviderLoader dataInstructionsProviderLoader) {
        this.table = table;
        this.tableIdentifier = tableIdentifier;
        this.dataInstructionsProviderLoader = dataInstructionsProviderLoader;
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
     * @return An Optional<Snapshot> containing the requested snapshot if it exists.
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
     * Return {@link TableDefinition table definition}.
     *
     * @return The table definition
     */
    public TableDefinition definition() {
        // Load the table from the catalog.
        return definition(null);
    }

    /**
     * Return {@link TableDefinition table definition} with optional instructions for customizations while reading.
     *
     * @param instructions The instructions for customizations while reading (or null for default instructions)
     * @return The table definition
     */
    public TableDefinition definition(@Nullable final IcebergReadInstructions instructions) {
        // Load the table from the catalog.
        return definition(null, instructions);
    }

    /**
     * Return {@link TableDefinition table definition} for the Iceberg table and snapshot id, with optional instructions
     * for customizations while reading.
     *
     * @param snapshotId The identifier of the snapshot to load
     * @param instructions The instructions for customizations while reading (or null for default instructions)
     * @return The table definition
     */
    public TableDefinition definition(
            final long snapshotId,
            @Nullable final IcebergReadInstructions instructions) {
        // Find the snapshot with the given snapshot id
        final Snapshot tableSnapshot =
                snapshot(snapshotId).orElseThrow(() -> new IllegalArgumentException(
                        "Snapshot with id " + snapshotId + " not found for table " + tableIdentifier));

        // Load the table from the catalog.
        return definition(tableSnapshot, instructions);
    }

    /**
     * Return {@link TableDefinition table definition} for the Iceberg table and snapshot, with optional instructions
     * for customizations while reading.
     *
     * @param tableSnapshot The snapshot to load
     * @param instructions The instructions for customizations while reading (or null for default instructions)
     * @return The table definition
     */
    public TableDefinition definition(
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergReadInstructions instructions) {

        final Schema schema;
        final org.apache.iceberg.PartitionSpec partitionSpec;

        if (tableSnapshot == null) {
            synchronized (this) {
                // Refresh only once and record the current schema and partition spec.
                refresh();
                schema = table.schema();
                partitionSpec = table.spec();
            }
        } else {
            // Use the schema from the snapshot
            schema = schema(tableSnapshot.schemaId()).get();
            partitionSpec = table.spec();
        }

        final IcebergReadInstructions userInstructions =
                instructions == null ? IcebergReadInstructions.DEFAULT : instructions;

        return fromSchema(schema,
                partitionSpec,
                userInstructions.tableDefinition().orElse(null),
                getRenameColumnMap(table, schema, userInstructions));
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of the Iceberg table.
     *
     * @return The table definition as a Deephaven table
     */
    public Table definitionTable() {
        return TableTools.metaTable(definition());
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of the Iceberg table, with optional
     * instructions for customizations while reading.
     *
     * @param instructions The instructions for customizations while reading
     * @return The table definition as a Deephaven table
     */
    public Table definitionTable(@Nullable final IcebergReadInstructions instructions) {
        return TableTools.metaTable(definition(null, instructions));
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of a given Iceberg table and
     * snapshot id, with optional instructions for customizations while reading.
     *
     * @param snapshotId The identifier of the snapshot to load
     * @param instructions The instructions for customizations while reading (or null for default instructions)
     * @return The table definition as a Deephaven table
     */
    public Table definitionTable(
            final long snapshotId,
            @Nullable final IcebergReadInstructions instructions) {
        return TableTools.metaTable(definition(snapshotId, instructions));
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of a given Iceberg table and
     * snapshot id, with optional instructions for customizations while reading.
     *
     * @param tableSnapshot The snapshot to load
     * @param instructions The instructions for customizations while reading (or null for default instructions)
     * @return The table definition as a Deephaven table
     */
    public Table definitionTable(
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergReadInstructions instructions) {
        return TableTools.metaTable(definition(tableSnapshot, instructions));
    }

    /**
     * Read the latest snapshot of an Iceberg table from the Iceberg catalog as a Deephaven {@link Table table}.
     *
     * @return The loaded table
     */
    public IcebergTable table() {
        return table(null);
    }

    /**
     * Read the latest snapshot of an Iceberg table from the Iceberg catalog as a Deephaven {@link Table table}.
     *
     * @param instructions The instructions for customizations while reading (or null for default instructions)
     * @return The loaded table
     */
    public IcebergTable table(@Nullable final IcebergReadInstructions instructions) {
        return table(null, instructions);
    }

    /**
     * Read a snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableSnapshotId The snapshot id to load
     * @return The loaded table
     */
    public IcebergTable table(final long tableSnapshotId) {
        return table(tableSnapshotId, null);
    }

    /**
     * Read a snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableSnapshotId The snapshot id to load
     * @param instructions The instructions for customizations while reading (or null for default instructions)
     * @return The loaded table
     */
    public IcebergTable table(final long tableSnapshotId, @Nullable final IcebergReadInstructions instructions) {
        // Find the snapshot with the given snapshot id
        final Snapshot tableSnapshot =
                snapshot(tableSnapshotId).orElseThrow(() -> new IllegalArgumentException(
                        "Snapshot with id " + tableSnapshotId + " not found for table " + tableIdentifier));

        return table(tableSnapshot, instructions);
    }

    /**
     * Read a snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableSnapshot The snapshot to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    public IcebergTable table(
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergReadInstructions instructions) {

        final Snapshot snapshot;
        final Schema schema;
        final org.apache.iceberg.PartitionSpec partitionSpec;

        if (tableSnapshot == null) {
            synchronized (this) {
                // Refresh only once and record the current snapshot, schema (which may be newer than the
                // snapshot schema), and partition spec.
                refresh();
                snapshot = table.currentSnapshot();
                schema = table.schema();
                partitionSpec = table.spec();
            }
        } else {
            snapshot = tableSnapshot;
            // Use the schema from the snapshot
            schema = schema(tableSnapshot.schemaId()).get();
            partitionSpec = table.spec();
        }

        // Get default instructions if none are provided
        final IcebergReadInstructions userInstructions =
                instructions == null ? IcebergReadInstructions.DEFAULT : instructions;

        // Get the user supplied table definition.
        final TableDefinition userTableDef = userInstructions.tableDefinition().orElse(null);

        // Map all the column names in the schema to their legalized names.
        final Map<String, String> legalizedColumnRenames = getRenameColumnMap(table, schema, userInstructions);

        // Get the table definition from the schema (potentially limited by the user supplied table definition and
        // applying column renames).
        final TableDefinition tableDef = fromSchema(schema, partitionSpec, userTableDef, legalizedColumnRenames);

        // Create the final instructions with the legalized column renames.
        final IcebergReadInstructions finalInstructions = userInstructions.withColumnRenames(legalizedColumnRenames);

        final IcebergBaseLayout keyFinder;
        if (partitionSpec.isUnpartitioned()) {
            // Create the flat layout location key finder
            keyFinder = new IcebergFlatLayout(this, snapshot, finalInstructions,
                    dataInstructionsProviderLoader);
        } else {
            // Create the partitioning column location key finder
            keyFinder = new IcebergKeyValuePartitionedLayout(this, snapshot, partitionSpec,
                    finalInstructions, dataInstructionsProviderLoader);
        }

        if (finalInstructions.updateMode().updateType() == IcebergUpdateMode.IcebergUpdateType.STATIC) {
            final IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> locationProvider =
                    new IcebergStaticTableLocationProvider<>(
                            StandaloneTableKey.getInstance(),
                            keyFinder,
                            new IcebergTableLocationFactory(),
                            tableIdentifier);

            return new IcebergTableImpl(
                    tableDef,
                    tableIdentifier.toString(),
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    locationProvider,
                    null);
        }

        final UpdateSourceRegistrar updateSourceRegistrar = ExecutionContext.getContext().getUpdateGraph();
        final IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> locationProvider;

        if (finalInstructions.updateMode().updateType() == IcebergUpdateMode.IcebergUpdateType.MANUAL_REFRESHING) {
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
                    finalInstructions.updateMode().autoRefreshMs(),
                    this,
                    tableIdentifier);
        }

        return new IcebergTableImpl(
                tableDef,
                tableIdentifier.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                locationProvider,
                updateSourceRegistrar);
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
     * Get a legalized column rename map from a table schema and user instructions.
     */
    private Map<String, String> getRenameColumnMap(
            @NotNull final org.apache.iceberg.Table table,
            @NotNull final Schema schema,
            @NotNull final IcebergReadInstructions instructions) {

        final Set<String> takenNames = new HashSet<>();

        // Map all the column names in the schema to their legalized names.
        final Map<String, String> legalizedColumnRenames = new HashMap<>();

        // Validate user-supplied names meet legalization instructions
        for (final Map.Entry<String, String> entry : instructions.columnRenames().entrySet()) {
            final String destinationName = entry.getValue();
            if (!NameValidator.isValidColumnName(destinationName)) {
                throw new TableDataException(
                        String.format("%s - invalid column name provided (%s)", table, destinationName));
            }
            // Add these renames to the legalized list.
            legalizedColumnRenames.put(entry.getKey(), destinationName);
            takenNames.add(destinationName);
        }

        for (final Types.NestedField field : schema.columns()) {
            final String name = field.name();
            // Do we already have a valid rename for this column from the user or a partitioned column?
            if (!legalizedColumnRenames.containsKey(name)) {
                final String legalizedName =
                        NameValidator.legalizeColumnName(name, s -> s.replace(" ", "_"), takenNames);
                if (!legalizedName.equals(name)) {
                    legalizedColumnRenames.put(name, legalizedName);
                    takenNames.add(legalizedName);
                }
            }
        }

        return legalizedColumnRenames;
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
     * Append the provided Deephaven table as a new partition to the existing Iceberg table in a single snapshot. This
     * will not change the schema of the existing table.
     *
     * @param icebergAppend The {@link IcebergAppend} object containing the data/instructions for writing
     */
    public void append(@NotNull final IcebergAppend icebergAppend) {
        writeImpl(icebergAppend.dhTables(),
                icebergAppend.partitionPaths(),
                icebergAppend.instructions(),
                false,
                true);
    }

    /**
     * Overwrite the existing Iceberg table with the provided Deephaven tables in a single snapshot. This will overwrite
     * the schema of the existing table to match the provided Deephaven table if they do not match.
     * <p>
     * Overwriting a table while racing with other writers can lead to failure/undefined results.
     *
     * @param icebergOverwrite The {@link IcebergOverwrite} object containing the data/instructions for writing
     */
    public void overwrite(@NotNull final IcebergOverwrite icebergOverwrite) {
        writeImpl(icebergOverwrite.dhTables(),
                icebergOverwrite.partitionPaths(),
                icebergOverwrite.instructions(),
                true,
                true);
    }

    /**
     * Writes data from Deephaven tables to an Iceberg table without creating a new snapshot. This method returns a list
     * of data files that were written. Users can use this list to create a transaction/snapshot if needed.
     *
     * @param icebergWriteDataFiles The {@link IcebergWriteDataFiles} object containing the data/instructions for
     *        writing
     */
    public List<DataFile> writeDataFiles(@NotNull final IcebergWriteDataFiles icebergWriteDataFiles) {
        return writeImpl(icebergWriteDataFiles.dhTables(),
                icebergWriteDataFiles.partitionPaths(),
                icebergWriteDataFiles.instructions(),
                false,
                false);
    }

    /**
     * Appends or overwrites data in an Iceberg table with the provided Deephaven tables.
     *
     * @param dhTables The Deephaven tables to write
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     * @param overwrite If true, the existing data in the Iceberg table will be overwritten; if false, the data will be
     *        appended
     * @param addSnapshot If true, a new snapshot will be created in the Iceberg table with the written data
     *
     * @return A list of DataFile objects representing the written data files.
     */
    private List<DataFile> writeImpl(
            @NotNull List<Table> dhTables,
            @NotNull final List<String> partitionPaths,
            @NotNull final IcebergWriteInstructions instructions,
            final boolean overwrite,
            final boolean addSnapshot) {
        if (overwrite && !addSnapshot) {
            throw new IllegalArgumentException("Cannot overwrite an Iceberg table without adding a snapshot");
        }
        if (dhTables.isEmpty()) {
            if (!overwrite) {
                // Nothing to append
                return Collections.emptyList();
            }
            // Overwrite with an empty table
            dhTables = List.of(TableTools.emptyTable(0));
        }

        final IcebergParquetWriteInstructions writeInstructions = verifyAndFillDefinition(instructions, dhTables);
        final TableDefinition useDefinition = writeInstructions.tableDefinition().get();

        // Don't verify schema by default if overwriting
        final boolean verifySchema = writeInstructions.verifySchema().orElse(!overwrite);

        final IcebergUtils.SpecAndSchema newSpecAndSchema;
        final List<PartitionData> partitionData;
        if (overwrite) {
            verifyPartitionPathsIfOverwritingOrCreating(partitionPaths, writeInstructions);
            newSpecAndSchema = createSpecAndSchema(useDefinition, writeInstructions);
        } else {
            verifyPartitionPathsIfAppending(table, partitionPaths);
            // Same spec and schema as the existing table
            newSpecAndSchema = new IcebergUtils.SpecAndSchema(table.spec(), table.schema());
        }
        partitionData = partitionDataFromPaths(newSpecAndSchema.partitionSpec(), partitionPaths);
        if (verifySchema) {
            try {
                if (overwrite) {
                    verifyOverwriteCompatibility(table.schema(), newSpecAndSchema.schema());
                    verifyOverwriteCompatibility(table.spec(), newSpecAndSchema.partitionSpec());
                } else {
                    verifyAppendCompatibility(table.schema(), useDefinition, writeInstructions);
                    verifyAppendCompatibility(table.spec(), useDefinition, writeInstructions);
                }
            } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("Schema verification failed. Please provide a compatible " +
                        "schema or disable verification in the Iceberg instructions. See the linked exception " +
                        "for more details.", e);
            }
        }

        final PartitionSpec newPartitionSpec = newSpecAndSchema.partitionSpec();
        final List<CompletedParquetWrite> parquetFileInfo =
                writeParquet(newPartitionSpec, dhTables, partitionPaths, writeInstructions);
        final List<DataFile> appendFiles = dataFilesFromParquet(parquetFileInfo, partitionData, newPartitionSpec);
        if (addSnapshot) {
            commit(newSpecAndSchema, appendFiles, overwrite, verifySchema);
        }
        return appendFiles;
    }

    static IcebergParquetWriteInstructions verifyAndFillDefinition(
            final IcebergWriteInstructions instructions,
            final List<Table> dhTables) {
        // We ony support writing to Parquet files
        if (!(instructions instanceof IcebergParquetWriteInstructions)) {
            throw new IllegalArgumentException("Unsupported instructions of class " + instructions.getClass() + " for" +
                    " writing Iceberg table, expected: " + IcebergParquetWriteInstructions.class);
        }
        final IcebergParquetWriteInstructions writeInstructions = (IcebergParquetWriteInstructions) instructions;
        if (writeInstructions.tableDefinition().isPresent()) {
            return writeInstructions;
        } else {
            // Verify that all tables have the same definition
            final int numTables = dhTables.size();
            if (numTables == 0) {
                throw new IllegalArgumentException("No Deephaven tables provided for writing");
            }
            final TableDefinition firstDefinition = dhTables.get(0).getDefinition();
            for (int idx = 1; idx < numTables; idx++) {
                if (!firstDefinition.equals(dhTables.get(idx).getDefinition())) {
                    throw new IllegalArgumentException(
                            "All Deephaven tables must have the same definition, else table definition should be " +
                                    "provided when writing multiple tables with different definitions");
                }
            }
            return writeInstructions.withTableDefinition(firstDefinition);
        }
    }

    private static void verifyPartitionPathsIfOverwritingOrCreating(
            @NotNull final Collection<String> partitionPaths,
            @NotNull final IcebergBaseInstructions instructions) {
        if (!partitionPaths.isEmpty()) {
            final TableDefinition tableDefinition = instructions.tableDefinition()
                    .orElseThrow(() -> new IllegalArgumentException("Table definition expected"));
            if (tableDefinition.getColumnStream().noneMatch(ColumnDefinition::isPartitioning)) {
                throw new IllegalArgumentException("Cannot write un-partitioned table to partition paths. Please " +
                        "remove partition paths from the Iceberg instructions or provide a table definition with " +
                        "partitioning columns.");
            }
        }
    }

    private static void verifyPartitionPathsIfAppending(
            final org.apache.iceberg.Table icebergTable,
            final Collection<String> partitionPaths) {
        if (icebergTable.spec().isPartitioned() && partitionPaths.isEmpty()) {
            throw new IllegalArgumentException("Cannot write data to a partitioned table without partition paths.");
        } else if (!icebergTable.spec().isPartitioned() && !partitionPaths.isEmpty()) {
            throw new IllegalArgumentException("Cannot write data to an un-partitioned table with partition paths.");
        }
    }

    private static class CompletedParquetWrite {
        private final URI destination;
        private final long numRows;
        private final long numBytes;

        private CompletedParquetWrite(final URI destination, final long numRows, final long numBytes) {
            this.destination = destination;
            this.numRows = numRows;
            this.numBytes = numBytes;
        }
    }

    @NotNull
    private List<CompletedParquetWrite> writeParquet(
            @NotNull final PartitionSpec partitionSpec,
            @NotNull final List<Table> dhTables,
            @NotNull final List<String> partitionPaths,
            @NotNull final IcebergParquetWriteInstructions writeInstructions) {
        // Build the parquet instructions
        final List<CompletedParquetWrite> parquetFilesWritten = new ArrayList<>(dhTables.size());
        final ParquetInstructions.OnWriteCompleted onWriteCompleted =
                (destination, numRows, numBytes) -> parquetFilesWritten
                        .add(new CompletedParquetWrite(destination, numRows, numBytes));

        // Remove all partitioning columns from the table definition before writing parquet files
        TableDefinition parquetTableDefinition = writeInstructions.tableDefinition()
                .orElseThrow(() -> new IllegalArgumentException("Table definition expected"));
        if (partitionSpec.isPartitioned()) {
            parquetTableDefinition = TableDefinition.of(
                    parquetTableDefinition.getColumnStream()
                            .filter(columnDefinition -> !columnDefinition.isPartitioning())
                            .collect(Collectors.toList()));
        }
        final ParquetInstructions parquetInstructions = writeInstructions.toParquetInstructions(
                onWriteCompleted, parquetTableDefinition, table.schema().idToName());

        // Write the data to parquet files
        int count = 0;
        for (int idx = 0; idx < dhTables.size(); idx++) {
            final Table dhTable = dhTables.get(idx);
            if (dhTable.numColumns() == 0) {
                // Skip writing empty tables with no columns
                continue;
            }
            final String fileName = String.format(
                    "00000-%d-%s.parquet",
                    count++,
                    UUID.randomUUID());
            final String relativePath;
            if (partitionSpec.isPartitioned()) {
                relativePath = String.format("%s/%s", partitionPaths.get(idx), fileName);
            } else {
                relativePath = fileName;
            }
            final String newDataLocation = table.locationProvider().newDataLocation(relativePath);
            ParquetTools.writeTable(dhTable, newDataLocation, parquetInstructions);
        }
        return parquetFilesWritten;
    }

    /**
     * Commit the changes to the Iceberg table by creating a snapshot.
     */
    private void commit(
            @NotNull final IcebergUtils.SpecAndSchema newSpecAndSchema,
            @NotNull final Iterable<DataFile> appendFiles,
            final boolean overwrite,
            final boolean schemaVerified) {
        final Transaction icebergTransaction = table.newTransaction();
        final Snapshot currentSnapshot = table.currentSnapshot();
        // For a null current snapshot, we are creating a new table. So we can just append instead of overwriting.
        if (overwrite && currentSnapshot != null) {
            // Fail if the table gets changed concurrently
            final OverwriteFiles overwriteFiles = icebergTransaction.newOverwrite()
                    .validateFromSnapshot(currentSnapshot.snapshotId())
                    .validateNoConflictingDeletes()
                    .validateNoConflictingData();

            // Delete all the existing data files in the table
            try (final Stream<DataFile> dataFiles = allDataFiles(table, currentSnapshot)) {
                dataFiles.forEach(overwriteFiles::deleteFile);
            }
            appendFiles.forEach(overwriteFiles::addFile);
            overwriteFiles.commit();

            // Update the spec and schema of the existing table.
            // If we have already verified the schema, we don't need to update it.
            if (!schemaVerified) {
                if (!table.schema().sameSchema(newSpecAndSchema.schema())) {
                    final UpdateSchema updateSchema = icebergTransaction.updateSchema().allowIncompatibleChanges();
                    table.schema().columns().stream()
                            .map(Types.NestedField::name)
                            .forEach(updateSchema::deleteColumn);
                    newSpecAndSchema.schema().columns()
                            .forEach(column -> updateSchema.addColumn(column.name(), column.type()));
                    updateSchema.commit();
                }
                if (!table.spec().compatibleWith(newSpecAndSchema.partitionSpec())) {
                    final UpdatePartitionSpec updateSpec = icebergTransaction.updateSpec();
                    table.spec().fields().forEach(field -> updateSpec.removeField(field.name()));
                    newSpecAndSchema.partitionSpec().fields().forEach(field -> updateSpec.addField(field.name()));
                    updateSpec.commit();
                }
            }
        } else {
            // Append the new data files to the table
            final AppendFiles append = icebergTransaction.newAppend();
            appendFiles.forEach(append::appendFile);
            append.commit();
        }

        // Commit the transaction, creating new snapshot for append/overwrite.
        // Note that no new snapshot will be created for the schema change.
        icebergTransaction.commitTransaction();
    }

    /**
     * Generate a list of {@link DataFile} objects from a list of parquet files written.
     */
    private static List<DataFile> dataFilesFromParquet(
            @NotNull final List<CompletedParquetWrite> parquetFilesWritten,
            @NotNull final List<PartitionData> partitionDataList,
            @NotNull final PartitionSpec partitionSpec) {
        final int numFiles = parquetFilesWritten.size();
        final List<DataFile> dataFiles = new ArrayList<>(numFiles);
        for (int idx = 0; idx < numFiles; idx++) {
            final CompletedParquetWrite completedWrite = parquetFilesWritten.get(idx);
            final DataFiles.Builder dataFileBuilder = DataFiles.builder(partitionSpec)
                    .withPath(completedWrite.destination.toString())
                    .withFormat(FileFormat.PARQUET)
                    .withRecordCount(completedWrite.numRows)
                    .withFileSizeInBytes(completedWrite.numBytes);
            if (partitionSpec.isPartitioned()) {
                dataFileBuilder.withPartition(partitionDataList.get(idx));
            }
            dataFiles.add(dataFileBuilder.build());
        }
        return dataFiles;
    }
}
