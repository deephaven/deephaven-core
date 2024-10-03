//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.PollingTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.iceberg.layout.IcebergFlatLayout;
import io.deephaven.iceberg.layout.IcebergKeyValuePartitionedLayout;
import io.deephaven.iceberg.location.IcebergTableLocationFactory;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.hadoop.util.StringUtils;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
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

import static io.deephaven.iceberg.base.IcebergUtils.convertToDHType;
import static io.deephaven.iceberg.base.IcebergUtils.convertToIcebergType;
import static io.deephaven.iceberg.base.IcebergUtils.getAllDataFiles;
import static io.deephaven.iceberg.base.IcebergUtils.verifyAppendCompatibility;
import static io.deephaven.iceberg.base.IcebergUtils.verifyOverwriteCompatibility;

public class IcebergCatalogAdapter {

    @VisibleForTesting
    static final TableDefinition NAMESPACE_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("Namespace"),
            ColumnDefinition.fromGenericType("NamespaceObject", Namespace.class));

    @VisibleForTesting
    static final TableDefinition TABLES_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("Namespace"),
            ColumnDefinition.ofString("TableName"),
            ColumnDefinition.fromGenericType("TableIdentifierObject", TableIdentifier.class));

    @VisibleForTesting
    static final TableDefinition SNAPSHOT_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("Id"),
            ColumnDefinition.ofTime("Timestamp"),
            ColumnDefinition.ofString("Operation"),
            ColumnDefinition.fromGenericType("Summary", Map.class),
            ColumnDefinition.fromGenericType("SnapshotObject", Snapshot.class));

    private static final String DEFAULT_GENERATED_FILE_FORMAT = "parquet";

    private final Catalog catalog;

    private final DataInstructionsProviderLoader dataInstructionsProvider;

    /**
     * Construct an IcebergCatalogAdapter from a catalog.
     */
    IcebergCatalogAdapter(@NotNull final Catalog catalog) {
        this(catalog, Map.of());
    }

    /**
     * Construct an IcebergCatalogAdapter from a catalog and property collection.
     */
    IcebergCatalogAdapter(
            @NotNull final Catalog catalog,
            @NotNull final Map<String, String> properties) {
        this.catalog = catalog;

        dataInstructionsProvider = DataInstructionsProviderLoader.create(Map.copyOf(properties));
    }

    /**
     * Create a single {@link TableDefinition} from a given Schema, PartitionSpec, and TableDefinition. Takes into
     * account {@link Map<> column rename instructions}
     *
     * @param schema The schema of the table.
     * @param partitionSpec The partition specification of the table.
     * @param userTableDef The table definition.
     * @param columnRename The map for renaming columns.
     * @return The generated TableDefinition.
     */
    private static TableDefinition fromSchema(
            @NotNull final Schema schema,
            @NotNull final PartitionSpec partitionSpec,
            @Nullable final TableDefinition userTableDef,
            @NotNull final Map<String, String> columnRename) {

        final Set<String> columnNames = userTableDef != null
                ? userTableDef.getColumnNameSet()
                : null;

        final Set<String> partitionNames =
                partitionSpec.fields().stream()
                        .map(PartitionField::name)
                        .map(colName -> columnRename.getOrDefault(colName, colName))
                        .collect(Collectors.toSet());

        final List<ColumnDefinition<?>> columns = new ArrayList<>();

        for (final Types.NestedField field : schema.columns()) {
            final String name = columnRename.getOrDefault(field.name(), field.name());
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
     * List all {@link Namespace namespaces} in the catalog. This method is only supported if the catalog implements
     * {@link SupportsNamespaces} for namespace discovery. See {@link SupportsNamespaces#listNamespaces(Namespace)}.
     *
     * @return A list of all namespaces.
     */
    public List<Namespace> listNamespaces() {
        return listNamespaces(Namespace.empty());
    }

    /**
     * List all {@link Namespace namespaces} in a given namespace. This method is only supported if the catalog
     * implements {@link SupportsNamespaces} for namespace discovery. See
     * {@link SupportsNamespaces#listNamespaces(Namespace)}.
     *
     * @param namespace The namespace to list namespaces in.
     * @return A list of all namespaces in the given namespace.
     */
    public List<Namespace> listNamespaces(@NotNull final Namespace namespace) {
        if (catalog instanceof SupportsNamespaces) {
            final SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            return nsCatalog.listNamespaces(namespace);
        }
        throw new UnsupportedOperationException(String.format(
                "%s does not implement org.apache.iceberg.catalog.SupportsNamespaces", catalog.getClass().getName()));
    }

    /**
     * List all {@link Namespace namespaces} in the catalog as a Deephaven {@link Table table}. The resulting table will
     * be static and contain the same information as {@link #listNamespaces()}.
     *
     * @return A {@link Table table} of all namespaces.
     */
    public Table listNamespacesAsTable() {
        return listNamespacesAsTable(Namespace.empty());
    }

    /**
     * List all {@link Namespace namespaces} in a given namespace as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listNamespaces(Namespace)}.
     *
     * @return A {@link Table table} of all namespaces.
     */
    public Table listNamespacesAsTable(@NotNull final Namespace namespace) {
        final List<Namespace> namespaces = listNamespaces(namespace);
        final long size = namespaces.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final String[] namespaceArr = new String[(int) size];
        columnSourceMap.put("Namespace",
                InMemoryColumnSource.getImmutableMemoryColumnSource(namespaceArr, String.class, null));

        final Namespace[] namespaceObjectArr = new Namespace[(int) size];
        columnSourceMap.put("NamespaceObject",
                InMemoryColumnSource.getImmutableMemoryColumnSource(namespaceObjectArr, Namespace.class, null));

        // Populate the column source arrays
        for (int i = 0; i < size; i++) {
            final Namespace ns = namespaces.get(i);
            namespaceArr[i] = ns.toString();
            namespaceObjectArr[i] = ns;
        }

        // Create and return the table
        return new QueryTable(NAMESPACE_DEFINITION, RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    /**
     * List all {@link Namespace namespaces} in a given namespace as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listNamespaces(Namespace)}.
     *
     * @return A {@link Table table} of all namespaces.
     */
    public Table listNamespacesAsTable(@NotNull final String... namespace) {
        return listNamespacesAsTable(Namespace.of(namespace));
    }

    /**
     * List all Iceberg {@link TableIdentifier tables} in a given namespace.
     *
     * @param namespace The namespace to list tables in.
     * @return A list of all tables in the given namespace.
     */
    public List<TableIdentifier> listTables(@NotNull final Namespace namespace) {
        return catalog.listTables(namespace);
    }

    /**
     * List all Iceberg {@link TableIdentifier tables} in a given namespace as a Deephaven {@link Table table}. The
     * resulting table will be static and contain the same information as {@link #listTables(Namespace)}.
     *
     * @param namespace The namespace from which to gather the tables
     * @return A list of all tables in the given namespace.
     */
    public Table listTablesAsTable(@NotNull final Namespace namespace) {
        final List<TableIdentifier> tableIdentifiers = listTables(namespace);
        final long size = tableIdentifiers.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final String[] namespaceArr = new String[(int) size];
        columnSourceMap.put("Namespace",
                InMemoryColumnSource.getImmutableMemoryColumnSource(namespaceArr, String.class, null));

        final String[] tableNameArr = new String[(int) size];
        columnSourceMap.put("TableName",
                InMemoryColumnSource.getImmutableMemoryColumnSource(tableNameArr, String.class, null));

        final TableIdentifier[] tableIdentifierArr = new TableIdentifier[(int) size];
        columnSourceMap.put("TableIdentifierObject",
                InMemoryColumnSource.getImmutableMemoryColumnSource(tableIdentifierArr, TableIdentifier.class, null));

        // Populate the column source arrays
        for (int i = 0; i < size; i++) {
            final TableIdentifier tableIdentifier = tableIdentifiers.get(i);
            namespaceArr[i] = tableIdentifier.namespace().toString();
            tableNameArr[i] = tableIdentifier.name();
            tableIdentifierArr[i] = tableIdentifier;
        }

        // Create and return the table
        return new QueryTable(TABLES_DEFINITION, RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    public Table listTablesAsTable(@NotNull final String... namespace) {
        return listTablesAsTable(Namespace.of(namespace));
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table.
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all snapshots of the given table.
     */
    public List<Snapshot> listSnapshots(@NotNull final TableIdentifier tableIdentifier) {
        final List<Snapshot> snapshots = new ArrayList<>();
        catalog.loadTable(tableIdentifier).snapshots().forEach(snapshots::add);
        return snapshots;
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listSnapshots(TableIdentifier)}.
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all tables in the given namespace.
     */
    public Table listSnapshotsAsTable(@NotNull final TableIdentifier tableIdentifier) {
        final List<Snapshot> snapshots = listSnapshots(tableIdentifier);
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
     * List all {@link Snapshot snapshots} of a given Iceberg table as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listSnapshots(TableIdentifier)}.
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all tables in the given namespace.
     */
    public Table listSnapshotsAsTable(@NotNull final String tableIdentifier) {
        return listSnapshotsAsTable(TableIdentifier.parse(tableIdentifier));
    }

    /**
     * Get a specific {@link Snapshot snapshot} of a given Iceberg table (or null if it does not exist).
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots
     * @param snapshotId The id of the snapshot to retrieve
     * @return The snapshot with the given id, or null if it does not exist
     */
    private Snapshot getSnapshot(@NotNull final TableIdentifier tableIdentifier, final long snapshotId) {
        return listSnapshots(tableIdentifier).stream()
                .filter(snapshot -> snapshot.snapshotId() == snapshotId)
                .findFirst()
                .orElse(null);
    }

    /**
     * Get a legalized column rename map from a table schema and user instructions.
     */
    private Map<String, String> getRenameColumnMap(
            @NotNull final org.apache.iceberg.Table table,
            @NotNull final Schema schema,
            @NotNull final IcebergInstructions instructions) {

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
     * Return {@link TableDefinition table definition} for a given Iceberg table, with optional instructions for
     * customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition
     */
    public TableDefinition getTableDefinition(
            @NotNull final String tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        final TableIdentifier tableId = TableIdentifier.parse(tableIdentifier);
        // Load the table from the catalog.
        return getTableDefinition(tableId, instructions);
    }

    /**
     * Return {@link TableDefinition table definition} for a given Iceberg table, with optional instructions for
     * customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition
     */
    public TableDefinition getTableDefinition(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        // Load the table from the catalog.
        return getTableDefinitionInternal(tableIdentifier, null, instructions);
    }

    /**
     * Return {@link TableDefinition table definition} for a given Iceberg table and snapshot id, with optional
     * instructions for customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param snapshotId The identifier of the snapshot to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition
     */
    public TableDefinition getTableDefinition(
            @NotNull final String tableIdentifier,
            final long snapshotId,
            @Nullable final IcebergInstructions instructions) {
        final TableIdentifier tableId = TableIdentifier.parse(tableIdentifier);

        // Find the snapshot with the given snapshot id
        final Snapshot tableSnapshot = getSnapshot(tableId, snapshotId);
        if (tableSnapshot == null) {
            throw new IllegalArgumentException("Snapshot with id " + snapshotId + " not found");
        }

        // Load the table from the catalog.
        return getTableDefinition(tableId, tableSnapshot, instructions);
    }

    /**
     * Return {@link TableDefinition table definition} for a given Iceberg table and snapshot id, with optional
     * instructions for customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param tableSnapshot The snapshot to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition
     */
    public TableDefinition getTableDefinition(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        // Load the table from the catalog.
        return getTableDefinitionInternal(tableIdentifier, tableSnapshot, instructions);
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of a given Iceberg table, with
     * optional instructions for customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition as a Deephaven table
     */
    public Table getTableDefinitionTable(
            @NotNull final String tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        final TableIdentifier tableId = TableIdentifier.parse(tableIdentifier);
        return getTableDefinitionTable(tableId, instructions);
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of a given Iceberg table, with
     * optional instructions for customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition as a Deephaven table
     */
    public Table getTableDefinitionTable(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        final TableDefinition definition = getTableDefinition(tableIdentifier, instructions);
        return TableTools.metaTable(definition);
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of a given Iceberg table and
     * snapshot id, with optional instructions for customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param snapshotId The identifier of the snapshot to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition as a Deephaven table
     */
    public Table getTableDefinitionTable(
            @NotNull final String tableIdentifier,
            final long snapshotId,
            @Nullable final IcebergInstructions instructions) {
        final TableIdentifier tableId = TableIdentifier.parse(tableIdentifier);

        // Find the snapshot with the given snapshot id
        final Snapshot tableSnapshot = getSnapshot(tableId, snapshotId);
        if (tableSnapshot == null) {
            throw new IllegalArgumentException("Snapshot with id " + snapshotId + " not found");
        }

        return getTableDefinitionTable(tableId, tableSnapshot, instructions);
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of a given Iceberg table and
     * snapshot id, with optional instructions for customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param tableSnapshot The snapshot to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition as a Deephaven table
     */
    public Table getTableDefinitionTable(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        final TableDefinition definition = getTableDefinition(tableIdentifier, tableSnapshot, instructions);
        return TableTools.metaTable(definition);
    }

    /**
     * Internal method to create a {@link TableDefinition} from the table schema, snapshot and user instructions.
     */
    private TableDefinition getTableDefinitionInternal(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        final org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableIdentifier);
        }

        final Snapshot snapshot = tableSnapshot != null ? tableSnapshot : table.currentSnapshot();
        final Schema schema = snapshot != null ? table.schemas().get(snapshot.schemaId()) : table.schema();

        final IcebergInstructions userInstructions = instructions == null ? IcebergInstructions.DEFAULT : instructions;

        return fromSchema(schema,
                table.spec(),
                userInstructions.tableDefinition().orElse(null),
                getRenameColumnMap(table, schema, userInstructions));
    }

    /**
     * Read the latest static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    public Table readTable(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        return readTableInternal(tableIdentifier, null, instructions);
    }

    /**
     * Read the latest static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    public Table readTable(
            @NotNull final String tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        return readTable(TableIdentifier.parse(tableIdentifier), instructions);
    }

    /**
     * Retrieve a snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param tableSnapshotId The snapshot id to load
     * @return The loaded table
     * @throws IllegalArgumentException if the snapshot with the given id is not found
     */
    private Snapshot getTableSnapshot(@NotNull TableIdentifier tableIdentifier, long tableSnapshotId) {
        return listSnapshots(tableIdentifier).stream()
                .filter(snapshot -> snapshot.snapshotId() == tableSnapshotId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Snapshot with id " + tableSnapshotId + " for table " + tableIdentifier + " not found"));
    }

    /**
     * Read a static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param tableSnapshotId The snapshot id to load
     * @return The loaded table
     */
    public Table readTable(@NotNull final TableIdentifier tableIdentifier, final long tableSnapshotId) {
        // Find the snapshot with the given snapshot id
        final Snapshot tableSnapshot = getTableSnapshot(tableIdentifier, tableSnapshotId);

        return readTableInternal(tableIdentifier, tableSnapshot, null);
    }


    /**
     * Read a static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param tableSnapshotId The snapshot id to load
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    public Table readTable(@NotNull final String tableIdentifier, final long tableSnapshotId) {
        final TableIdentifier tableId = TableIdentifier.parse(tableIdentifier);
        // Find the snapshot with the given snapshot id
        final Snapshot tableSnapshot = getTableSnapshot(tableId, tableSnapshotId);

        return readTableInternal(tableId, tableSnapshot, null);
    }

    /**
     * Read a static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param tableSnapshotId The snapshot id to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    public Table readTable(
            @NotNull final TableIdentifier tableIdentifier,
            final long tableSnapshotId,
            @Nullable final IcebergInstructions instructions) {
        // Find the snapshot with the given snapshot id
        final Snapshot tableSnapshot = getSnapshot(tableIdentifier, tableSnapshotId);
        if (tableSnapshot == null) {
            throw new IllegalArgumentException("Snapshot with id " + tableSnapshotId + " not found");
        }
        return readTableInternal(tableIdentifier, tableSnapshot, instructions);
    }

    /**
     * Read a static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param tableSnapshotId The snapshot id to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    public Table readTable(
            @NotNull final String tableIdentifier,
            final long tableSnapshotId,
            @Nullable final IcebergInstructions instructions) {
        return readTable(TableIdentifier.parse(tableIdentifier), tableSnapshotId, instructions);
    }

    /**
     * Read a static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param tableSnapshot The {@link Snapshot snapshot} to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    public Table readTable(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        return readTableInternal(tableIdentifier, tableSnapshot, instructions);
    }

    private Table readTableInternal(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        // Load the table from the catalog.
        final org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableIdentifier);
        }

        // Do we want the latest or a specific snapshot?
        final Snapshot snapshot = tableSnapshot != null ? tableSnapshot : table.currentSnapshot();
        final Schema schema = snapshot == null ? table.schema() : table.schemas().get(snapshot.schemaId());

        // Load the partitioning schema.
        final org.apache.iceberg.PartitionSpec partitionSpec = table.spec();

        // Get default instructions if none are provided
        final IcebergInstructions userInstructions = instructions == null ? IcebergInstructions.DEFAULT : instructions;

        // Get the user supplied table definition.
        final TableDefinition userTableDef = userInstructions.tableDefinition().orElse(null);

        // Map all the column names in the schema to their legalized names.
        final Map<String, String> legalizedColumnRenames = getRenameColumnMap(table, schema, userInstructions);

        // Get the table definition from the schema (potentially limited by the user supplied table definition and
        // applying column renames).
        final TableDefinition tableDef = fromSchema(schema, partitionSpec, userTableDef, legalizedColumnRenames);

        final String description;
        final TableLocationKeyFinder<IcebergTableLocationKey> keyFinder;
        final TableDataRefreshService refreshService;
        final UpdateSourceRegistrar updateSourceRegistrar;

        if (partitionSpec.isUnpartitioned()) {
            // Create the flat layout location key finder
            keyFinder = new IcebergFlatLayout(tableDef, table, snapshot, table.io(), userInstructions,
                    dataInstructionsProvider);
        } else {
            // Create the partitioning column location key finder
            keyFinder = new IcebergKeyValuePartitionedLayout(tableDef, table, snapshot, table.io(), partitionSpec,
                    userInstructions, dataInstructionsProvider);
        }

        refreshService = null;
        updateSourceRegistrar = null;
        description = "Read static iceberg table with " + keyFinder;

        final AbstractTableLocationProvider locationProvider = new PollingTableLocationProvider<>(
                StandaloneTableKey.getInstance(),
                keyFinder,
                new IcebergTableLocationFactory(),
                refreshService);

        final PartitionAwareSourceTable result = new PartitionAwareSourceTable(
                tableDef,
                description,
                RegionedTableComponentFactoryImpl.INSTANCE,
                locationProvider,
                updateSourceRegistrar);

        return result;
    }

    /**
     * Returns the underlying Iceberg {@link Catalog catalog} used by this adapter.
     */
    public Catalog catalog() {
        return catalog;
    }

    /**
     * Append the provided deephaven table as a new partition to the existing iceberg table in a single snapshot. This
     * will not change the schema of the existing table.
     *
     * @param tableIdentifier The identifier string for the iceberg table to append to
     * @param dhTable The deephaven table to append
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     */
    public void append(
            @NotNull final String tableIdentifier,
            @NotNull final Table dhTable,
            @Nullable final IcebergWriteInstructions instructions) {
        append(tableIdentifier, new Table[] {dhTable}, instructions);
    }

    /**
     * Append the provided deephaven table as a new partition to the existing iceberg table in a single snapshot. This
     * will not change the schema of the existing table.
     *
     * @param tableIdentifier The identifier for the iceberg table to append to
     * @param dhTable The deephaven table to append
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     */
    public void append(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Table dhTable,
            @Nullable final IcebergWriteInstructions instructions) {
        append(tableIdentifier, new Table[] {dhTable}, instructions);
    }

    /**
     * Append the provided deephaven tables as new partitions to the existing iceberg table in a single snapshot. All
     * tables should have the same definition, else a table definition should be provided in the instructions. This will
     * not change the schema of the existing table.
     *
     * @param tableIdentifier The identifier string for the iceberg table to append to
     * @param dhTables The deephaven tables to append
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     */
    public void append(
            @NotNull final String tableIdentifier,
            @NotNull final Table[] dhTables,
            @Nullable final IcebergWriteInstructions instructions) {
        append(TableIdentifier.parse(tableIdentifier), dhTables, instructions);
    }

    /**
     * Append the provided deephaven tables as new partitions to the existing iceberg table in a single snapshot. All
     * tables should have the same definition, else a table definition should be provided in the instructions. This will
     * not change the schema of the existing table.
     *
     * @param tableIdentifier The identifier for the iceberg table to append to
     * @param dhTables The deephaven tables to append
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     */
    public void append(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Table[] dhTables,
            @Nullable final IcebergWriteInstructions instructions) {
        writeImpl(tableIdentifier, dhTables, instructions, false, true);
    }

    /**
     * Overwrite the existing iceberg table with the provided deephaven table in a single snapshot. This will change the
     * schema of the existing table to match the provided deephaven table.
     *
     * @param tableIdentifier The identifier string for the iceberg table to overwrite
     * @param dhTable The deephaven table to overwrite with
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     */
    public void overwrite(
            @NotNull final String tableIdentifier,
            @NotNull final Table dhTable,
            @Nullable final IcebergWriteInstructions instructions) {
        overwrite(tableIdentifier, new Table[] {dhTable}, instructions);
    }

    /**
     * Overwrite the existing iceberg table with the provided deephaven table in a single snapshot. This will change the
     * schema of the existing table to match the provided deephaven table.
     *
     * @param tableIdentifier The identifier for the iceberg table to overwrite
     * @param dhTable The deephaven table to overwrite with
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     */
    public void overwrite(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Table dhTable,
            @Nullable final IcebergWriteInstructions instructions) {
        overwrite(tableIdentifier, new Table[] {dhTable}, instructions);
    }

    /**
     * Overwrite the existing iceberg table with the provided deephaven tables appended together in a single snapshot.
     * All tables should have the same definition, else a table definition should be provided in the instructions. This
     * will change the schema of the existing table to match the provided deephaven tables.
     *
     * @param tableIdentifier The identifier string for the iceberg table to overwrite
     * @param dhTables The deephaven tables to overwrite with
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     */
    public void overwrite(
            @NotNull final String tableIdentifier,
            @NotNull final Table[] dhTables,
            @Nullable final IcebergWriteInstructions instructions) {
        overwrite(TableIdentifier.parse(tableIdentifier), dhTables, instructions);
    }

    /**
     * Overwrite the existing iceberg table with the provided deephaven tables appended together in a single snapshot.
     * All tables should have the same definition, else a table definition should be provided in the instructions. This
     * will change the schema of the existing table to match the provided deephaven tables.
     *
     * @param tableIdentifier The identifier for the iceberg table to overwrite
     * @param dhTables The deephaven tables to overwrite with
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     */
    public void overwrite(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Table[] dhTables,
            @Nullable final IcebergWriteInstructions instructions) {
        writeImpl(tableIdentifier, dhTables, instructions, true, true);
    }

    /**
     * Writes data from a Deephaven table to an Iceberg table without creating a new snapshot. This method returns a
     * list of data files that were written. Users can use this list to create a transaction/snapshot if needed.
     *
     * @param tableIdentifier The identifier string for the Iceberg table to write to.
     * @param dhTable The Deephaven table containing the data to be written.
     * @param instructions The instructions for customizations while writing, or null to use default instructions.
     *
     * @return A list of {@link DataFile} objects representing the written data files.
     */
    public List<DataFile> write(
            @NotNull final String tableIdentifier,
            @NotNull final Table dhTable,
            @Nullable final IcebergWriteInstructions instructions) {
        return write(tableIdentifier, new Table[] {dhTable}, instructions);
    }

    /**
     * Writes data from a Deephaven table to an Iceberg table without creating a new snapshot. This method returns a
     * list of data files that were written. Users can use this list to create a transaction/snapshot if needed.
     *
     * @param tableIdentifier The identifier for the Iceberg table to write to.
     * @param dhTable The Deephaven table containing the data to be written.
     * @param instructions The instructions for customizations while writing, or null to use default instructions.
     *
     * @return A list of {@link DataFile} objects representing the written data files.
     */
    public List<DataFile> write(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Table dhTable,
            @Nullable final IcebergWriteInstructions instructions) {
        return write(tableIdentifier, new Table[] {dhTable}, instructions);
    }

    /**
     * Writes data from Deephaven tables to an Iceberg table without creating a new snapshot. This method returns a list
     * of data files that were written. Users can use this list to create a transaction/snapshot if needed. All tables
     * should have the same definition, else a table definition should be provided in the instructions.
     *
     * @param tableIdentifier The identifier string for the Iceberg table to write to.
     * @param dhTables The Deephaven tables containing the data to be written.
     * @param instructions The instructions for customizations while writing, or null to use default instructions.
     *
     * @return A list of {@link DataFile} objects representing the written data files.
     */
    public List<DataFile> write(
            @NotNull final String tableIdentifier,
            @NotNull final Table[] dhTables,
            @Nullable final IcebergWriteInstructions instructions) {
        return write(TableIdentifier.parse(tableIdentifier), dhTables, instructions);
    }

    /**
     * Writes data from Deephaven tables to an Iceberg table without creating a new snapshot. This method returns a list
     * of data files that were written. Users can use this list to create a transaction/snapshot if needed. All tables
     * should have the same definition, else a table definition should be provided in the instructions.
     *
     * @param tableIdentifier The identifier for the Iceberg table to write to.
     * @param dhTables The Deephaven tables containing the data to be written.
     * @param instructions The instructions for customizations while writing, or null to use default instructions.
     *
     * @return A list of {@link DataFile} objects representing the written data files.
     */
    public List<DataFile> write(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Table[] dhTables,
            @Nullable final IcebergWriteInstructions instructions) {
        return writeImpl(tableIdentifier, dhTables, instructions, false, false);
    }

    /**
     * Appends or overwrites data in an Iceberg table with the provided Deephaven tables.
     *
     * @param tableIdentifier The identifier for the Iceberg table to append to or overwrite
     * @param dhTables The Deephaven tables to write
     * @param instructions The instructions for customizations while writing, or null to use default instructions
     * @param overwrite If true, the existing data in the Iceberg table will be overwritten; if false, the data will be
     *        appended
     * @param addSnapshot If true, a new snapshot will be created in the Iceberg table with the written data
     *
     * @return A list of DataFile objects representing the written data files.
     */
    private List<DataFile> writeImpl(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Table[] dhTables,
            @Nullable final IcebergWriteInstructions instructions,
            final boolean overwrite,
            final boolean addSnapshot) {
        final IcebergParquetWriteInstructions writeInstructions = verifyWriteInstructions(instructions);
        final boolean verifySchema = writeInstructions.verifySchema().orElse(!overwrite);

        // Verify that all tables have the same definition
        final TableDefinition useDefinition;
        if (writeInstructions.tableDefinition().isPresent()) {
            useDefinition = writeInstructions.tableDefinition().get();
        } else {
            final TableDefinition firstDefinition = dhTables[0].getDefinition();
            for (int idx = 1; idx < dhTables.length; idx++) {
                if (!firstDefinition.equals(dhTables[idx].getDefinition())) {
                    throw new IllegalArgumentException(
                            "All Deephaven tables must have the same definition, else table definition should be " +
                                    "provided when writing multiple tables with different definitions");
                }
            }
            useDefinition = firstDefinition;
        }

        // Try loading the table from the catalog, or create if required
        final org.apache.iceberg.Table icebergTable;
        final SpecAndSchema newSpecAndSchema;
        final boolean newNamespaceCreated;
        final boolean newTableCreated;
        if (catalog.tableExists(tableIdentifier)) {
            icebergTable = catalog.loadTable(tableIdentifier);
            newSpecAndSchema = fromTableDefinition(useDefinition, writeInstructions);
            newNamespaceCreated = false;
            newTableCreated = false;
            if (verifySchema) {
                try {
                    if (overwrite) {
                        verifyOverwriteCompatibility(icebergTable.schema(), newSpecAndSchema.schema);
                        verifyOverwriteCompatibility(icebergTable.spec(), newSpecAndSchema.partitionSpec);
                    } else {
                        verifyAppendCompatibility(icebergTable.schema(), useDefinition);
                        verifyAppendCompatibility(icebergTable.spec(), useDefinition);
                    }
                } catch (final IllegalArgumentException e) {
                    throw new IllegalArgumentException("Schema verification failed. Please provide a compatible " +
                            "schema or disable verification in the Iceberg instructions. See the linked exception " +
                            "for more details.", e);
                }
            }
        } else if (writeInstructions.createTableIfNotExist()) {
            newNamespaceCreated = createNamespaceIfNotExists(tableIdentifier.namespace());
            newSpecAndSchema = fromTableDefinition(useDefinition, writeInstructions);
            icebergTable = createNewIcebergTable(tableIdentifier, newSpecAndSchema, writeInstructions);
            newTableCreated = true;
        } else {
            throw new IllegalArgumentException(
                    "Table does not exist: " + tableIdentifier + ", update the instructions " +
                            "to create the table if it does not exist and try again.");
        }

        try {
            final List<CompletedParquetWrite> parquetFileinfo =
                    writeParquet(icebergTable, dhTables, writeInstructions);
            final List<DataFile> appendFiles = dataFilesFromParquet(parquetFileinfo);
            if (addSnapshot) {
                commit(icebergTable, newSpecAndSchema, appendFiles, overwrite && !newTableCreated, verifySchema);
            }
            return appendFiles;
        } catch (final RuntimeException writeException) {
            if (newTableCreated) {
                // Delete it to avoid leaving a partial table in the catalog
                try {
                    catalog.dropTable(tableIdentifier, true);
                } catch (final RuntimeException dropException) {
                    writeException.addSuppressed(dropException);
                }
            }
            if (newNamespaceCreated) {
                // Delete it to avoid leaving a partial namespace in the catalog
                dropNamespaceIfExists(tableIdentifier.namespace());
            }
            throw writeException;
        }
    }

    private static IcebergParquetWriteInstructions verifyWriteInstructions(
            @Nullable final IcebergWriteInstructions instructions) {
        if (instructions == null) {
            return IcebergParquetWriteInstructions.DEFAULT;
        }
        // We ony support writing to Parquet files
        if (!(instructions instanceof IcebergParquetWriteInstructions)) {
            throw new IllegalArgumentException("Unsupported instructions of class " + instructions.getClass() + " for" +
                    " writing Iceberg table, expected: " + IcebergParquetWriteInstructions.class);
        }
        return (IcebergParquetWriteInstructions) instructions;
    }

    private boolean createNamespaceIfNotExists(@NotNull final Namespace namespace) {
        if (catalog instanceof SupportsNamespaces) {
            final SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            if (!nsCatalog.namespaceExists(namespace)) {
                nsCatalog.createNamespace(namespace);
                return true;
            }
        }
        return false;
    }

    private void dropNamespaceIfExists(@NotNull final Namespace namespace) {
        if (catalog instanceof SupportsNamespaces) {
            final SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            nsCatalog.dropNamespace(namespace);
        }
    }

    private static class SpecAndSchema {
        private final PartitionSpec partitionSpec;
        private Schema schema;

        private SpecAndSchema(final PartitionSpec partitionSpec, final Schema schema) {
            this.partitionSpec = partitionSpec;
            this.schema = schema;
        }
    }

    /**
     * Create {@link PartitionSpec} and {@link Schema} from a {@link TableDefinition}.
     */
    private static SpecAndSchema fromTableDefinition(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final IcebergBaseInstructions instructions) {
        final Collection<String> partitioningColumnNames = new ArrayList<>();
        final List<Types.NestedField> fields = new ArrayList<>();
        int fieldID = 1; // Iceberg field IDs start from 1

        // Create the schema first and use it to build the partition spec
        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
            final String dhColumnName = columnDefinition.getName();
            final String icebergColName = instructions.columnRenames().getOrDefault(dhColumnName, dhColumnName);
            final Type icebergType = convertToIcebergType(columnDefinition.getDataType());
            fields.add(Types.NestedField.optional(fieldID, icebergColName, icebergType));
            if (columnDefinition.isPartitioning()) {
                partitioningColumnNames.add(icebergColName);
            }
            fieldID++;
        }
        final Schema schema = new Schema(fields);

        final PartitionSpec partitionSpec = createPartitionSpec(schema, partitioningColumnNames);
        return new SpecAndSchema(partitionSpec, schema);
    }

    private static PartitionSpec createPartitionSpec(
            @NotNull final Schema schema,
            @NotNull final Iterable<String> partitionColumnNames) {
        final PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
        for (final String partitioningColumnName : partitionColumnNames) {
            partitionSpecBuilder.identity(partitioningColumnName);
        }
        return partitionSpecBuilder.build();
    }

    private org.apache.iceberg.Table createNewIcebergTable(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final SpecAndSchema specAndSchema,
            @NotNull final IcebergParquetWriteInstructions writeInstructions) {
        if (catalog.tableExists(tableIdentifier)) {
            throw new IllegalArgumentException("Table already exists: " + tableIdentifier);
        }
        return catalog.createTable(tableIdentifier, specAndSchema.schema, specAndSchema.partitionSpec, null,
                Map.of(TableProperties.DEFAULT_FILE_FORMAT, DEFAULT_GENERATED_FILE_FORMAT,
                        TableProperties.PARQUET_COMPRESSION,
                        StringUtils.toLowerCase(writeInstructions.compressionCodecName())));
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
    private static List<CompletedParquetWrite> writeParquet(
            @NotNull final org.apache.iceberg.Table icebergTable,
            @NotNull final Table[] dhTables,
            @NotNull final IcebergParquetWriteInstructions writeInstructions) {
        // Build the parquet instructions
        final List<CompletedParquetWrite> parquetFilesWritten = new ArrayList<>(dhTables.length);
        final ParquetInstructions.OnWriteCompleted onWriteCompleted =
                (destination, numRows, numBytes) -> parquetFilesWritten
                        .add(new CompletedParquetWrite(destination, numRows, numBytes));
        final ParquetInstructions parquetInstructions = writeInstructions.toParquetInstructions(
                onWriteCompleted, icebergTable.schema().idToName());

        // Write the data to parquet files
        for (final Table dhTable : dhTables) {
            final long epochMicrosNow = Instant.now().getEpochSecond() * 1_000_000 + Instant.now().getNano() / 1_000;
            final String filename = new StringBuilder()
                    .append(epochMicrosNow) // To keep the data ordered by time
                    .append("-")
                    .append(UUID.randomUUID())
                    .append(".parquet")
                    .toString();
            final String newDataLocation =
                    icebergTable.locationProvider().newDataLocation(filename);
            ParquetTools.writeTable(dhTable, newDataLocation, parquetInstructions);
        }
        return parquetFilesWritten;
    }

    /**
     * Commit the changes to the Iceberg table by creating a single snapshot.
     */
    private static void commit(
            @NotNull final org.apache.iceberg.Table icebergTable,
            @NotNull final SpecAndSchema newSpecAndSchema,
            @NotNull final Iterable<DataFile> appendFiles,
            final boolean overwrite,
            final boolean schemaVerified) {
        // Append new data files to the table
        final Transaction icebergTransaction = icebergTable.newTransaction();

        if (overwrite) {
            // Delete all the existing data files in the table
            final DeleteFiles deletes = icebergTransaction.newDelete();
            try (final Stream<DataFile> dataFiles = getAllDataFiles(icebergTable, icebergTable.currentSnapshot())) {
                dataFiles.forEach(deletes::deleteFile);
            }
            deletes.commit();

            // Update the spec and schema of the existing table.
            // If we have already verified the schema, we don't need to update it.
            if (!schemaVerified) {
                if (!icebergTable.schema().sameSchema(newSpecAndSchema.schema)) {
                    final UpdateSchema updateSchema = icebergTransaction.updateSchema().allowIncompatibleChanges();
                    icebergTable.schema().columns().stream()
                            .map(Types.NestedField::name)
                            .forEach(updateSchema::deleteColumn);
                    newSpecAndSchema.schema.columns()
                            .forEach(column -> updateSchema.addColumn(column.name(), column.type()));
                    updateSchema.commit();
                }
                if (!icebergTable.spec().compatibleWith(newSpecAndSchema.partitionSpec)) {
                    final UpdatePartitionSpec updateSpec = icebergTransaction.updateSpec();
                    icebergTable.spec().fields().forEach(field -> updateSpec.removeField(field.name()));
                    newSpecAndSchema.partitionSpec.fields().forEach(field -> updateSpec.addField(field.name()));
                    updateSpec.commit();
                }
            }
        }

        // Append the new data files to the table
        final AppendFiles append = icebergTransaction.newAppend();
        appendFiles.forEach(append::appendFile);
        append.commit();

        // Commit the transaction to create a new snapshot
        icebergTransaction.commitTransaction();
    }

    /**
     * Generate a list of {@link DataFile} objects from a list of parquet files written.
     */
    private static List<DataFile> dataFilesFromParquet(
            @NotNull final Collection<CompletedParquetWrite> parquetFilesWritten) {
        if (parquetFilesWritten.isEmpty()) {
            throw new UncheckedDeephavenException("Failed to generate data files because no parquet files written");
        }
        // TODO This assumes no partition data is written, is that okay?
        return parquetFilesWritten.stream()
                .map(parquetFileWritten -> DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath(parquetFileWritten.destination.toString())
                        .withFormat(FileFormat.PARQUET)
                        .withRecordCount(parquetFileWritten.numRows)
                        .withFileSizeInBytes(parquetFileWritten.numBytes)
                        .build())
                .collect(Collectors.toList());
    }
}
