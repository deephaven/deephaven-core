//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

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
import io.deephaven.iceberg.internal.DataInstructionsProviderPlugin;
import io.deephaven.iceberg.layout.IcebergFlatLayout;
import io.deephaven.iceberg.layout.IcebergKeyValuePartitionedLayout;
import io.deephaven.iceberg.location.IcebergTableLocationFactory;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

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

    private final Catalog catalog;

    private final DataInstructionsProviderLoader dataInstructionsProvider;

    /**
     * Construct an IcebergCatalogAdapter from a Catalog. The properties supplied are provided to support
     * {@link DataInstructionsProviderPlugin} resolution. In the case where {@code catalog} is a {@link RESTCatalog},
     * {@link RESTCatalog#properties()} will be used instead.
     *
     * @param catalog the catalog
     * @return the catalog adapter
     */
    static IcebergCatalogAdapter of(Catalog catalog, Map<String, String> properties) {
        if (catalog instanceof RESTCatalog) {
            return of((RESTCatalog) catalog);
        }
        return new IcebergCatalogAdapter(catalog, properties);
    }

    /**
     * Construct an IcebergCatalogAdapter from a REST catalog. This passes along {@link RESTCatalog#properties()} which
     * will include any additional properties the REST Catalog implementation sent back as part of the initial
     * {@link ResourcePaths#config() config} call. These properties will be used for resolving
     * {@link io.deephaven.iceberg.internal.DataInstructionsProviderPlugin}.
     *
     * @param restCatalog the rest catalog
     * @return the catalog adapter
     */
    static IcebergCatalogAdapter of(RESTCatalog restCatalog) {
        return new IcebergCatalogAdapter(restCatalog, restCatalog.properties());
    }

    /**
     * Construct an IcebergCatalogAdapter from a catalog.
     *
     * @deprecated use a method or constructor which is more explicit about the properties it uses
     */
    @Deprecated(forRemoval = true)
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
            final io.deephaven.qst.type.Type<?> qstType = convertPrimitiveType(type);
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
     * Convert an Iceberg data type to a Deephaven type.
     *
     * @param icebergType The Iceberg data type to be converted.
     * @return The converted Deephaven type.
     */
    static io.deephaven.qst.type.Type<?> convertPrimitiveType(@NotNull final Type icebergType) {
        final Type.TypeID typeId = icebergType.typeId();
        switch (typeId) {
            case BOOLEAN:
                return io.deephaven.qst.type.Type.booleanType().boxedType();
            case DOUBLE:
                return io.deephaven.qst.type.Type.doubleType();
            case FLOAT:
                return io.deephaven.qst.type.Type.floatType();
            case INTEGER:
                return io.deephaven.qst.type.Type.intType();
            case LONG:
                return io.deephaven.qst.type.Type.longType();
            case STRING:
                return io.deephaven.qst.type.Type.stringType();
            case TIMESTAMP:
                final Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                return timestampType.shouldAdjustToUTC()
                        ? io.deephaven.qst.type.Type.find(Instant.class)
                        : io.deephaven.qst.type.Type.find(LocalDateTime.class);
            case DATE:
                return io.deephaven.qst.type.Type.find(java.time.LocalDate.class);
            case TIME:
                return io.deephaven.qst.type.Type.find(java.time.LocalTime.class);
            case DECIMAL:
                return io.deephaven.qst.type.Type.find(java.math.BigDecimal.class);
            case FIXED: // Fall through
            case BINARY:
                return io.deephaven.qst.type.Type.find(byte[].class);
            case UUID: // Fall through
            case STRUCT: // Fall through
            case LIST: // Fall through
            case MAP: // Fall through
            default:
                throw new TableDataException("Unsupported iceberg column type " + typeId.name());
        }
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
    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
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
    @SuppressWarnings("unused")
    public Catalog catalog() {
        return catalog;
    }
}
