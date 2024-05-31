//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

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
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.iceberg.layout.IcebergFlatLayout;
import io.deephaven.iceberg.layout.IcebergKeyValuePartitionedLayout;
import io.deephaven.iceberg.location.IcebergTableLocationFactory;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class IcebergCatalogAdapter {
    private final Catalog catalog;
    private final FileIO fileIO;

    /**
     * Construct an IcebergCatalogAdapter from a catalog and file IO.
     */
    IcebergCatalogAdapter(
            @NotNull final Catalog catalog,
            @NotNull final FileIO fileIO) {
        this.catalog = catalog;
        this.fileIO = fileIO;
    }

    private static TableDefinition fromSchema(
            @NotNull final Schema schema,
            @NotNull final PartitionSpec partitionSpec,
            @Nullable final TableDefinition tableDefinition,
            @NotNull final Map<String, String> columnRenameMap) {

        final Set<String> columnNames = tableDefinition != null
                ? tableDefinition.getColumnNameSet()
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
            final io.deephaven.qst.type.Type<?> qstType = convertPrimitiveType(type);
            final ColumnDefinition<?> column;
            if (partitionNames.contains(name)) {
                column = ColumnDefinition.of(name, qstType).withPartitioning();
            } else {
                column = ColumnDefinition.of(name, qstType);
            }
            columns.add(column);
        }

        return TableDefinition.of(columns);
    }

    static io.deephaven.qst.type.Type<?> convertPrimitiveType(@NotNull final Type icebergType) {
        final Type.TypeID typeId = icebergType.typeId();
        switch (typeId) {
            case BOOLEAN:
                return io.deephaven.qst.type.Type.booleanType();
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

    public List<Namespace> listNamespaces() {
        return listNamespaces(Namespace.empty());
    }

    public List<Namespace> listNamespaces(@NotNull Namespace namespace) {
        if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces) {
            final SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            return nsCatalog.listNamespaces(namespace);
        }
        throw new UnsupportedOperationException(String.format(
                "%s does not implement org.apache.iceberg.catalog.SupportsNamespaces", catalog.getClass().getName()));
    }

    public Table listNamespacesAsTable() {
        return listNamespacesAsTable(Namespace.empty());
    }

    public Table listNamespacesAsTable(@NotNull Namespace namespace) {
        final List<Namespace> namespaces = listNamespaces();
        final long size = namespaces.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final WritableColumnSource<String> namespaceColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, String.class, null);
        columnSourceMap.put("namespace", namespaceColumn);

        final WritableColumnSource<Object> objectColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, Object.class, Namespace.class);
        columnSourceMap.put("namespace_object", objectColumn);

        // Populate the column source(s)
        for (int i = 0; i < size; i++) {
            final Namespace ns = namespaces.get(i);
            namespaceColumn.set(i, ns.toString());
            objectColumn.set(i, ns);
        }

        // Create and return the table
        return new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    public List<TableIdentifier> listTables(@NotNull final Namespace namespace) {
        return catalog.listTables(namespace);
    }

    public Table listTablesAsTable(@NotNull final Namespace namespace) {
        final List<TableIdentifier> tableIdentifiers = listTables(namespace);
        final long size = tableIdentifiers.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final WritableColumnSource<String> namespaceColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, String.class, null);
        columnSourceMap.put("namespace", namespaceColumn);

        final WritableColumnSource<String> tableColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, String.class, null);
        columnSourceMap.put("table_name", tableColumn);

        final WritableColumnSource<Object> objectColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, Object.class, TableIdentifier.class);
        columnSourceMap.put("table_identifier_object", objectColumn);

        // Populate the column source(s)
        for (int i = 0; i < size; i++) {
            final TableIdentifier tableIdentifier = tableIdentifiers.get(i);
            namespaceColumn.set(i, tableIdentifier.namespace().toString());
            tableColumn.set(i, tableIdentifier.name());
            objectColumn.set(i, tableIdentifier);
        }

        // Create and return the table
        return new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    public List<Snapshot> listSnapshots(@NotNull final TableIdentifier tableIdentifier) {
        final List<Snapshot> snapshots = new ArrayList<>();
        catalog.loadTable(tableIdentifier).snapshots().forEach(snapshots::add);
        return snapshots;
    }

    public Table listSnapshotsAsTable(@NotNull final TableIdentifier tableIdentifier) {
        final List<Snapshot> snapshots = listSnapshots(tableIdentifier);
        final long size = snapshots.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final WritableColumnSource<Long> idColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, Long.class, null);
        columnSourceMap.put("id", idColumn);

        final WritableColumnSource<Long> timestampMsColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, Long.class, null);
        columnSourceMap.put("timestamp_ms", timestampMsColumn);

        final WritableColumnSource<String> operationColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, String.class, null);
        columnSourceMap.put("operation", operationColumn);

        final WritableColumnSource<Object> summaryColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, Object.class, Map.class);
        columnSourceMap.put("summary", summaryColumn);

        final WritableColumnSource<Object> objectColumn =
                ArrayBackedColumnSource.getMemoryColumnSource(size, Object.class, Snapshot.class);
        columnSourceMap.put("snapshot_object", objectColumn);

        // Populate the column source(s)
        for (int i = 0; i < size; i++) {
            final Snapshot snapshot = snapshots.get(i);
            idColumn.set(i, snapshot.snapshotId());
            timestampMsColumn.set(i, snapshot.timestampMillis());
            operationColumn.set(i, snapshot.operation());
            summaryColumn.set(i, snapshot.summary());
            objectColumn.set(i, snapshot);
        }

        // Create and return the table
        return new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    /**
     * Read the latest static snapshot of a table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    public Table readTable(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final IcebergInstructions instructions) {
        return readTableInternal(tableIdentifier, null, instructions);
    }

    /**
     * Read a static snapshot of a table from the Iceberg catalog.
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
            @NotNull final IcebergInstructions instructions) {
        return readTableInternal(tableIdentifier, tableSnapshot, instructions);
    }

    private Table readTableInternal(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final Snapshot tableSnapshot,
            @NotNull final IcebergInstructions instructions) {

        // Load the table from the catalog
        final org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);

        // Do we want the latest or a specific snapshot?
        final Snapshot snapshot = tableSnapshot != null ? tableSnapshot : table.currentSnapshot();
        final Schema schema = table.schemas().get(snapshot.schemaId());

        // Load the partitioning schema
        final org.apache.iceberg.PartitionSpec partitionSpec = table.spec();

        // Get the user supplied table definition.
        final TableDefinition userTableDef = instructions.tableDefinition().orElse(null);

        // Get the table definition from the schema (potentially limited by the user supplied table definition and
        // applying column renames).
        final TableDefinition icebergTableDef =
                fromSchema(schema, partitionSpec, userTableDef, instructions.columnRenameMap());

        // If the user supplied a table definition, make sure it's fully compatible.
        final TableDefinition tableDef;
        if (userTableDef != null) {
            tableDef = icebergTableDef.checkCompatibility(userTableDef);
        } else {
            // Use the snapshot schema as the table definition.
            tableDef = icebergTableDef;
        }

        final String description;
        final TableLocationKeyFinder<IcebergTableLocationKey> keyFinder;
        final TableDataRefreshService refreshService;
        final UpdateSourceRegistrar updateSourceRegistrar;

        if (partitionSpec.isUnpartitioned()) {
            // Create the flat layout location key finder
            keyFinder = new IcebergFlatLayout(tableDef, table, snapshot, fileIO, instructions);
        } else {
            // Create the partitioning column location key finder
            keyFinder = new IcebergKeyValuePartitionedLayout(tableDef, table, snapshot, fileIO, partitionSpec,
                    instructions);
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
}
