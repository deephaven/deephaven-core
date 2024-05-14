//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.PollingTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class IcebergCatalogAdapter {
    private final Catalog catalog;
    private final FileIO fileIO;
    private final IcebergInstructions instructions;

    /**
     * Construct an IcebergCatalogAdapter given a set of configurable instructions.
     */
    @SuppressWarnings("unused")
    IcebergCatalogAdapter(
            @NotNull final Catalog catalog,
            @NotNull final FileIO fileIO) {
        this(catalog, fileIO, IcebergInstructions.builder().build());
    }

    /**
     * Construct an IcebergCatalogAdapter given a set of configurable instructions.
     */
    IcebergCatalogAdapter(
            @NotNull final Catalog catalog,
            @NotNull final FileIO fileIO,
            @NotNull final IcebergInstructions instructions) {
        this.catalog = catalog;
        this.fileIO = fileIO;
        this.instructions = instructions;
    }

    static TableDefinition fromSchema(
            final Schema schema,
            final PartitionSpec partitionSpec,
            final IcebergInstructions instructions) {
        final Map<String, String> renameMap = instructions.columnRenameMap();

        final Set<String> columnNames = instructions.tableDefinition().isPresent()
                ? new HashSet<>(instructions.tableDefinition().get().getColumnNames())
                : null;

        final Set<String> partitionNames =
                partitionSpec.fields().stream()
                        .map(PartitionField::name)
                        .map(colName -> renameMap.getOrDefault(colName, colName))
                        .collect(Collectors.toSet());

        final List<ColumnDefinition<?>> columns = new ArrayList<>();

        for (final Types.NestedField field : schema.columns()) {
            final String name = renameMap.getOrDefault(field.name(), field.name());
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

    static io.deephaven.qst.type.Type<?> convertPrimitiveType(final Type icebergType) {
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
            default:
                throw new TableDataException("Unsupported iceberg column type " + typeId.name());
        }
    }

    @SuppressWarnings("unused")
    public List<TableIdentifier> listTables(final Namespace namespace) {
        // TODO: have this return a Deephaven Table of table identifiers
        return catalog.listTables(namespace);
    }

    public List<Long> listTableSnapshots(@NotNull final TableIdentifier tableIdentifier) {
        final List<Long> snapshotIds = new ArrayList<>();
        catalog.loadTable(tableIdentifier).snapshots().forEach(snapshot -> snapshotIds.add(snapshot.snapshotId()));
        return snapshotIds;
    }

    /**
     * Read the latest static snapshot of a table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    public Table snapshotTable(@NotNull final TableIdentifier tableIdentifier) {
        return readTableInternal(tableIdentifier, -1, false);
    }

    /**
     * Read a static snapshot of a table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param snapshotId The snapshot ID to load
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    public Table snapshotTable(
            @NotNull final TableIdentifier tableIdentifier,
            final long snapshotId) {
        return readTableInternal(tableIdentifier, snapshotId, false);
    }

    private Table readTableInternal(
            @NotNull final TableIdentifier tableIdentifier,
            final long snapshotId,
            final boolean isRefreshing) {

        // Load the table from the catalog
        final org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);

        // Do we want the latest or a specific snapshot?
        final Snapshot snapshot;
        if (snapshotId < 0) {
            snapshot = table.currentSnapshot();
        } else {
            snapshot = table.snapshot(snapshotId);
        }
        final Schema schema = table.schemas().get(snapshot.schemaId());

        // Load the partitioning schema
        final org.apache.iceberg.PartitionSpec partitionSpec = table.spec();

        // Load the table-definition from the snapshot schema.
        final TableDefinition icebergTableDef = fromSchema(schema, partitionSpec, instructions);

        // If the user supplied a table definition, make sure it's fully compatible.
        final TableDefinition tableDef;
        if (instructions.tableDefinition().isPresent()) {
            final TableDefinition userTableDef = instructions.tableDefinition().get();
            tableDef = icebergTableDef.checkMutualCompatibility(userTableDef);
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
            keyFinder = new IcebergFlatLayout(table, snapshot, fileIO, instructions);
        } else {
            final String[] partitionColumns = partitionSpec.fields().stream()
                    .map(PartitionField::name)
                    .map(colName -> instructions.columnRenameMap().getOrDefault(colName, colName))
                    .toArray(String[]::new);

            // Verify that the partitioning columns are present in the table definition.
            final Map<String, ColumnDefinition<?>> columnDefinitionMap = tableDef.getColumnNameMap();
            final String[] missingColumns = Arrays.stream(partitionColumns)
                    .filter(col -> !columnDefinitionMap.containsKey(col)).toArray(String[]::new);
            if (missingColumns.length > 0) {
                throw new IllegalStateException(
                        String.format("%s:%d - Partitioning column(s) %s were not found in the table definition",
                                table, snapshot.snapshotId(), Arrays.toString(missingColumns)));
            }

            // Create the partitioning column location key finder
            keyFinder = new IcebergKeyValuePartitionedLayout(
                    tableDef,
                    table,
                    snapshot,
                    fileIO,
                    instructions);
        }

        if (isRefreshing) {
            refreshService = TableDataRefreshService.getSharedRefreshService();
            updateSourceRegistrar = ExecutionContext.getContext().getUpdateGraph();
            description = "Read refreshing iceberg table with " + keyFinder;
        } else {
            refreshService = null;
            updateSourceRegistrar = null;
            description = "Read static iceberg table with " + keyFinder;
        }

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
