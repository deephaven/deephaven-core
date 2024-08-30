//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

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

    private final Catalog catalog;

    /**
     * Construct an IcebergCatalogAdapter from a catalog and file IO.
     */
    IcebergCatalogAdapter(@NotNull final Catalog catalog) {
        this.catalog = catalog;
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
    public Table namespaces() {
        return namespaces(Namespace.empty());
    }

    /**
     * List all {@link Namespace namespaces} in a given namespace as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listNamespaces(Namespace)}.
     *
     * @return A {@link Table table} of all namespaces.
     */
    public Table namespaces(@NotNull final Namespace namespace) {
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
    @SuppressWarnings("unused")
    public Table namespaces(@NotNull final String... namespace) {
        return namespaces(Namespace.of(namespace));
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
    public Table tables(@NotNull final Namespace namespace) {
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

    public Table tables(@NotNull final String... namespace) {
        return tables(Namespace.of(namespace));
    }

    /**
     * Load an Iceberg table from the catalog.
     *
     * @param tableIdentifier The identifier of the table to load.
     * @return The {@link IcebergTableAdapter table adapter} for the Iceberg table.
     */
    public IcebergTableAdapter loadTable(@NotNull final TableIdentifier tableIdentifier) {
        // Load the table from the catalog.
        final org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableIdentifier);
        }
        return new IcebergTableAdapter(tableIdentifier, table);
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table.
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all snapshots of the given table.
     */
    @Deprecated(forRemoval = true)
    public List<Snapshot> listSnapshots(@NotNull final String tableIdentifier) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.listSnapshots();
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table.
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all snapshots of the given table.
     */
    @Deprecated(forRemoval = true)
    public List<Snapshot> listSnapshots(@NotNull final TableIdentifier tableIdentifier) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.listSnapshots();
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
     * <td>TimestampMs</td>
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
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all tables in the given namespace.
     */
    @Deprecated(forRemoval = true)
    public Table snapshots(@NotNull final TableIdentifier tableIdentifier) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.snapshots();
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listSnapshots(TableIdentifier)}.
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all tables in the given namespace.
     */
    @Deprecated(forRemoval = true)
    public Table snapshots(@NotNull final String tableIdentifier) {
        return snapshots(TableIdentifier.parse(tableIdentifier));
    }

    /**
     * Return {@link TableDefinition table definition} for a given Iceberg table, with optional instructions for
     * customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition
     */
    @Deprecated(forRemoval = true)
    public TableDefinition getTableDefinition(
            @NotNull final String tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.definition(instructions);
    }

    /**
     * Return {@link TableDefinition table definition} for a given Iceberg table, with optional instructions for
     * customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition
     */
    @Deprecated(forRemoval = true)
    public TableDefinition getTableDefinition(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.definition(instructions);
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
    @Deprecated(forRemoval = true)
    public TableDefinition getTableDefinition(
            @NotNull final String tableIdentifier,
            final long snapshotId,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.definition(snapshotId, instructions);
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
    @Deprecated(forRemoval = true)
    public TableDefinition getTableDefinition(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.definition(tableSnapshot, instructions);
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of a given Iceberg table, with
     * optional instructions for customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition as a Deephaven table
     */
    @Deprecated(forRemoval = true)
    public Table getTableDefinitionTable(
            @NotNull final String tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.definitionTable(instructions);
    }

    /**
     * Return {@link Table table} containing the {@link TableDefinition definition} of a given Iceberg table, with
     * optional instructions for customizations while reading.
     *
     * @param tableIdentifier The identifier of the table to load
     * @param instructions The instructions for customizations while reading
     * @return The table definition as a Deephaven table
     */
    @Deprecated(forRemoval = true)
    public Table getTableDefinitionTable(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.definitionTable(instructions);
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
    @Deprecated(forRemoval = true)
    public Table getTableDefinitionTable(
            @NotNull final String tableIdentifier,
            final long snapshotId,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.definitionTable(snapshotId, instructions);
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
    @Deprecated(forRemoval = true)
    public Table getTableDefinitionTable(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.definitionTable(tableSnapshot, instructions);
    }

    /**
     * Read the latest static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    @Deprecated(forRemoval = true)
    public IcebergTable readTable(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.table(instructions);
    }

    /**
     * Read the latest static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    @Deprecated(forRemoval = true)
    public IcebergTable readTable(
            @NotNull final String tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.table(instructions);
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
    @Deprecated(forRemoval = true)
    public IcebergTable readTable(
            @NotNull final TableIdentifier tableIdentifier,
            final long tableSnapshotId,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.table(tableSnapshotId, instructions);
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
    @Deprecated(forRemoval = true)
    public IcebergTable readTable(
            @NotNull final String tableIdentifier,
            final long tableSnapshotId,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.table(tableSnapshotId, instructions);
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
    @Deprecated(forRemoval = true)
    public IcebergTable readTable(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        final IcebergTableAdapter tableAdapter = loadTable(tableIdentifier);
        return tableAdapter.table(tableSnapshot, instructions);
    }

    /**
     * Load an Iceberg table from the catalog.
     *
     * @param tableIdentifier The identifier of the table to load.
     * @return The {@link IcebergTableAdapter table adapter} for the Iceberg table.
     */
    @Deprecated(forRemoval = true)
    public IcebergTableAdapter loadTable(final String tableIdentifier) {
        return loadTable(TableIdentifier.parse(tableIdentifier));
    }

    /**
     * Returns the underlying Iceberg {@link Catalog catalog} used by this adapter.
     */
    @SuppressWarnings("unused")
    public Catalog catalog() {
        return catalog;
    }
}
