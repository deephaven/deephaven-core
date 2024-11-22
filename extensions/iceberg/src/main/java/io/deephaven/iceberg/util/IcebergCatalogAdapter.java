//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.iceberg.internal.DataInstructionsProviderPlugin;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.jetbrains.annotations.NotNull;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.ResourcePaths;

import java.util.*;

import static io.deephaven.iceberg.base.IcebergUtils.createNamespaceIfNotExists;
import static io.deephaven.iceberg.base.IcebergUtils.dropNamespaceIfExists;

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
    public IcebergTableAdapter loadTable(final String tableIdentifier) {
        return loadTable(TableIdentifier.parse(tableIdentifier));
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
        return new IcebergTableAdapter(catalog, tableIdentifier, table, dataInstructionsProvider);
    }

    /**
     * Returns the underlying Iceberg {@link Catalog catalog} used by this adapter.
     */
    public Catalog catalog() {
        return catalog;
    }

    /**
     * Create a new Iceberg table in the catalog with the given table identifier and definition.
     * <p>
     * All columns of type {@link ColumnDefinition.ColumnType#Partitioning partitioning} will be used to create the
     * partition spec for the table.
     *
     * @param tableIdentifier The identifier string of the new table.
     * @param definition The {@link TableDefinition} of the new table.
     * @return The {@link IcebergTableAdapter table adapter} for the new Iceberg table.
     * @throws AlreadyExistsException if the table already exists
     */
    public IcebergTableAdapter createTable(
            @NotNull final String tableIdentifier,
            @NotNull final TableDefinition definition) {
        return createTable(TableIdentifier.parse(tableIdentifier), definition);
    }

    /**
     * Create a new Iceberg table in the catalog with the given table identifier and definition.
     * <p>
     * All columns of type {@link ColumnDefinition.ColumnType#Partitioning partitioning} will be used to create the
     * partition spec for the table.
     *
     * @param tableIdentifier The identifier of the new table.
     * @param definition The {@link TableDefinition} of the new table.
     * @return The {@link IcebergTableAdapter table adapter} for the new Iceberg table.
     * @throws AlreadyExistsException if the table already exists
     */
    public IcebergTableAdapter createTable(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final TableDefinition definition) {
        final IcebergUtils.SpecAndSchema specAndSchema = IcebergUtils.createSpecAndSchema(definition);
        return createTable(tableIdentifier, specAndSchema.schema, specAndSchema.partitionSpec);
    }

    private IcebergTableAdapter createTable(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Schema schema,
            @NotNull final PartitionSpec partitionSpec) {
        final boolean newNamespaceCreated = createNamespaceIfNotExists(catalog, tableIdentifier.namespace());
        try {
            final org.apache.iceberg.Table table =
                    catalog.createTable(tableIdentifier, schema, partitionSpec,
                            Map.of(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));
            return new IcebergTableAdapter(catalog, tableIdentifier, table, dataInstructionsProvider);
        } catch (final Throwable throwable) {
            if (newNamespaceCreated) {
                // Delete it to avoid leaving a partial namespace in the catalog
                try {
                    dropNamespaceIfExists(catalog, tableIdentifier.namespace());
                } catch (final RuntimeException dropException) {
                    throwable.addSuppressed(dropException);
                }
            }
            throw throwable;
        }
    }
}
