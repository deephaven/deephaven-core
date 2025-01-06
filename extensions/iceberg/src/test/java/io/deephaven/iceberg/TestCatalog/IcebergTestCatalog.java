//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.TestCatalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.ResolvingFileIO;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.*;

public class IcebergTestCatalog implements Catalog, SupportsNamespaces, AutoCloseable {
    private final Map<Namespace, Map<TableIdentifier, Table>> namespaceTableMap;
    private final Map<TableIdentifier, Table> tableMap;

    private final ResolvingFileIO fileIO;

    private IcebergTestCatalog(final String path, @NotNull final Map<String, String> properties) {
        namespaceTableMap = new HashMap<>();
        tableMap = new HashMap<>();
        final Configuration hadoopConf = new Configuration();
        fileIO = new ResolvingFileIO();
        fileIO.setConf(hadoopConf);
        fileIO.initialize(properties);

        // Assume first level is namespace.
        final File root = new File(path);
        for (final File namespaceFile : root.listFiles()) {
            if (namespaceFile.isDirectory()) {
                final Namespace namespace = Namespace.of(namespaceFile.getName());
                namespaceTableMap.putIfAbsent(namespace, new HashMap<>());
                for (final File tableFile : namespaceFile.listFiles()) {
                    if (tableFile.isDirectory()) {
                        // Second level is table name.
                        final TableIdentifier tableId = TableIdentifier.of(namespace, tableFile.getName());
                        final Table table = IcebergTestTable.loadFromMetadata(tableFile.getAbsolutePath(), fileIO);

                        // Add it to the maps.
                        namespaceTableMap.get(namespace).put(tableId, table);
                        tableMap.put(tableId, table);
                    }
                }
            }
        }
    }

    public static IcebergTestCatalog create(final String path, @NotNull final Map<String, String> properties) {
        return new IcebergTestCatalog(path, properties);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        if (namespaceTableMap.containsKey(namespace)) {
            return new ArrayList<>(namespaceTableMap.get(namespace).keySet());
        }
        return List.of();
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean b) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) {
        if (tableMap.containsKey(tableIdentifier)) {
            return tableMap.get(tableIdentifier);
        }
        return null;
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> map) {

    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        return new ArrayList<>(namespaceTableMap.keySet());
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        return Map.of();
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        return false;
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> map) throws NoSuchNamespaceException {
        return false;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> set) throws NoSuchNamespaceException {
        return false;
    }

    @Override
    public void close() {
        fileIO.close();
    }
}
