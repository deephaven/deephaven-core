package io.deephaven.iceberg.TestCatalog;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class IcebergTestCatalog implements Catalog {
    private Map<TableIdentifier, IcebergTestTable> tables;

    private IcebergTestCatalog() {
        this.tables = new LinkedHashMap<>();
    }

    public static IcebergTestCatalog create() {
        return new IcebergTestCatalog();
    }

    public void addTable(final TableIdentifier tableIdentifier, final IcebergTestTable table) {
        tables.put(tableIdentifier, table);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return List.of();
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean b) {
        final Table t = tables.remove(tableIdentifier);
        return t != null;
    }

    @Override
    public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {
        final IcebergTestTable t = tables.remove(tableIdentifier);
        if (t != null) {
            tables.put(tableIdentifier1, t);
        }
    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) {
        return tables.get(tableIdentifier);
    }
}
