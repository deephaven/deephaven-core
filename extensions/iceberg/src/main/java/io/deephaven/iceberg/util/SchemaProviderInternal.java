//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.Objects;

/**
 * Internal class to extract {@link Schema} from a {@link Table}.
 */
final class SchemaProviderInternal implements SchemaProvider.Visitor<Schema> {

    public static Schema of(SchemaProvider provider, Table table) {
        return provider.walk(new SchemaProviderInternal(table));
    }

    private final Table table;

    SchemaProviderInternal(Table table) {
        this.table = Objects.requireNonNull(table);
    }

    @Override
    public Schema visit(SchemaProvider.TableSchema tableSchema) {
        return getCurrentSchema(table);
    }

    @Override
    public Schema visit(SchemaProvider.TableSnapshot tableSnapshot) {
        return getSchemaForCurrentSnapshot(table);
    }

    @Override
    public Schema visit(SchemaProvider.SchemaId schemaId) {
        return getSchemaForId(table, schemaId.schemaId());
    }

    @Override
    public Schema visit(SchemaProvider.SnapshotId snapshotId) {
        return getSchemaForSnapshotId(table, snapshotId.snapshotId());
    }

    @Override
    public Schema visit(SchemaProvider.DirectSchema schema) {
        return schema.schema();
    }

    // --------------------------------------------------------------------------------------------------

    // Methods for extracting the schema from the table
    private static Schema getCurrentSchema(final Table table) {
        return table.schema();
    }

    private static Schema getSchemaForId(final Table table, final int schemaId) {
        final Schema schema = table.schemas().get(schemaId);
        if (schema == null) {
            throw new IllegalArgumentException("Schema with ID " + schemaId + " not found for table " + table);
        }
        return schema;
    }

    private static Schema getSchemaForSnapshotId(final Table table, final long snapshotId) {
        final Snapshot snapshot = table.snapshot(snapshotId);
        if (snapshot == null) {
            throw new IllegalArgumentException("Snapshot with ID " + snapshotId + " not found for table " +
                    table);
        }
        final Integer schemaId = snapshot.schemaId();
        if (schemaId == null) {
            throw new IllegalArgumentException("Snapshot with ID " + snapshotId + " does not have a schema ID");
        }
        return getSchemaForId(table, schemaId);
    }

    private static Schema getSchemaForCurrentSnapshot(final Table table) {
        final Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            throw new IllegalArgumentException("Table " + table + " does not have a current snapshot");
        }
        final Integer schemaId = currentSnapshot.schemaId();
        if (schemaId == null) {
            throw new IllegalArgumentException("Current snapshot with ID " + currentSnapshot.snapshotId() +
                    " for table " + table + " does not have a schema ID");
        }
        return getSchemaForId(table, schemaId);
    }
}

