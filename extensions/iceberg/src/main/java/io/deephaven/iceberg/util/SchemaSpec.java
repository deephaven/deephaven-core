//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.Iterator;

/**
 * A specification for extracting the schema from a table.
 */
interface SchemaSpec {

    /**
     * Returns the schema for the given table based on this SchemaSpec.
     */
    Schema getSchema(Table table);

    // Static factory methods for creating SchemaSpec instances
    static SchemaSpec initial() {
        return new InitialSchemaSpec();
    }

    static SchemaSpec fromSchemaId(final int id) {
        return new IdSchemaSpec(id);
    }

    static SchemaSpec fromSchema(final Schema schema) {
        return new DirectSchemaSpec(schema);
    }

    static SchemaSpec fromSnapshotId(final int snapshotId) {
        return new SnapshotIdSchemaSpec(snapshotId);
    }

    static SchemaSpec latest() {
        return new LatestSchemaSpec();
    }

    // --------------------------------------------------------------------------------------------------

    // Implementations of SchemaSpec
    class InitialSchemaSpec implements SchemaSpec {
        @Override
        public Schema getSchema(Table table) {
            return getInitialSchema(table);
        }
    }

    class IdSchemaSpec implements SchemaSpec {
        private final int schemaId;

        IdSchemaSpec(final int schemaId) {
            this.schemaId = schemaId;
        }

        @Override
        public Schema getSchema(final Table table) {
            return getSchemaForId(table, schemaId);
        }
    }

    class DirectSchemaSpec implements SchemaSpec {
        private final Schema schema;

        DirectSchemaSpec(final Schema schema) {
            this.schema = schema;
        }

        @Override
        public Schema getSchema(final Table table) {
            return schema;
        }
    }

    class SnapshotIdSchemaSpec implements SchemaSpec {
        private final int snapshotId;

        SnapshotIdSchemaSpec(final int snapshotId) {
            this.snapshotId = snapshotId;
        }

        @Override
        public Schema getSchema(Table table) {
            return getSchemaForSnapshotId(table, snapshotId);
        }
    }

    class LatestSchemaSpec implements SchemaSpec {
        @Override
        public Schema getSchema(Table table) {
            return getLatestSchema(table);
        }
    }

    // --------------------------------------------------------------------------------------------------

    // Methods for extracting the schema from the table
    private static Schema getInitialSchema(final Table table) {
        final Iterable<Snapshot> snapshots = table.snapshots();
        final Iterator<Snapshot> iterator = snapshots.iterator();

        if (iterator.hasNext()) {
            final Snapshot initialSnapshot = iterator.next();
            final Integer initialSchemaId = initialSnapshot.schemaId();
            if (initialSchemaId != null) {
                getSchemaForId(table, initialSchemaId);
            }
        }
        return table.schema();
    }

    private static Schema getSchemaForId(final Table table, final int schemaId) {
        final Schema schema = table.schemas().get(schemaId);
        if (schema == null) {
            throw new IllegalArgumentException("Schema with ID " + schemaId + " not found for table " + table);
        }
        return schema;
    }

    private static Schema getSchemaForSnapshotId(final Table table, final int snapshotId) {
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

    private static Schema getLatestSchema(final Table table) {
        return table.schema();
    }
}
