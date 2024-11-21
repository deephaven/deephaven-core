//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Internal class containing the implementations of {@link SchemaProvider}.
 */
class SchemaProviderInternal {

    interface SchemaProviderImpl {
        /**
         * Returns the schema for the given table based on this {@link SchemaProvider}.
         */
        Schema getSchema(Table table);
    }

    // Implementations of SchemaProvider
    static class CurrentSchemaProvider implements SchemaProvider, SchemaProviderImpl {
        @Override
        public Schema getSchema(final Table table) {
            return getCurrentSchema(table);
        }
    }

    static class IdSchemaProvider implements SchemaProvider, SchemaProviderImpl {
        private final int schemaId;

        IdSchemaProvider(final int schemaId) {
            this.schemaId = schemaId;
        }

        @Override
        public Schema getSchema(final Table table) {
            return getSchemaForId(table, schemaId);
        }
    }

    static class DirectSchemaProvider implements SchemaProvider, SchemaProviderImpl {
        private final Schema schema;

        DirectSchemaProvider(final Schema schema) {
            this.schema = schema;
        }

        @Override
        public Schema getSchema(final Table table) {
            return schema;
        }
    }

    static class SnapshotIdSchemaProvider implements SchemaProvider, SchemaProviderImpl {
        private final int snapshotId;

        SnapshotIdSchemaProvider(final int snapshotId) {
            this.snapshotId = snapshotId;
        }

        @Override
        public Schema getSchema(final Table table) {
            return getSchemaForSnapshotId(table, snapshotId);
        }
    }

    static class CurrentSnapshotSchemaProvider implements SchemaProvider, SchemaProviderImpl {
        @Override
        public Schema getSchema(final Table table) {
            return getSchemaForCurrentSnapshot(table);
        }
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

