//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Internal class containing the implementations of {@link SchemaProvider}.
 */
class SchemaProviderInternal {

    interface SchemaProviderImpl extends SchemaProvider {
        /**
         * Returns the schema for the given table based on this {@link SchemaProvider}.
         */
        Schema getSchema(Table table);
    }

    // Implementations of SchemaProvider
    enum CurrentSchemaProvider implements SchemaProviderImpl {
        CURRENT_SCHEMA;

        @Override
        public Schema getSchema(final Table table) {
            return getCurrentSchema(table);
        }
    }

    static class IdSchemaProvider implements SchemaProviderImpl {
        private final int schemaId;

        IdSchemaProvider(final int schemaId) {
            this.schemaId = schemaId;
        }

        @Override
        public Schema getSchema(final Table table) {
            return getSchemaForId(table, schemaId);
        }
    }

    static class DirectSchemaProvider implements SchemaProviderImpl {
        private final Schema schema;

        DirectSchemaProvider(final Schema schema) {
            if (schema.schemaId() != 0) {
                // It's unfortunate that org.apache.iceberg.Schema.DEFAULT_SCHEMA_ID overlaps with a real schema id
                throw new IllegalArgumentException(
                        "Direct schemas should not set a schema id; use fromSchemaId instead");
            }
            this.schema = schema;
        }

        @Override
        public Schema getSchema(final Table table) {
            return schema;
        }
    }

    static class SnapshotIdSchemaProvider implements SchemaProviderImpl {
        private final long snapshotId;

        SnapshotIdSchemaProvider(final long snapshotId) {
            this.snapshotId = snapshotId;
        }

        @Override
        public Schema getSchema(final Table table) {
            return getSchemaForSnapshotId(table, snapshotId);
        }
    }

    enum CurrentSnapshotSchemaProvider implements SchemaProviderImpl {
        CURRENT_SNAPSHOT;

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

