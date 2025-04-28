//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;

/**
 * A specification for extracting the schema from a table.
 */
public interface SchemaProvider {

    // Static factory methods for creating SchemaProvider instances

    /**
     * Use the current schema from the table.
     */
    static SchemaProvider fromCurrent() {
        return SchemaProviderInternal.CurrentSchemaProvider.CURRENT_SCHEMA;
    }

    /**
     * Use the schema with the given ID from the table.
     */
    static SchemaProvider fromSchemaId(final int id) {
        return new SchemaProviderInternal.IdSchemaProvider(id);
    }

    /**
     * Use the given schema directly.
     */
    static SchemaProvider fromSchema(final Schema schema) {
        return new SchemaProviderInternal.DirectSchemaProvider(schema);
    }

    /**
     * Use the schema from the snapshot with the given ID.
     */
    static SchemaProvider fromSnapshotId(final long snapshotId) {
        return new SchemaProviderInternal.SnapshotIdSchemaProvider(snapshotId);
    }

    /**
     * Use the schema from the current snapshot of the table.
     */
    static SchemaProvider fromCurrentSnapshot() {
        return SchemaProviderInternal.CurrentSnapshotSchemaProvider.CURRENT_SNAPSHOT;
    }
}
