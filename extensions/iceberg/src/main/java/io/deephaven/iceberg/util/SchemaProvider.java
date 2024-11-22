//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;

/**
 * A specification for extracting the schema from a table.
 */
public interface SchemaProvider {

    // Static factory methods for creating SchemaProvider instances
    static SchemaProvider fromCurrent() {
        return new SchemaProviderInternal.CurrentSchemaProvider();
    }

    static SchemaProvider fromSchemaId(final int id) {
        return new SchemaProviderInternal.IdSchemaProvider(id);
    }

    static SchemaProvider fromSchema(final Schema schema) {
        return new SchemaProviderInternal.DirectSchemaProvider(schema);
    }

    static SchemaProvider fromSnapshotId(final int snapshotId) {
        return new SchemaProviderInternal.SnapshotIdSchemaProvider(snapshotId);
    }

    static SchemaProvider fromCurrentSnapshot() {
        return new SchemaProviderInternal.CurrentSnapshotSchemaProvider();
    }
}
