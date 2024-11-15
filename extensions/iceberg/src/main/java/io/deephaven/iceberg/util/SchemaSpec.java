//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;

/**
 * A specification for extracting the schema from a table.
 */
public interface SchemaSpec {

    // Static factory methods for creating SchemaSpec instances
    static SchemaSpec current() {
        return new SchemaSpecInternal.CurrentSchemaSpec();
    }

    static SchemaSpec fromSchemaId(final int id) {
        return new SchemaSpecInternal.IdSchemaSpec(id);
    }

    static SchemaSpec fromSchema(final Schema schema) {
        return new SchemaSpecInternal.DirectSchemaSpec(schema);
    }

    static SchemaSpec fromSnapshotId(final int snapshotId) {
        return new SchemaSpecInternal.SnapshotIdSchemaSpec(snapshotId);
    }

    static SchemaSpec fromCurrentSnapshot() {
        return new SchemaSpecInternal.CurrentSnapshotSchemaSpec();
    }
}
