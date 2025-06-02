//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;

import java.util.Objects;

/**
 * A specification for extracting the schema from a table.
 */
public interface SchemaProvider {

    // Static factory methods for creating SchemaProvider instances

    /**
     * Use the current schema from the table.
     */
    static TableSchema fromCurrent() {
        return TableSchema.TABLE_SCHEMA;
    }

    /**
     * Use the schema with the given ID from the table.
     */
    static SchemaId fromSchemaId(final int id) {
        return new SchemaId(id);
    }

    /**
     * Use the given schema directly.
     */
    static DirectSchema fromSchema(final Schema schema) {
        return new DirectSchema(schema);
    }

    /**
     * Use the schema from the snapshot with the given ID.
     */
    static SnapshotId fromSnapshotId(final long snapshotId) {
        return new SnapshotId(snapshotId);
    }

    /**
     * Use the schema from the current snapshot of the table.
     */
    static TableSnapshot fromCurrentSnapshot() {
        return TableSnapshot.TABLE_SNAPSHOT;
    }

    interface Visitor<T> {
        T visit(TableSchema tableSchema);

        T visit(TableSnapshot tableSnapshot);

        T visit(SchemaId schemaId);

        T visit(SnapshotId snapshotId);

        T visit(DirectSchema schema);
    }

    <T> T walk(Visitor<T> visitor);

    enum TableSchema implements SchemaProvider {
        TABLE_SCHEMA;

        @Override
        public <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    enum TableSnapshot implements SchemaProvider {
        TABLE_SNAPSHOT;

        @Override
        public <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    final class SchemaId implements SchemaProvider {

        private final int schemaId;

        private SchemaId(int schemaId) {
            this.schemaId = schemaId;
        }

        public int schemaId() {
            return schemaId;
        }

        @Override
        public <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SchemaId))
                return false;
            SchemaId schemaId1 = (SchemaId) o;
            return schemaId == schemaId1.schemaId;
        }

        @Override
        public int hashCode() {
            return schemaId;
        }

        @Override
        public String toString() {
            return "SchemaId{" +
                    "schemaId=" + schemaId +
                    '}';
        }
    }

    final class SnapshotId implements SchemaProvider {

        private final long snapshotId;

        private SnapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
        }

        public long snapshotId() {
            return snapshotId;
        }

        @Override
        public <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SnapshotId))
                return false;
            SnapshotId that = (SnapshotId) o;
            return snapshotId == that.snapshotId;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(snapshotId);
        }

        @Override
        public String toString() {
            return "SnapshotId{" +
                    "snapshotId=" + snapshotId +
                    '}';
        }
    }

    final class DirectSchema implements SchemaProvider {
        private final Schema schema;

        private DirectSchema(Schema schema) {
            this.schema = Objects.requireNonNull(schema);
            if (schema.schemaId() != 0) {
                // It's unfortunate that org.apache.iceberg.Schema.DEFAULT_SCHEMA_ID overlaps with a real schema id
                throw new IllegalArgumentException(
                        "Direct schemas should not set a schema id; use fromSchemaId instead");
            }
        }

        public Schema schema() {
            return schema;
        }

        @Override
        public <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof DirectSchema))
                return false;
            DirectSchema that = (DirectSchema) o;
            return schema.sameSchema(that.schema);
        }

        @Override
        public int hashCode() {
            // This is a correct hashcode implementation that may collide when there are schemas with the same struct,
            // but different identifierFieldIds (see implementation of Schema.sameSchema). This is unlikely an issue in
            // practice, and simplifies this layer of code from having to know more implementation details of sameSchema
            // (for example, if sameSchema adds new fields in the future, this code should still be correct).
            return schema.asStruct().hashCode();
        }
    }
}
