//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaProviderTest {
    private static Schema newSchema1() {
        return new Schema(Types.NestedField.optional(42, "Foo", Types.IntegerType.get()));
    }

    private static Schema newSchema2() {
        return new Schema(Types.NestedField.optional(43, "Bar", Types.IntegerType.get()));
    }

    @Test
    void fromCurrent() {
        final SchemaProvider.TableSchema actual = SchemaProvider.fromCurrent();
        assertVisitSelf(actual)
                .isEqualTo(SchemaProvider.fromCurrent())
                .isNotEqualTo(SchemaProvider.fromCurrentSnapshot());
    }

    @Test
    void fromSchemaId() {
        final SchemaProvider.SchemaId actual = SchemaProvider.fromSchemaId(42);
        assertThat(actual.schemaId()).isEqualTo(42);
        assertVisitSelf(actual)
                .isEqualTo(SchemaProvider.fromSchemaId(42))
                .isNotEqualTo(SchemaProvider.fromSchemaId(43));
    }

    @Test
    void fromSchema() {
        final Schema actualSchema = newSchema1();
        final SchemaProvider.DirectSchema actual = SchemaProvider.fromSchema(actualSchema);
        assertThat(actual.schema()).isSameAs(actualSchema);
        // Note: important that we are creating a new schema to test equality
        assertVisitSelf(actual)
                .isEqualTo(SchemaProvider.fromSchema(newSchema1()))
                .isNotEqualTo(SchemaProvider.fromSchema(newSchema2()));
    }

    @Test
    void fromSnapshotId() {
        final SchemaProvider.SnapshotId actual = SchemaProvider.fromSnapshotId(43);
        assertThat(actual.snapshotId()).isEqualTo(43);
        assertVisitSelf(actual)
                .isEqualTo(SchemaProvider.fromSnapshotId(43))
                .isNotEqualTo(SchemaProvider.fromSnapshotId(44));
    }

    @Test
    void fromCurrentSnapshot() {
        final SchemaProvider.TableSnapshot actual = SchemaProvider.fromCurrentSnapshot();
        assertVisitSelf(actual)
                .isEqualTo(SchemaProvider.fromCurrentSnapshot())
                .isNotEqualTo(SchemaProvider.fromCurrent());
    }

    static ObjectAssert<SchemaProvider> assertVisitSelf(SchemaProvider provider) {
        return assertThat(ReturnSelf.of(provider));
    }

    // This may seem a bit silly, but it demonstrates that each implementation implements visit correctly

    enum ReturnSelf implements SchemaProvider.Visitor<SchemaProvider> {
        RETURN_SELF;

        public static SchemaProvider of(SchemaProvider provider) {
            return provider.walk(RETURN_SELF);
        }

        @Override
        public SchemaProvider visit(SchemaProvider.TableSchema tableSchema) {
            return tableSchema;
        }

        @Override
        public SchemaProvider visit(SchemaProvider.TableSnapshot tableSnapshot) {
            return tableSnapshot;
        }

        @Override
        public SchemaProvider visit(SchemaProvider.SchemaId schemaId) {
            return schemaId;
        }

        @Override
        public SchemaProvider visit(SchemaProvider.SnapshotId snapshotId) {
            return snapshotId;
        }

        @Override
        public SchemaProvider visit(SchemaProvider.DirectSchema schema) {
            return schema;
        }
    }
}
