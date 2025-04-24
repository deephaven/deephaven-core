//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static io.deephaven.iceberg.util.ColumnInstructions.partitionField;
import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static io.deephaven.iceberg.util.ColumnInstructions.schemaFieldName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * Tests for {@link UnboundResolver}.
 */
class UnboundResolverTest {
    private static final Types.IntegerType IT = Types.IntegerType.get();

    private static Schema simpleSchema(org.apache.iceberg.types.Type type) {
        return new Schema(
                Types.NestedField.optional(42, "F1", type),
                Types.NestedField.required(43, "F2", type));
    }

    private static TableDefinition simpleDefinition(Type<?> type) {
        return TableDefinition.of(
                ColumnDefinition.of("F1", type),
                ColumnDefinition.of("F2", type));
    }

    @Test
    void unboundResolverPrimitiveMapping() {
        final Schema schema = simpleSchema(IT);
        final TableDefinition td = simpleDefinition(Type.intType());
        final Resolver actual = UnboundResolver.builder()
                .schema(SchemaProvider.fromSchema(schema))
                .definition(td)
                .build()
                .resolver(null);
        final Resolver expected = Resolver.builder()
                .schema(schema)
                .definition(td)
                .putColumnInstructions("F1", schemaFieldName("F1"))
                .putColumnInstructions("F2", schemaFieldName("F2"))
                .build();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void unboundResolverExtraSchemaColumnsAreIgnored() {
        final Schema schema = simpleSchema(IT); // F1 and F2 exist in Iceberg
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1")); // Only map F1
        final Resolver actual = UnboundResolver.builder()
                .schema(SchemaProvider.fromSchema(schema))
                .definition(td)
                .build()
                .resolver(null);
        final Resolver expected = Resolver.builder()
                .schema(schema)
                .definition(td)
                .putColumnInstructions("F1", schemaFieldName("F1"))
                .build();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void unboundResolverExplicitMappingRenamedColumn() {
        final Schema schema = simpleSchema(IT);
        TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("S1"),
                ColumnDefinition.ofInt("F2"));

        final int f1FieldId = schema.findField("F1").fieldId(); // Map to S1
        final Resolver actual = UnboundResolver.builder()
                .schema(SchemaProvider.fromSchema(schema))
                .definition(td)
                .putColumnInstructions("S1", schemaField(f1FieldId))
                .build()
                .resolver(null);

        final Resolver expected = Resolver.builder()
                .schema(schema)
                .definition(td)
                .putColumnInstructions("S1", schemaField(f1FieldId))
                .putColumnInstructions("F2", schemaFieldName("F2"))
                .build();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void partitionFieldUsedWithoutPartitioningColumn() {
        final Schema schema = simpleSchema(IT);
        TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1"));
        try {
            UnboundResolver.builder()
                    .schema(SchemaProvider.fromSchema(schema))
                    .definition(td)
                    .putColumnInstructions("F1", partitionField(99))
                    .build()
                    .resolver(null);
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining(
                    "Should only specify Iceberg partitionField in combination with a Deephaven partitioning column");
        }
    }

    @Test
    void unboundResolverMissingColumnRejected() {
        final Schema schema = simpleSchema(IT);
        TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("NotInSchema"));
        try {
            UnboundResolver.builder()
                    .schema(SchemaProvider.fromSchema(schema))
                    .definition(td)
                    .build()
                    .resolver(null);
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column NotInSchema");
            assertThat(e).cause().hasMessageContaining("Unable to find field by name: `NotInSchema`");
        }
    }

    @Test
    void schemaFieldIdNotInSchemaRejected() {
        final Schema schema = simpleSchema(IT);
        TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1"));
        try {
            UnboundResolver.builder()
                    .schema(SchemaProvider.fromSchema(schema))
                    .definition(td)
                    .putColumnInstructions("F1", schemaField(9999)) // schema field with ID=9999 not present
                    .build()
                    .resolver(null);
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining("Unable to find field id 9999");
        }
    }

    @Test
    void unknownColumnName() {
        try {
            UnboundResolver.builder()
                    .definition(TableDefinition.of(ColumnDefinition.ofInt("Foo")))
                    .putColumnInstructions("Bar", ColumnInstructions.schemaField(1))
                    .build();
            failBecauseExceptionWasNotThrown(NoSuchColumnException.class);
        } catch (NoSuchColumnException e) {
            assertThat(e).hasMessageContaining("Unknown column names [Bar], available column names are [Foo]");
        }
    }
}
