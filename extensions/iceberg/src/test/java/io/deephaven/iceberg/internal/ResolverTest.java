//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionFieldHack;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecHack;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.deephaven.iceberg.util.ColumnInstructions.partitionField;
import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static io.deephaven.iceberg.util.ColumnInstructions.unmapped;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * testing mapping is tougher than testing inference because it needs to deal with type conversion logic
 */
class ResolverTest {

    private static final IntegerType IT = IntegerType.get();

    @Test
    void normal() {
        Resolver.builder()
                .schema(simpleSchema(IT))
                .definition(simpleDefinition(Type.intType()))
                .putColumnInstructions("F1", schemaField(42))
                .putColumnInstructions("F2", schemaField(43))
                .build();
    }

    @Test
    void partitionSpec() {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("F1")
                .build();
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1").withPartitioning(),
                ColumnDefinition.ofInt("F2"));
        Resolver.builder()
                .schema(schema)
                .spec(spec)
                .definition(td)
                .putColumnInstructions("F1", partitionField(spec.fields().get(0).fieldId()))
                .putColumnInstructions("F2", schemaField(43))
                .build();
    }

    @Test
    void duplicateMapping() {
        // It's okay to map the same Iceberg field to different DH columns
        Resolver.builder()
                .schema(simpleSchema(IT))
                .definition(TableDefinition.of(
                        ColumnDefinition.of("F1", Type.intType()),
                        ColumnDefinition.of("F2", Type.intType()),
                        ColumnDefinition.of("F3", Type.intType())))
                .putColumnInstructions("F1", schemaField(42))
                .putColumnInstructions("F2", schemaField(43))
                .putColumnInstructions("F3", schemaField(43))
                .build();

    }

    @Test
    void unmappedIcebergField() {
        // It's okay to not map all the Iceberg fields
        Resolver.builder()
                .schema(new Schema(
                        NestedField.optional(42, "F1", IT),
                        NestedField.required(43, "F2", IT),
                        NestedField.required(44, "F3", IT)))
                .definition(simpleDefinition(Type.intType()))
                .putColumnInstructions("F1", schemaField(42))
                .putColumnInstructions("F2", schemaField(43))
                .build();
    }

    @Test
    void nestedStruct() {
        final Schema schema = new Schema(NestedField.optional(1, "S1", Types.StructType.of(
                NestedField.optional(2, "S2", Types.StructType.of(
                        NestedField.optional(3, "F1", IT),
                        NestedField.required(4, "F2", IT))))));
        Resolver.builder()
                .schema(schema)
                .definition(TableDefinition.of(
                        ColumnDefinition.ofInt("S1_S2_F1"),
                        ColumnDefinition.ofInt("S1_S2_F2")))
                .putColumnInstructions("S1_S2_F1", schemaField(3))
                .putColumnInstructions("S1_S2_F2", schemaField(4))
                .build();
    }

    @Test
    void unmappedColumn() {
        // All DH columns must be mapped by default.
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .definition(simpleDefinition(Type.intType()))
                    .putColumnInstructions("F1", schemaField(42))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Column `F2` is not mapped");
        }
        // But, there is support to allow them, which is necessary for cases where a Schema field has been deleted, but
        // we want to keep the column in DH
        Resolver.builder()
                .schema(simpleSchema(IT))
                .definition(simpleDefinition(Type.intType()))
                .putColumnInstructions("F1", schemaField(42))
                .putColumnInstructions("F2", unmapped())
                .build();
    }

    @Test
    void partitioningColumnSchemaFieldNoPartitionSpec() {
        final Schema schema = simpleSchema(IT);
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1").withPartitioning(),
                ColumnDefinition.ofInt("F2"));
        try {
            Resolver.builder()
                    .schema(schema)
                    .definition(td)
                    .putColumnInstructions("F1", schemaField(42))
                    .putColumnInstructions("F2", schemaField(43))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause()
                    .hasMessageContaining("No PartitionField with source field id 42 exists in PartitionSpec []");
        }
    }

    @Test
    void partitioningColumnSchemaFieldPartitionSpec() {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("F1").build();
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1").withPartitioning(),
                ColumnDefinition.ofInt("F2"));
        Resolver.builder()
                .schema(schema)
                .spec(spec)
                .definition(td)
                .putColumnInstructions("F1", schemaField(42))
                .putColumnInstructions("F2", schemaField(43))
                .build();
    }

    @Test
    void partitioningColumnPartitionFieldPartitionSpec() {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("F1").build();
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1").withPartitioning(),
                ColumnDefinition.ofInt("F2"));
        Resolver.builder()
                .schema(schema)
                .spec(spec)
                .definition(td)
                .putColumnInstructions("F1", partitionField(spec.fields().get(0).fieldId()))
                .putColumnInstructions("F2", schemaField(43))
                .build();
    }

    @Test
    void normalColumnSchemaFieldPartitionSpec() {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("F1").build();
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1"),
                ColumnDefinition.ofInt("F2"));
        Resolver.builder()
                .schema(schema)
                .spec(spec)
                .definition(td)
                .putColumnInstructions("F1", schemaField(42))
                .putColumnInstructions("F2", schemaField(43))
                .build();
    }

    @Test
    void normalColumnPartitionFieldPartitionSpec() {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("F1").build();
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1"),
                ColumnDefinition.ofInt("F2"));
        try {
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(td)
                    .putColumnInstructions("F1", partitionField(spec.fields().get(0).fieldId()))
                    .putColumnInstructions("F2", schemaField(43))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining(
                    "Should only specify Iceberg partitionField in combination with a Deephaven partitioning column");
        }
    }

    @Test
    void noSuchColumn() {
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .definition(simpleDefinition(Type.intType()))
                    .putColumnInstructions("F1", schemaField(42))
                    .putColumnInstructions("F2", schemaField(43))
                    .putColumnInstructions("F3", schemaField(42))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (NoSuchColumnException e) {
            assertThat(e).hasMessageContaining("Unknown column names [F3], available column names are [F1, F2]");
        }
    }

    @Test
    void noSuchPath() {
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .definition(simpleDefinition(Type.intType()))
                    .putColumnInstructions("F1", schemaField(42))
                    .putColumnInstructions("F2", schemaField(44))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F2");
            assertThat(e).cause().hasMessageContaining("Unable to find field id 44");
        }
    }

    @Test
    void noSuchPartitionField() {
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1"),
                ColumnDefinition.ofInt("F2").withPartitioning());
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .definition(td)
                    .putColumnInstructions("F1", schemaField(42))
                    .putColumnInstructions("F2", partitionField(9999))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F2");
            assertThat(e).cause().hasMessageContaining("Unable to find partition field id 9999");
        }
    }


    @Test
    void invalidMappingType() {
        // TODO: we should try to be thorough in describing what we do and do not support
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .definition(simpleDefinition(Type.stringType()))
                    .putColumnInstructions("F1", schemaField(42))
                    .putColumnInstructions("F2", schemaField(43))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining(
                    "Unable to map Iceberg type `int` to Deephaven type `io.deephaven.qst.type.StringType`");
        }
    }

    @Test
    void invalidFieldPath() {
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .definition(TableDefinition.of(
                            ColumnDefinition.of("F1", Type.intType()),
                            ColumnDefinition.of("F2", Type.intType())))
                    .putColumnInstructions("F1", schemaField(42))
                    .putColumnInstructions("F2", schemaField(44))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F2");
            assertThat(e).cause().hasMessageContaining("Unable to find field id 44");
        }
    }

    @Test
    void fieldPathToStruct() {
        final Schema schema = new Schema(NestedField.optional(1, "S1", Types.StructType.of(
                NestedField.optional(2, "S2", Types.StructType.of(
                        NestedField.optional(3, "F1", IT),
                        NestedField.required(4, "F2", IT))))));
        try {
            Resolver.builder()
                    .schema(schema)
                    .definition(TableDefinition.of(ColumnDefinition.ofInt("S1_S2")))
                    .putColumnInstructions("S1_S2", schemaField(2))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column S1_S2");
            assertThat(e).cause().hasMessageContaining(
                    "Only support mapping to primitive types, field=[2: S2: optional struct<3: F1: optional int, 4: F2: required int>]");
        }
    }

    @Test
    void badPartitionFieldId() {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("F1")
                .build();
        try {
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(TableDefinition.of(
                            ColumnDefinition.of("F1", Type.intType()).withPartitioning(),
                            ColumnDefinition.of("F2", Type.intType())))
                    .putColumnInstructions("F1", partitionField(9999))
                    .putColumnInstructions("F2", schemaField(43))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining("Unable to find partition field id 9999");
        }
    }

    @Test
    void badPartitionType() {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema)
                .bucket("F1", 99)
                .build();
        try {
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(TableDefinition.of(
                            ColumnDefinition.of("F1", Type.intType()).withPartitioning(),
                            ColumnDefinition.of("F2", Type.intType())))
                    .putColumnInstructions("F1", partitionField(spec.fields().get(0).fieldId()))
                    .putColumnInstructions("F2", schemaField(43))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining(
                    "Unable to map partition field `1000: F1_bucket: bucket[99](42)`, only identity transform is supported");
        }
    }

    @Test
    void specThatIsNotCompatibleWithSchemaIsOk() {
        final Schema s1 = new Schema(NestedField.optional(42, "F1", IT));
        final Schema s2 = simpleSchema(IT);
        final PartitionSpec spec2 = PartitionSpec.builderFor(s2)
                .identity("F1")
                .identity("F2")
                .build();
        final int partitionFieldId1 = spec2.fields().get(0).fieldId();
        final int partitionFieldId2 = spec2.fields().get(1).fieldId();

        // This is mainly "proof" that spec2 is not directly compatible with s1
        try {
            spec2.toUnbound().bind(s1);
            failBecauseExceptionWasNotThrown(ValidationException.class);
        } catch (ValidationException e) {
            assertThat(e).hasMessageContaining("Cannot find source column for partition field: 1001: F2: identity(43)");
        }

        // But, it's still OK to build a resolver with it
        Resolver.builder()
                .schema(s1)
                .spec(spec2)
                .definition(TableDefinition.of(
                        ColumnDefinition.of("F1", Type.intType()).withPartitioning()))
                .putColumnInstructions("F1", partitionField(partitionFieldId1))
                .build();

        // Of course, we can't actually _reference_ the partition fields that aren't in the schema, as they will fail to
        // resolve
        try {
            Resolver.builder()
                    .schema(s1)
                    .spec(spec2)
                    .definition(TableDefinition.of(
                            ColumnDefinition.of("F1", Type.intType()).withPartitioning()))
                    .putColumnInstructions("F1", partitionField(partitionFieldId2))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining(
                    "Unable to find partition field `1001: F2: identity(43)` in schema");
        }

    }

    private static Schema simpleSchema(org.apache.iceberg.types.Type type) {
        return new Schema(
                NestedField.optional(42, "F1", type),
                NestedField.required(43, "F2", type));
    }

    private static TableDefinition simpleDefinition(Type<?> type) {
        return TableDefinition.of(
                ColumnDefinition.of("F1", type),
                ColumnDefinition.of("F2", type));
    }

    @Test
    void simplePrimitiveMapping() {
        final Schema schema = simpleSchema(IT);
        final TableDefinition td = simpleDefinition(Type.intType());
        final Resolver expected = Resolver.builder()
                .schema(schema)
                .definition(td)
                .putColumnInstructions("F1", schemaField(42))
                .putColumnInstructions("F2", schemaField(43))
                .build();
        assertThat(Resolver.simple(schema, td)).isEqualTo(expected);
    }

    @Test
    void simpleExtraSchemaColumnsAreIgnored() {
        final Schema schema = simpleSchema(IT); // F1 and F2 exist in Iceberg
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1")); // Only map F1
        final Resolver expected = Resolver.builder()
                .schema(schema)
                .definition(td)
                .putColumnInstructions("F1", schemaField(42))
                .build();
        assertThat(Resolver.simple(schema, td)).isEqualTo(expected);
    }

    @Test
    void simplePartitioningColumnRejected() {
        final Schema schema = simpleSchema(IT);
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1").withPartitioning()); // should not be allowed
        assertThatThrownBy(() -> Resolver.simple(schema, td))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partitioning column");
    }

    @Test
    void simpleMissingColumnRejected() {
        final Schema schema = simpleSchema(IT);
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("NotInSchema"));
        assertThatThrownBy(() -> Resolver.simple(schema, td))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found in Iceberg schema");
    }

    @Test
    void partitionFieldAgainstNonPrimitiveType() {
        final Schema schema = new Schema(NestedField.optional(1, "S1", Types.StructType.of(
                NestedField.optional(2, "F1", IT),
                NestedField.required(3, "F2", IT))));

        // https://iceberg.apache.org/spec/#partitioning
        // > The source columns, selected by ids, must be a primitive type
        // Normally, Iceberg itself will not allow this sort of type to be constructed
        try {
            PartitionSpec.builderFor(schema).identity("S1").build();
            failBecauseExceptionWasNotThrown(ValidationException.class);
        } catch (ValidationException e) {
            assertThat(e).hasMessageContaining(
                    "Cannot partition by non-primitive source field: struct<2: F1: optional int, 3: F2: required int>");
        }

        // Of course, if there was a mis-implemented Catalog, it's possible that things get deserialized without proper
        // checks
        final PartitionSpec manualSpec = PartitionSpecHack.newPartitionSpecUnchecked(
                schema,
                List.of(PartitionFieldHack.of(1, 1000, "S1_identity", Transforms.fromString("identity"))),
                1);
        try {
            Resolver.builder()
                    .schema(schema)
                    .spec(manualSpec)
                    .definition(TableDefinition.of(
                            ColumnDefinition.ofInt("S1").withPartitioning(),
                            ColumnDefinition.ofInt("S1_F2")))
                    .putColumnInstructions("S1", schemaField(1))
                    .putColumnInstructions("S1_F2", schemaField(3))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column S1");
            assertThat(e).cause().hasMessageContaining(
                    "Cannot partition by non-primitive source field: struct<2: F1: optional int, 3: F2: required int>");
        }
    }

    @Test
    void partitionFieldContainedWithinList() {
        final Schema schema = new Schema(NestedField.optional(1, "S1", Types.ListType.ofOptional(2, IT)));
        // https://iceberg.apache.org/spec/#partitioning
        // > The source columns ... cannot be contained in a list
        // Iceberg does *not* currently guard against this (may open up a PR later for this)
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("S1.element").build();
        try {
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(TableDefinition.of(
                            ColumnDefinition.ofInt("S1").withPartitioning()))
                    .putColumnInstructions("S1", partitionField(spec.fields().get(0).fieldId()))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column S1");
            assertThat(e).cause().hasMessageContaining("Partition fields may not be contained in a list");
        }
    }

    @Test
    void partitionFieldContainedWithinMap() {
        final Schema schema = new Schema(NestedField.optional(1, "S1", Types.MapType.ofOptional(2, 3, IT, IT)));
        // https://iceberg.apache.org/spec/#partitioning
        // > The source columns ... cannot be contained in a map
        // Iceberg does *not* currently guard against this (may open up a PR later for this)
        for (final PartitionSpec spec : List.of(
                PartitionSpec.builderFor(schema).identity("S1.key").build(),
                PartitionSpec.builderFor(schema).identity("S1.value").build())) {
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(TableDefinition.of(
                                ColumnDefinition.ofInt("S1").withPartitioning()))
                        .putColumnInstructions("S1", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column S1");
                assertThat(e).cause().hasMessageContaining("Partition fields may not be contained in a map");
            }
        }
    }
}
