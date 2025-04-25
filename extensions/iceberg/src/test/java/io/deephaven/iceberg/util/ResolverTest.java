//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.qst.type.PrimitiveType;
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

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.iceberg.util.ColumnInstructions.partitionField;
import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static io.deephaven.iceberg.util.ColumnInstructions.schemaFieldName;
import static io.deephaven.iceberg.util.ColumnInstructions.unmapped;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * All of these tests should resolve around explicit construction via {@link Resolver#builder()}. Other build pathways
 * should be tested elsewhere, such as {@link ResolverFromTest} or {@link ResolverInferTest}.
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
    void nestedStructByName() {
        final Schema schema = new Schema(NestedField.optional(1, "S1", Types.StructType.of(
                NestedField.optional(2, "S2", Types.StructType.of(
                        NestedField.optional(3, "F1", IT),
                        NestedField.required(4, "F2", IT))))));
        Resolver.builder()
                .schema(schema)
                .definition(TableDefinition.of(
                        ColumnDefinition.ofInt("S1_S2_F1"),
                        ColumnDefinition.ofInt("S1_S2_F2")))
                .putColumnInstructions("S1_S2_F1", schemaFieldName("S1.S2.F1"))
                .putColumnInstructions("S1_S2_F2", schemaFieldName("S1.S2.F2"))
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
    void partitioningColumnSchemaFieldNamePartitionSpec() {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("F1").build();
        final TableDefinition td = TableDefinition.of(
                ColumnDefinition.ofInt("F1").withPartitioning(),
                ColumnDefinition.ofInt("F2"));
        Resolver.builder()
                .schema(schema)
                .spec(spec)
                .definition(td)
                .putColumnInstructions("F1", schemaFieldName("F1"))
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
        try {
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(simpleDefinition(Type.intType()))
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
                    "Incompatible types @ `F1`, icebergType=`int`, type=`io.deephaven.qst.type.StringType`");
        }
    }

    @Test
    void invalidFieldPath() {
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
    void invalidFieldName() {
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .definition(simpleDefinition(Type.intType()))
                    .putColumnInstructions("F1", schemaFieldName("DoesNotExist"))
                    .putColumnInstructions("F2", schemaField(43))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining("Unable to find field by name: `DoesNotExist`");
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
                    "Incompatible types @ `S1.S2`, icebergType=`struct<3: F1: optional int, 4: F2: required int>`, type=`io.deephaven.qst.type.IntType`");
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
            assertThat(e).cause().hasMessageContaining("Transform `bucket[99]` is not supported");
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
    void byteArray() {
        try {
            Resolver.builder()
                    .schema(new Schema(List.of(NestedField.optional(1, "Foo", Types.BinaryType.get()))))
                    .definition(TableDefinition.of(ColumnDefinition.of("Foo", Type.byteType().arrayType())))
                    .putColumnInstructions("Foo", schemaField(1))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
            assertThat(e).cause().hasMessageContaining(
                    "Incompatible types @ `Foo`, icebergType=`binary`, type=`NativeArrayType{clazz=class [B, componentType=io.deephaven.qst.type.ByteType}`");
        }
    }

    // TODO: all of the expected types from ResolverFromTest (or, potential types one might assume to construct
    // manually), we should test out here more explicitly

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
                1, schema,
                List.of(PartitionFieldHack.of(1, 1000, "S1_identity", Transforms.identity())));
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

    @Test
    void listType() {
        final Schema schema = new Schema(NestedField.optional(1, "MyListField", Types.ListType.ofOptional(2, IT)));
        for (final TableDefinition definition : PrimitiveType.instances()
                .map(Type::arrayType)
                .map(x -> ColumnDefinition.of("Foo", x))
                .map(TableDefinition::of)
                .collect(Collectors.toList())) {
            try {
                Resolver.builder()
                        .definition(definition)
                        .schema(schema)
                        .putColumnInstructions("Foo", schemaField(1))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause()
                        .hasMessageContaining("Incompatible types @ `MyListField`, icebergType=`list<int>`");
            }
            try {
                Resolver.builder()
                        .definition(definition)
                        .schema(schema)
                        .putColumnInstructions("Foo", schemaField(2))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "List subpath @ `MyListField` (in `MyListField.element`) is not supported");
            }
        }
    }

    @Test
    void mapType() {
        final Schema schema = new Schema(NestedField.optional(1, "MyMapField", Types.MapType.ofOptional(2, 3, IT, IT)));
        for (final TableDefinition definition : PrimitiveType.instances()
                .map(Type::arrayType)
                .map(x -> ColumnDefinition.of("Foo", x))
                .map(TableDefinition::of)
                .collect(Collectors.toList())) {
            try {
                Resolver.builder()
                        .definition(definition)
                        .schema(schema)
                        .putColumnInstructions("Foo", schemaField(1))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause()
                        .hasMessageContaining("Incompatible types @ `MyMapField`, icebergType=`map<int, int>`");
            }
            try {
                Resolver.builder()
                        .definition(definition)
                        .schema(schema)
                        .putColumnInstructions("Foo", schemaField(2))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause()
                        .hasMessageContaining("Map subpath @ `MyMapField` (in `MyMapField.key`) is not supported");
            }
            try {
                Resolver.builder()
                        .definition(definition)
                        .schema(schema)
                        .putColumnInstructions("Foo", schemaField(3))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause()
                        .hasMessageContaining("Map subpath @ `MyMapField` (in `MyMapField.value`) is not supported");
            }
        }
    }

    @Test
    void stringIdentityPartition() {
        final Schema schema = new Schema(List.of(NestedField.optional(1, "Foo", Types.StringType.get())));
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("Foo").build();
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofString("Foo").withPartitioning());
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(td)
                    .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                    .build();
        }
        // No alt-type
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `string` does not support coercion to io.deephaven.qst.type.IntType");
            }
        }
    }

    @Test
    void booleanIdentityPartition() {
        final Schema schema = new Schema(List.of(NestedField.optional(1, "Foo", Types.BooleanType.get())));
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("Foo").build();
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofBoolean("Foo").withPartitioning());
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(td)
                    .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                    .build();
        }
        // No alt-type
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofString("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `boolean` does not support coercion to io.deephaven.qst.type.StringType");
            }
        }
    }

    @Test
    void intIdentityPartition() {
        final Schema schema = new Schema(List.of(NestedField.optional(1, "Foo", Types.IntegerType.get())));
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("Foo").build();
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt("Foo").withPartitioning());
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(td)
                    .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                    .build();
        }

        // No widening currently
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofLong("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `int` does not support coercion to io.deephaven.qst.type.LongType");
            }
        }

        // No tightening currently
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofShort("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `int` does not support coercion to io.deephaven.qst.type.ShortType");
            }
        }

        // No alt-type
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofString("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `int` does not support coercion to io.deephaven.qst.type.StringType");
            }
        }
    }

    @Test
    void longIdentityPartition() {
        final Schema schema = new Schema(List.of(NestedField.optional(1, "Foo", Types.LongType.get())));
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("Foo").build();
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofLong("Foo").withPartitioning());
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(td)
                    .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                    .build();
        }

        // No tightening currently
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `long` does not support coercion to io.deephaven.qst.type.IntType");
            }
        }

        // No alt-type
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofString("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `long` does not support coercion to io.deephaven.qst.type.StringType");
            }
        }
    }

    @Test
    void floatIdentityPartition() {
        final Schema schema = new Schema(List.of(NestedField.optional(1, "Foo", Types.FloatType.get())));
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("Foo").build();
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofFloat("Foo").withPartitioning());
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(td)
                    .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                    .build();
        }

        // No widening currently
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofDouble("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `float` does not support coercion to io.deephaven.qst.type.DoubleType");
            }
        }

        // No alt-type
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofString("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `float` does not support coercion to io.deephaven.qst.type.StringType");
            }
        }
    }

    @Test
    void doubleIdentityPartition() {
        final Schema schema = new Schema(List.of(NestedField.optional(1, "Foo", Types.DoubleType.get())));
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("Foo").build();
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofDouble("Foo").withPartitioning());
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(td)
                    .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                    .build();
        }

        // No tightening currently
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofFloat("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `double` does not support coercion to io.deephaven.qst.type.FloatType");
            }
        }

        // No alt-type
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofString("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `double` does not support coercion to io.deephaven.qst.type.StringType");
            }
        }
    }

    @Test
    void dateIdentityPartition() {
        final Schema schema = new Schema(List.of(NestedField.optional(1, "Foo", Types.DateType.get())));
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("Foo").build();
        {
            final TableDefinition td =
                    TableDefinition.of(ColumnDefinition.of("Foo", Type.find(LocalDate.class)).withPartitioning());
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(td)
                    .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                    .build();
        }
        // No alt-type
        {
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt("Foo").withPartitioning());
            try {
                Resolver.builder()
                        .schema(schema)
                        .spec(spec)
                        .definition(td)
                        .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                        .build();
                failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
            } catch (Resolver.MappingException e) {
                assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                assertThat(e).cause().hasMessageContaining(
                        "Identity transform of type `date` does not support coercion to io.deephaven.qst.type.IntType");
            }
        }
    }

    @Test
    void unsupportedIdentityTypes() {
        for (org.apache.iceberg.types.Type.PrimitiveType type : List.of(
                Types.TimeType.get(),
                Types.TimestampType.withZone(),
                Types.TimestampType.withoutZone(),
                Types.TimestampNanoType.withZone(),
                Types.TimestampNanoType.withoutZone(),
                Types.DecimalType.of(4, 4),
                Types.BinaryType.get(),
                Types.FixedType.ofLength(13),
                Types.UUIDType.get(),
                Types.UnknownType.get())) {
            final Schema schema = new Schema(List.of(NestedField.optional(1, "Foo", type)));
            final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("Foo").build();
            {
                final TableDefinition td =
                        TableDefinition.of(ColumnDefinition.of("Foo", Type.find(type.getClass())).withPartitioning());
                try {
                    Resolver.builder()
                            .schema(schema)
                            .spec(spec)
                            .definition(td)
                            .putColumnInstructions("Foo", partitionField(spec.fields().get(0).fieldId()))
                            .build();
                    failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
                } catch (Resolver.MappingException e) {
                    assertThat(e).hasMessageContaining("Unable to map Deephaven column Foo");
                    assertThat(e).cause().hasMessageContaining(
                            String.format("Identity transform of type `%s` is not supported", type));
                }
            }
        }
    }
}
