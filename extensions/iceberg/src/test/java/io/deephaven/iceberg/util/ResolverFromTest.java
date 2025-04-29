//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionFieldHack;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecHack;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.deephaven.iceberg.util.ColumnInstructions.partitionField;
import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;


/**
 * This test specifics around {@link Resolver#from(TableDefinition)}; more general validations around {@link Resolver}
 * should be in {@link ResolverTest}.
 */
class ResolverFromTest {

    public static boolean equalsModuloSchemaId(Resolver resolver, Resolver other) {
        // Schema does not implement equals; this is _ok_ when we are doing tests that have an existing Schema, but when
        // we are building one ourselves, we need to use Schema#sameSchema.
        return resolver.definition().equals(other.definition())
                && resolver.schema().sameSchema(other.schema())
                && resolver.spec().equals(other.spec())
                && resolver.columnInstructions().equals(other.columnInstructions());
    }

    private static ObjectAssert<Resolver> assertResolverFrom(TableDefinition definition) {
        return assertThat(Resolver.from(definition)).usingEquals(ResolverFromTest::equalsModuloSchemaId);
    }

    // todo: fill this out more generally

    @Test
    void shortType() {
        try {
            Resolver.from(TableDefinition.of(ColumnDefinition.of("Foo", Type.shortType())));
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining(
                    "Unable to infer the best Iceberg type for Deephaven column type `io.deephaven.qst.type.ShortType`");
        }
    }

    @Test
    void intType() {
        final TableDefinition definition = TableDefinition.of(ColumnDefinition.of("Foo", Type.intType()));
        final Resolver expected = Resolver.builder()
                .definition(definition)
                .schema(new Schema(List.of(Types.NestedField.optional(1, "Foo", Types.IntegerType.get()))))
                .putColumnInstructions("Foo", schemaField(1))
                .build();
        assertResolverFrom(definition).isEqualTo(expected);
    }

    @Test
    void byteArrayType() {
        try {
            Resolver.from(TableDefinition.of(ColumnDefinition.of("Foo", Type.byteType().arrayType())));
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining(
                    "Unable to infer the best Iceberg type for Deephaven column type `NativeArrayType{clazz=class [B, componentType=io.deephaven.qst.type.ByteType}`");
        }
    }

    @Test
    void shortArrayType() {
        try {
            Resolver.from(TableDefinition.of(ColumnDefinition.of("Foo", Type.shortType().arrayType())));
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining(
                    "Unable to infer the best Iceberg type for Deephaven column type `NativeArrayType{clazz=class [S, componentType=io.deephaven.qst.type.ShortType}`");
        }
    }

    @Test
    void intArrayType() {
        try {
            Resolver.from(TableDefinition.of(ColumnDefinition.of("Foo", Type.intType().arrayType())));
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining(
                    "Unable to infer the best Iceberg type for Deephaven column type `NativeArrayType{clazz=class [I, componentType=io.deephaven.qst.type.IntType}`");
        }
    }

    @Test
    void refreshIds() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("Foo"),
                ColumnDefinition.ofDouble("Bar"),
                ColumnDefinition.ofString("Baz"));

        final Resolver internalResolver;
        {
            final Schema internalSchema = new Schema(
                    Types.NestedField.optional(1, "Foo", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "Bar", Types.DoubleType.get()),
                    Types.NestedField.optional(3, "Baz", Types.StringType.get()));

            internalResolver = Resolver.builder()
                    .definition(definition)
                    .schema(internalSchema)
                    .putColumnInstructions("Foo", schemaField(1))
                    .putColumnInstructions("Bar", schemaField(2))
                    .putColumnInstructions("Baz", schemaField(3))
                    .build();
        }

        final Schema freshSchema = new Schema(
                Types.NestedField.optional(2, "Foo", Types.IntegerType.get()),
                Types.NestedField.optional(1, "Bar", Types.DoubleType.get()),
                Types.NestedField.optional(42, "Baz", Types.StringType.get()));

        final Resolver expected = Resolver.builder()
                .definition(definition)
                .schema(freshSchema)
                .putColumnInstructions("Foo", schemaField(2))
                .putColumnInstructions("Bar", schemaField(1))
                .putColumnInstructions("Baz", schemaField(42))
                .build();

        assertThat(Resolver.refreshIds(internalResolver, freshSchema, PartitionSpec.unpartitioned()))
                .isEqualTo(expected);
    }

    @Test
    void refreshIdsPartitionSpec() {
        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("Foo").withPartitioning(),
                ColumnDefinition.ofDouble("Bar").withPartitioning(),
                ColumnDefinition.ofString("Baz"));

        final Resolver internalResolver;
        final String fooSpecName;
        final String barSpecName;
        {
            final Schema internalSchema = new Schema(
                    Types.NestedField.optional(1, "Foo", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "Bar", Types.DoubleType.get()),
                    Types.NestedField.optional(3, "Baz", Types.StringType.get()));

            final PartitionSpec internalSpec = PartitionSpec.builderFor(internalSchema)
                    .identity("Foo")
                    .identity("Bar")
                    .build();

            internalResolver = Resolver.builder()
                    .definition(definition)
                    .schema(internalSchema)
                    .spec(internalSpec)
                    .putColumnInstructions("Foo", partitionField(internalSpec.fields().get(0).fieldId()))
                    .putColumnInstructions("Bar", partitionField(internalSpec.fields().get(1).fieldId()))
                    .putColumnInstructions("Baz", schemaField(3))
                    .build();
            // The current impl simply uses the same names "Foo" and "Bar" for the spec names, but we shouldn't need to
            // rely on that for this test.
            fooSpecName = internalSpec.fields().get(0).name();
            barSpecName = internalSpec.fields().get(1).name();
        }

        final Schema freshSchema = new Schema(
                Types.NestedField.optional(42, "Foo", Types.IntegerType.get()),
                Types.NestedField.optional(43, "Bar", Types.DoubleType.get()),
                Types.NestedField.optional(44, "Baz", Types.StringType.get()));

        final PartitionSpec freshSpec = PartitionSpecHack.newPartitionSpec(1, freshSchema, List.of(
                PartitionFieldHack.of(42, 1111, fooSpecName, Transforms.identity()),
                PartitionFieldHack.of(43, 1112, barSpecName, Transforms.identity())));

        final Resolver expected = Resolver.builder()
                .definition(definition)
                .schema(freshSchema)
                .spec(freshSpec)
                .putColumnInstructions("Foo", partitionField(1111))
                .putColumnInstructions("Bar", partitionField(1112))
                .putColumnInstructions("Baz", schemaField(44))
                .build();

        assertThat(Resolver.refreshIds(internalResolver, freshSchema, freshSpec)).isEqualTo(expected);
    }
}
