//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import org.apache.iceberg.PartitionFieldHack;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecHack;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static io.deephaven.iceberg.util.ColumnInstructions.partitionField;
import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test specifics around {@link Resolver#from(TableDefinition)}; more general validations around {@link Resolver}
 * should be in {@link ResolverTest}.
 */
class ResolverFromTest {

    private static final String COL = "Foo";

    private static boolean equalsModuloSchemaId(Resolver resolver, Resolver other) {
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

    private static void assertType(
            final Type<?> dhType,
            final org.apache.iceberg.types.Type icebergType) {
        final TableDefinition definition = TableDefinition.of(ColumnDefinition.of(COL, dhType));
        final Resolver expected = Resolver.builder()
                .definition(definition)
                .schema(new Schema(
                        Types.NestedField.optional(1, COL, icebergType)))
                .putColumnInstructions(COL, schemaField(1))
                .build();
        assertResolverFrom(definition).isEqualTo(expected);
    }

    private static void assertArrayType(
            final ArrayType<?, ?> dhArrayType,
            final Type<?> expectedDefinitionType,
            final org.apache.iceberg.types.Type icebergListElementType) {
        Resolver.from(TableDefinition.of(ColumnDefinition.of(COL, dhArrayType)));
        final TableDefinition definition = TableDefinition.of(ColumnDefinition.of(COL, expectedDefinitionType));
        final Resolver expected = Resolver.builder()
                .definition(definition)
                .schema(new Schema(
                        Types.NestedField.optional(2, COL,
                                Types.ListType.ofOptional(1, icebergListElementType))))
                .putColumnInstructions(COL, schemaField(2))
                .build();
        assertResolverFrom(definition).isEqualTo(expected);
    }

    private static void assertVectorType(
            final ArrayType<?, ?> dhVectorType,
            final org.apache.iceberg.types.Type icebergListElementType) {
        assertArrayType(dhVectorType, dhVectorType, icebergListElementType);
    }

    @Test
    void booleanType() {
        assertType(Type.find(Boolean.class), Types.BooleanType.get());
    }

    @Test
    void byteType() {
        assertType(Type.byteType(), Types.IntegerType.get());
    }

    @Test
    void shortType() {
        assertType(Type.shortType(), Types.IntegerType.get());
    }

    @Test
    void intType() {
        assertType(Type.intType(), Types.IntegerType.get());
    }

    @Test
    void longType() {
        assertType(Type.longType(), Types.LongType.get());
    }

    @Test
    void floatType() {
        assertType(Type.floatType(), Types.FloatType.get());
    }

    @Test
    void doubleType() {
        assertType(Type.doubleType(), Types.DoubleType.get());
    }

    @Test
    void stringType() {
        assertType(Type.stringType(), Types.StringType.get());
    }

    @Test
    void instantType() {
        assertType(Type.instantType(), Types.TimestampType.withZone());
    }

    @Test
    void localDateTimeType() {
        assertType(Type.find(LocalDateTime.class), Types.TimestampType.withoutZone());
    }

    @Test
    void localDateType() {
        assertType(Type.find(LocalDate.class), Types.DateType.get());
    }

    @Test
    void localTimeType() {
        assertType(Type.find(LocalTime.class), Types.TimeType.get());
    }

    @Test
    void bigDecimalType() {
        assertThatThrownBy(
                () -> Resolver.from(TableDefinition.of(ColumnDefinition.of(COL, Type.find(BigDecimal.class)))))
                .isInstanceOf(Resolver.MappingException.class);
    }

    /** Array types **/

    @Test
    void booleanArrayType() {
        assertArrayType(Type.find(Boolean.class).arrayType(),
                ObjectVector.type(Type.booleanType().boxedType()), Types.BooleanType.get());
    }

    @Test
    void byteArrayType() {
        assertArrayType(Type.byteType().arrayType(), IntVector.type(), Types.IntegerType.get());
    }

    @Test
    void shortArrayType() {
        assertArrayType(Type.shortType().arrayType(), IntVector.type(), Types.IntegerType.get());
    }

    @Test
    void intArrayType() {
        assertArrayType(Type.intType().arrayType(), IntVector.type(), Types.IntegerType.get());
    }

    @Test
    void longArrayType() {
        assertArrayType(Type.longType().arrayType(), LongVector.type(), Types.LongType.get());
    }

    @Test
    void floatArrayType() {
        assertArrayType(Type.floatType().arrayType(), FloatVector.type(), Types.FloatType.get());
    }

    @Test
    void doubleArrayType() {
        assertArrayType(Type.doubleType().arrayType(), DoubleVector.type(), Types.DoubleType.get());
    }

    @Test
    void stringArrayType() {
        assertArrayType(
                Type.stringType().arrayType(),
                ObjectVector.type(Type.stringType()),
                Types.StringType.get());
    }

    @Test
    void instantArrayType() {
        assertArrayType(
                Type.instantType().arrayType(),
                ObjectVector.type(Type.instantType()),
                Types.TimestampType.withZone());
    }

    @Test
    void localDateTimeArray() {
        assertArrayType(
                Type.find(LocalDateTime.class).arrayType(),
                ObjectVector.type((GenericType<?>) Type.find(LocalDateTime.class)),
                Types.TimestampType.withoutZone());
    }

    @Test
    void localDateArrayType() {
        assertArrayType(
                Type.find(LocalDate.class).arrayType(),
                ObjectVector.type((GenericType<?>) Type.find(LocalDate.class)),
                Types.DateType.get());
    }

    @Test
    void localTimeArrayType() {
        assertArrayType(
                Type.find(LocalTime.class).arrayType(),
                ObjectVector.type((GenericType<?>) Type.find(LocalTime.class)),
                Types.TimeType.get());
    }

    @Test
    void bigDecimalArrayType() {
        assertThatThrownBy(() -> Resolver
                .from(TableDefinition.of(ColumnDefinition.of(COL, Type.find(BigDecimal.class).arrayType()))))
                .isInstanceOf(Resolver.MappingException.class);
    }

    /** Vector types **/

    @Test
    void booleanVectorType() {
        assertVectorType(ObjectVector.type(Type.booleanType().boxedType()),
                Types.BooleanType.get());
    }

    @Test
    void byteVectorType() {
        assertVectorType(ByteVector.type(), Types.IntegerType.get());
    }

    @Test
    void byteObjectVectorType() {
        assertVectorType(ObjectVector.type(Type.byteType().boxedType()), Types.IntegerType.get());
    }

    @Test
    void shortVectorType() {
        assertVectorType(ShortVector.type(), Types.IntegerType.get());
    }

    @Test
    void shortObjectVectorType() {
        assertVectorType(ObjectVector.type(Type.shortType().boxedType()), Types.IntegerType.get());
    }

    @Test
    void intVectorType() {
        assertVectorType(IntVector.type(), Types.IntegerType.get());
    }

    @Test
    void intObjectVectorType() {
        assertVectorType(ObjectVector.type(Type.intType().boxedType()), Types.IntegerType.get());
    }

    @Test
    void longVectorType() {
        assertVectorType(LongVector.type(), Types.LongType.get());
    }

    @Test
    void longObjectVectorType() {
        assertVectorType(ObjectVector.type(Type.longType().boxedType()), Types.LongType.get());
    }

    @Test
    void floatVectorType() {
        assertVectorType(FloatVector.type(), Types.FloatType.get());
    }

    @Test
    void floatObjectVectorType() {
        assertVectorType(ObjectVector.type(Type.floatType().boxedType()), Types.FloatType.get());
    }

    @Test
    void doubleVectorType() {
        assertVectorType(DoubleVector.type(), Types.DoubleType.get());
    }

    @Test
    void doubleObjectVectorType() {
        assertVectorType(ObjectVector.type(Type.doubleType().boxedType()), Types.DoubleType.get());
    }

    @Test
    void stringVectorType() {
        assertVectorType(ObjectVector.type(Type.stringType()), Types.StringType.get());
    }

    @Test
    void instantVectorType() {
        assertVectorType(ObjectVector.type(Type.instantType()), Types.TimestampType.withZone());
    }

    @Test
    void localDateTimeVectorType() {
        assertVectorType(
                ObjectVector.type((GenericType<?>) Type.find(LocalDateTime.class)),
                Types.TimestampType.withoutZone());
    }

    @Test
    void localDateVectorType() {
        assertVectorType(
                ObjectVector.type((GenericType<?>) Type.find(LocalDate.class)),
                Types.DateType.get());
    }

    @Test
    void localTimeVectorType() {
        assertVectorType(
                ObjectVector.type((GenericType<?>) Type.find(LocalTime.class)),
                Types.TimeType.get());
    }

    @Test
    void bigDecimalVectorType() {
        assertThatThrownBy(() -> Resolver.from(TableDefinition.of(ColumnDefinition.of(COL,
                ObjectVector.type((GenericType<?>) Type.find(BigDecimal.class))))))
                .isInstanceOf(Resolver.MappingException.class);
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
