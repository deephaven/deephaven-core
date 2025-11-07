//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampNanoType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;
import org.apache.iceberg.types.Types.VariantType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static io.deephaven.iceberg.util.ColumnInstructions.partitionField;
import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * This test specifics around {@link Resolver#infer(InferenceInstructions)}; more general validations around
 * {@link Resolver} should be in {@link ResolverTest}.
 */
class ResolverInferTest {

    // Note: Schema does not implement equals. In this testing, the expected and actual need to share the exact same
    // schema (not a problem, since we aren't inferring the Schema, it's given to us).

    private static final IntegerType IT = IntegerType.get();

    static InferenceInstructions ia(Schema schema) {
        return InferenceInstructions.builder()
                .schema(schema)
                .failOnUnsupportedTypes(true)
                .build();
    }

    public static Resolver empty(Schema schema) {
        return Resolver.builder()
                .schema(schema)
                .definition(TableDefinition.of(List.of()))
                .build();
    }

    @Test
    void BooleanType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(BooleanType.get());
        final Resolver expected = simpleMapping(schema, Type.booleanType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void IntegerType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(IT);
        final Resolver expected = simpleMapping(schema, Type.intType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void LongType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(LongType.get());
        final Resolver expected = simpleMapping(schema, Type.longType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void FloatType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(FloatType.get());
        final Resolver expected = simpleMapping(schema, Type.floatType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void DoubleType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(DoubleType.get());
        final Resolver expected = simpleMapping(schema, Type.doubleType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void DateType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(DateType.get());
        final Resolver expected = simpleMapping(schema, Type.find(LocalDate.class));
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void TimeType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(TimeType.get());
        final Resolver expected = simpleMapping(schema, Type.find(LocalTime.class));
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void TimestampTypeWithZone() throws TypeInference.Exception {
        final Schema schema = simpleSchema(TimestampType.withZone());
        final Resolver expected = simpleMapping(schema, Type.instantType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void TimestampTypeWithoutZone() throws TypeInference.Exception {
        final Schema schema = simpleSchema(TimestampType.withoutZone());
        final Resolver expected = simpleMapping(schema, Type.find(LocalDateTime.class));
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void TimestampNanoTypeWithZone() throws TypeInference.Exception {
        final Schema schema = simpleSchema(TimestampNanoType.withZone());
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `timestamptz_ns` at fieldName `F1`");
        }
    }

    @Test
    void TimestampNanoTypeWithoutZone() throws TypeInference.Exception {
        final Schema schema = simpleSchema(TimestampNanoType.withoutZone());
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `timestamp_ns` at fieldName `F1`");
        }
    }

    @Test
    void StringType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(StringType.get());
        final Resolver expected = simpleMapping(schema, Type.stringType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void BinaryType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(BinaryType.get());
        final Resolver expected = simpleMapping(schema, Type.byteType().arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void FixedType_4() throws TypeInference.Exception {
        final Schema schema = simpleSchema(FixedType.ofLength(4));
        final Resolver expected = simpleMapping(schema, Type.byteType().arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void DecimalType_3_4() throws TypeInference.Exception {
        final Schema schema = simpleSchema(DecimalType.of(3, 4));
        final Resolver expected = simpleMapping(schema, Type.find(BigDecimal.class));
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void UuidType() throws TypeInference.Exception {
        final Schema schema = simpleSchema(UUIDType.get());
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `uuid` at fieldName `F1`");
            assertThat(e.type()).isEqualTo(UUIDType.get());
        }
    }

    @Test
    void BooleanListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(BooleanType.get());
        final Resolver expected = simpleMapping(schema, Type.booleanType().boxedType().arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void IntegerListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(IT);
        final Resolver expected = simpleMapping(schema, Type.intType().arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void LongListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(LongType.get());
        final Resolver expected = simpleMapping(schema, Type.longType().arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void FloatListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(FloatType.get());
        final Resolver expected = simpleMapping(schema, Type.floatType().arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void DoubleListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(DoubleType.get());
        final Resolver expected = simpleMapping(schema, Type.doubleType().arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void DateListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(DateType.get());
        final Resolver expected = simpleMapping(schema, Type.find(LocalDate.class).arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void TimeListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(TimeType.get());
        final Resolver expected = simpleMapping(schema, Type.find(LocalTime.class).arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void TimestampTypeWithZoneList() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(TimestampType.withZone());
        final Resolver expected = simpleMapping(schema, Type.instantType().arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void TimestampTypeWithoutZoneList() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(TimestampType.withoutZone());
        final Resolver expected =
                simpleMapping(schema, Type.find(LocalDateTime.class).arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void TimestampNanoTypeWithZoneList() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(TimestampNanoType.withZone());
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `list<timestamptz_ns>` at fieldName `F1`");
        }
    }

    @Test
    void TimestampNanoTypeWithoutZoneList() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(TimestampNanoType.withoutZone());
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `list<timestamp_ns>` at fieldName `F1`");
        }
    }

    @Test
    void StringListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(StringType.get());
        final Resolver expected = simpleMapping(schema, Type.stringType().arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void BinaryListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(BinaryType.get());
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `list<binary>` at fieldName `F1`");
            assertThat(e.type()).isEqualTo(ListType.ofOptional(1, BinaryType.get()));
        }
    }

    @Test
    void FixedType_4List() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(FixedType.ofLength(4));
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `list<fixed[4]>` at fieldName `F1`");
            assertThat(e.type()).isEqualTo(ListType.ofOptional(1, FixedType.ofLength(4)));
        }
    }

    @Test
    void DecimalType_3_4List() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(DecimalType.of(3, 4));
        final Resolver expected = simpleMapping(schema, Type.find(BigDecimal.class).arrayType());
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void UuidListType() throws TypeInference.Exception {
        final Schema schema = simpleListSchema(UUIDType.get());
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `list<uuid>` at fieldName `F1`");
            assertThat(e.type()).isEqualTo(ListType.ofOptional(1, UUIDType.get()));
        }
    }

    @Test
    void StructType() throws TypeInference.Exception {
        final Schema schema = new Schema(
                NestedField.optional(3, "S1", StructType.of(
                        NestedField.optional(1, "F1", IT),
                        NestedField.required(2, "F2", IT))),
                NestedField.required(6, "S2", StructType.of(
                        NestedField.optional(4, "F1", IT),
                        NestedField.required(5, "F2", IT))));
        final Resolver expected = Resolver.builder()
                .schema(schema)
                .definition(TableDefinition.of(
                        ColumnDefinition.ofInt("S1_F1"),
                        ColumnDefinition.ofInt("S1_F2"),
                        ColumnDefinition.ofInt("S2_F1"),
                        ColumnDefinition.ofInt("S2_F2")))
                .putColumnInstructions("S1_F1", schemaField(1))
                .putColumnInstructions("S1_F2", schemaField(2))
                .putColumnInstructions("S2_F1", schemaField(4))
                .putColumnInstructions("S2_F2", schemaField(5))
                .build();
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void NestedStructType() throws TypeInference.Exception {
        final Schema schema = new Schema(NestedField.optional(1, "S1", StructType.of(
                NestedField.optional(2, "S2", StructType.of(
                        NestedField.optional(3, "F1", IT),
                        NestedField.required(4, "F2", IT))))));
        final Resolver expected = Resolver.builder()
                .schema(schema)
                .definition(TableDefinition.of(
                        ColumnDefinition.ofInt("S1_S2_F1"),
                        ColumnDefinition.ofInt("S1_S2_F2")))
                .putColumnInstructions("S1_S2_F1", schemaField(3))
                .putColumnInstructions("S1_S2_F2", schemaField(4))
                .build();
        assertThat(Resolver.infer(schema)).isEqualTo(expected);
        assertThat(Resolver.infer(ia(schema))).isEqualTo(expected);
    }

    @Test
    void MapType() throws TypeInference.Exception {
        final Schema schema = new Schema(
                NestedField.optional(9, "M1", MapType.ofOptional(1, 2, IT, IT)),
                NestedField.optional(10, "M2", MapType.ofRequired(3, 4, IT, IT)),
                NestedField.required(11, "M3", MapType.ofOptional(5, 6, IT, IT)),
                NestedField.required(12, "M4", MapType.ofRequired(7, 8, IT, IT)));
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `map<int, int>` at fieldName `M1`");
            assertThat(e.type()).isEqualTo(MapType.ofOptional(1, 2, IT, IT));
        }
    }

    @Test
    void VariantType() throws TypeInference.UnsupportedType {
        final Schema schema = simpleSchema(VariantType.get());
        assertThat(Resolver.infer(schema)).isEqualTo(empty(schema));
        try {
            Resolver.infer(ia(schema));
            failBecauseExceptionWasNotThrown(TypeInference.Exception.class);
        } catch (TypeInference.UnsupportedType e) {
            assertThat(e).hasMessageContaining("Unsupported Iceberg type `variant` at fieldName `F1`");
            assertThat(e.type()).isEqualTo(VariantType.get());
        }
    }

    @Test
    void identityPartition() throws TypeInference.UnsupportedType {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("F1").build();
        final InferenceInstructions ii = InferenceInstructions.builder()
                .schema(schema)
                .spec(spec)
                .failOnUnsupportedTypes(true)
                .build();
        assertThat(Resolver.infer(ii)).isEqualTo(Resolver.builder()
                .schema(schema)
                .spec(spec)
                .definition(TableDefinition.of(
                        ColumnDefinition.ofInt("F1").withPartitioning(),
                        ColumnDefinition.ofInt("F2")))
                .putColumnInstructions("F1", partitionField(spec.fields().get(0).fieldId()))
                .putColumnInstructions("F2", schemaField(43))
                .build());
    }

    @Test
    void skipUnknownPartition() throws TypeInference.UnsupportedType {
        final Schema schema = simpleSchema(IT);
        final PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("F1", 99).build();
        final InferenceInstructions ii = InferenceInstructions.builder()
                .schema(schema)
                .spec(spec)
                .failOnUnsupportedTypes(true)
                .build();
        assertThat(Resolver.infer(ii)).isEqualTo(Resolver.builder()
                .schema(schema)
                .definition(TableDefinition.of(
                        ColumnDefinition.ofInt("F1"),
                        ColumnDefinition.ofInt("F2")))
                .putColumnInstructions("F1", schemaField(42))
                .putColumnInstructions("F2", schemaField(43))
                .build());
    }

    // Note: this was a proposed test for a skip feature. If we need this feature, we can re-add it in the future.
    // @Test
    // void skipFields() throws Inference.UnsupportedType {
    // final Schema schema = new Schema(
    // NestedField.optional(42, "F1", IT),
    // NestedField.required(43, "F2", IT),
    // NestedField.optional(44, "F3", ListType.ofOptional(1, IT)));
    //
    // // We should be able to skip types, regardless of whether we support them or not.
    // // In this case, we are skipping a supported type [42], and skipping an unsupported type [44] which would
    // // otherwise cause an UnsupportedType exception.
    // final InferenceInstructions instructions = InferenceInstructions.builder()
    // .schema(schema)
    // .failOnUnsupportedTypes(true)
    // .addSkip(FieldPath.of(42))
    // .addSkip(FieldPath.of(44))
    // .build();
    //
    // assertThat(Resolver.infer(instructions)).isEqualTo(Resolver.builder()
    // .schema(schema)
    // .definition(TableDefinition.of(ColumnDefinition.ofInt("F2")))
    // .putColumnInstructions("F2", schemaField(43))
    // .build());
    // }

    @Test
    void alternativeNamer() throws TypeInference.UnsupportedType {
        final Schema schema = simpleSchema(IT);
        final InferenceInstructions instructions = InferenceInstructions.builder()
                .schema(schema)
                .failOnUnsupportedTypes(true)
                .namerFactory(InferenceInstructions.Namer.Factory.fieldId())
                .build();

        assertThat(Resolver.infer(instructions)).isEqualTo(Resolver.builder()
                .schema(schema)
                .definition(TableDefinition.of(
                        ColumnDefinition.ofInt("FieldId_42"),
                        ColumnDefinition.ofInt("FieldId_43")))
                .putColumnInstructions("FieldId_42", schemaField(42))
                .putColumnInstructions("FieldId_43", schemaField(43))
                .build());
    }

    private static Schema simpleSchema(org.apache.iceberg.types.Type type) {
        return new Schema(
                NestedField.optional(42, "F1", type),
                NestedField.required(43, "F2", type));
    }

    private static Schema simpleListSchema(org.apache.iceberg.types.Type type) {
        return new Schema(
                NestedField.optional(42, "F1", ListType.ofOptional(1, type)),
                NestedField.required(43, "F2", ListType.ofOptional(2, type)));
    }

    private static TableDefinition simpleDefinition(Type<?> type) {
        return TableDefinition.of(
                ColumnDefinition.of("F1", type),
                ColumnDefinition.of("F2", type));
    }

    private static Resolver simpleMapping(Schema schema, Type<?> type) {
        return Resolver.builder()
                .schema(schema)
                .definition(simpleDefinition(type))
                .putColumnInstructions("F1", schemaField(42))
                .putColumnInstructions("F2", schemaField(43))
                .build();
    }
}
