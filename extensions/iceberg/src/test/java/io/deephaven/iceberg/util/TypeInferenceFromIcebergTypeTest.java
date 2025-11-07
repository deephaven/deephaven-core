//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.qst.type.Type;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.OptionalAssert;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;

class TypeInferenceFromIcebergTypeTest {

    @Test
    void booleanType() {
        assertInference(Types.BooleanType.get()).hasValue(Type.booleanType().boxedType());
    }

    @Test
    void integerType() {
        assertInference(Types.IntegerType.get()).hasValue(Type.intType());
    }

    @Test
    void longType() {
        assertInference(Types.LongType.get()).hasValue(Type.longType());
    }

    @Test
    void floatType() {
        assertInference(Types.FloatType.get()).hasValue(Type.floatType());
    }

    @Test
    void doubleType() {
        assertInference(Types.DoubleType.get()).hasValue(Type.doubleType());
    }

    @Test
    void dateType() {
        assertInference(Types.DateType.get()).hasValue(Type.find(LocalDate.class));
    }

    @Test
    void timeType() {
        assertInference(Types.TimeType.get()).hasValue(Type.find(LocalTime.class));
    }

    @Test
    void timestampType() {
        assertInference(Types.TimestampType.withZone()).hasValue(Type.instantType());
        assertInference(Types.TimestampType.withoutZone()).hasValue(Type.find(LocalDateTime.class));
    }

    @Test
    void timestampNanoType() {
        assertInference(Types.TimestampNanoType.withZone()).isEmpty();
        assertInference(Types.TimestampNanoType.withoutZone()).isEmpty();
    }

    @Test
    void stringType() {
        assertInference(Types.StringType.get()).hasValue(Type.stringType());
    }

    @Test
    void uuidType() {
        assertInference(Types.UUIDType.get()).isEmpty();
    }

    @Test
    void fixedType() {
        assertInference(Types.FixedType.ofLength(1)).hasValue(Type.byteType().arrayType());
        assertInference(Types.FixedType.ofLength(42)).hasValue(Type.byteType().arrayType());
    }

    @Test
    void binaryType() {
        assertInference(Types.BinaryType.get()).hasValue(Type.byteType().arrayType());
    }

    @Test
    void decimalType() {
        assertInference(Types.DecimalType.of(3, 4)).hasValue(Type.find(BigDecimal.class));
        assertInference(Types.DecimalType.of(5, 5)).hasValue(Type.find(BigDecimal.class));
    }

    @Test
    void booleanListType() {
        assertInference(Types.ListType.ofOptional(1, Types.BooleanType.get()))
                .hasValue(Type.booleanType().boxedType().arrayType());
    }

    @Test
    void integerListType() {
        assertInference(Types.ListType.ofOptional(1, Types.IntegerType.get())).hasValue(Type.intType().arrayType());
    }

    @Test
    void longListType() {
        assertInference(Types.ListType.ofOptional(1, Types.LongType.get())).hasValue(Type.longType().arrayType());
    }

    @Test
    void floatListType() {
        assertInference(Types.ListType.ofOptional(1, Types.FloatType.get())).hasValue(Type.floatType().arrayType());
    }

    @Test
    void doubleListType() {
        assertInference(Types.ListType.ofOptional(1, Types.DoubleType.get())).hasValue(Type.doubleType().arrayType());
    }

    @Test
    void dateListType() {
        assertInference(Types.ListType.ofOptional(1, Types.DateType.get()))
                .hasValue(Type.find(LocalDate.class).arrayType());
    }

    @Test
    void timeListType() {
        assertInference(Types.ListType.ofOptional(1, Types.TimeType.get()))
                .hasValue(Type.find(LocalTime.class).arrayType());
    }

    @Test
    void timestampListType() {
        assertInference(Types.ListType.ofOptional(1, Types.TimestampType.withZone()))
                .hasValue(Type.instantType().arrayType());
        assertInference(Types.ListType.ofOptional(1, Types.TimestampType.withoutZone()))
                .hasValue(Type.find(LocalDateTime.class).arrayType());
    }

    @Test
    void timestampNanoListType() {
        assertInference(Types.ListType.ofOptional(1, Types.TimestampNanoType.withZone())).isEmpty();
        assertInference(Types.ListType.ofOptional(1, Types.TimestampNanoType.withoutZone())).isEmpty();
    }

    @Test
    void stringListType() {
        assertInference(Types.ListType.ofOptional(1, Types.StringType.get()))
                .hasValue(Type.stringType().arrayType());
    }

    @Test
    void uuidListType() {
        assertInference(Types.ListType.ofOptional(1, Types.UUIDType.get())).isEmpty();
    }

    @Test
    void fixedListType() {
        assertInference(Types.ListType.ofOptional(1, Types.FixedType.ofLength(1))).isEmpty();
        assertInference(Types.ListType.ofOptional(1, Types.FixedType.ofLength(42))).isEmpty();
    }

    @Test
    void binaryListType() {
        assertInference(Types.ListType.ofOptional(1, Types.BinaryType.get())).isEmpty();
    }

    @Test
    void decimalListType() {
        assertInference(Types.ListType.ofOptional(1, Types.DecimalType.of(3, 4)))
                .hasValue(Type.find(BigDecimal.class).arrayType());
        assertInference(Types.ListType.ofOptional(1, Types.DecimalType.of(5, 5)))
                .hasValue(Type.find(BigDecimal.class).arrayType());
    }

    @Test
    void structType() {
        assertInference(Types.StructType.of(Types.NestedField.optional(1, "Foo", Types.IntegerType.get()))).isEmpty();
    }

    @Test
    void mapType() {
        assertInference(Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.IntegerType.get())).isEmpty();
    }

    @Test
    void variantType() {
        assertInference(Types.VariantType.get()).isEmpty();
    }

    @Test
    void unknownType() {
        assertInference(Types.UnknownType.get()).isEmpty();
    }

    private static OptionalAssert<Type<?>> assertInference(org.apache.iceberg.types.Type type) {
        return assertThat(TypeInference.of(type));
    }
}
