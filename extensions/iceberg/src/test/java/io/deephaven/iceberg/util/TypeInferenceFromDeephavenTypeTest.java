//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.Type;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.OptionalAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class TypeInferenceFromDeephavenTypeTest {

    private TypeUtil.NextID nextId;

    @BeforeEach
    void setUp() {
        nextId = new AtomicInteger(1)::getAndIncrement;
    }

    @Test
    void booleanType() {
        assertInference(Type.booleanType()).hasValue(Types.BooleanType.get());
        assertInference(Type.booleanType().boxedType()).hasValue(Types.BooleanType.get());
    }

    @Test
    void byteType() {
        assertInference(Type.byteType()).isEmpty();
        assertInference(Type.byteType().boxedType()).isEmpty();
    }

    @Test
    void charType() {
        assertInference(Type.charType()).isEmpty();
        assertInference(Type.charType().boxedType()).isEmpty();
    }

    @Test
    void shortType() {
        assertInference(Type.shortType()).isEmpty();
        assertInference(Type.shortType().boxedType()).isEmpty();
    }

    @Test
    void intType() {
        assertInference(Type.intType()).hasValue(Types.IntegerType.get());
        assertInference(Type.intType().boxedType()).hasValue(Types.IntegerType.get());
    }

    @Test
    void longType() {
        assertInference(Type.longType()).hasValue(Types.LongType.get());
        assertInference(Type.longType().boxedType()).hasValue(Types.LongType.get());
    }

    @Test
    void floatType() {
        assertInference(Type.floatType()).hasValue(Types.FloatType.get());
        assertInference(Type.floatType().boxedType()).hasValue(Types.FloatType.get());
    }

    @Test
    void doubleType() {
        assertInference(Type.doubleType()).hasValue(Types.DoubleType.get());
        assertInference(Type.doubleType().boxedType()).hasValue(Types.DoubleType.get());
    }

    @Test
    void stringType() {
        assertInference(Type.stringType()).hasValue(Types.StringType.get());
    }

    @Test
    void instantType() {
        assertInference(Type.instantType()).hasValue(Types.TimestampType.withZone());
    }

    @Test
    void localDateTimeType() {
        assertInference(LocalDateTime.class).hasValue(Types.TimestampType.withoutZone());
    }

    @Test
    void localDateType() {
        assertInference(LocalDate.class).hasValue(Types.DateType.get());
    }

    @Test
    void localTimeType() {
        assertInference(LocalTime.class).hasValue(Types.TimeType.get());
    }

    @Test
    void bigDecimalType() {
        // Note: this is a difference right now; we can infer / read Iceberg DecimalType, but we can't infer for
        // BigDecimal for writing.
        assertInference(BigDecimal.class).isEmpty();
    }

    @Test
    void someCustomType() {
        assertInference(SomeCustomType.class).isEmpty();
    }

    @Test
    void arrayTypes() {
        for (final NativeArrayType<?, ?> type : PrimitiveType.instances().map(Type::arrayType)
                .collect(Collectors.toList())) {
            assertInference(type).isEmpty();
        }
        for (final NativeArrayType<?, ?> type : PrimitiveType.instances().map(PrimitiveType::boxedType)
                .map(Type::arrayType).collect(Collectors.toList())) {
            assertInference(type).isEmpty();
        }
        assertInference(Type.stringType().arrayType()).isEmpty();
        assertInference(Type.instantType().arrayType()).isEmpty();
        assertInference(Type.find(LocalDateTime.class).arrayType()).isEmpty();
        assertInference(Type.find(LocalDate.class).arrayType()).isEmpty();
        assertInference(Type.find(LocalTime.class).arrayType()).isEmpty();
        assertInference(Type.find(SomeCustomType.class).arrayType()).isEmpty();
    }

    @Test
    void vectorTypes() {
        for (final PrimitiveVectorType<?, ?> type : Arrays.asList(ByteVector.type(), CharVector.type(),
                ShortVector.type(), IntVector.type(), LongVector.type(), FloatVector.type(), DoubleVector.type())) {
            assertInference(type).isEmpty();
        }
        assertInference(ObjectVector.type(Type.stringType())).isEmpty();
        assertInference(ObjectVector.type(Type.instantType())).isEmpty();
        assertInference(ObjectVector.type(CustomType.of(LocalDateTime.class))).isEmpty();
        assertInference(ObjectVector.type(CustomType.of(LocalDate.class))).isEmpty();
        assertInference(ObjectVector.type(CustomType.of(LocalTime.class))).isEmpty();
        assertInference(ObjectVector.type(CustomType.of(SomeCustomType.class))).isEmpty();
    }

    private OptionalAssert<org.apache.iceberg.types.Type> assertInference(Class<?> clazz) {
        return assertInference(Type.find(clazz));
    }

    private OptionalAssert<org.apache.iceberg.types.Type> assertInference(Type<?> type) {
        return assertThat(TypeInference.of(type, nextId));
    }

    public static class SomeCustomType {

    }
}
