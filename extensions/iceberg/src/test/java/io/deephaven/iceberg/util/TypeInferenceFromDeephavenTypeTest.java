//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.OptionalAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;

class TypeInferenceFromDeephavenTypeTest {

    private Type.Visitor<org.apache.iceberg.types.Type> inferenceVisitor;

    // Dummy value for field ID, used for array types and vectors.
    private static final int FIELD_ID = 5;

    @BeforeEach
    void setUp() {
        inferenceVisitor = new TypeInference.BestIcebergType(() -> FIELD_ID);
    }

    @Test
    void booleanType() {
        assertInference(Type.booleanType()).hasValue(Types.BooleanType.get());
        assertInference(Type.booleanType().boxedType()).hasValue(Types.BooleanType.get());
    }

    @Test
    void byteType() {
        assertInference(Type.byteType()).hasValue(Types.IntegerType.get());
        assertInference(Type.byteType().boxedType()).hasValue(Types.IntegerType.get());
    }

    @Test
    void charType() {
        assertInference(Type.charType()).isEmpty();
        assertInference(Type.charType().boxedType()).isEmpty();
    }

    @Test
    void shortType() {
        assertInference(Type.shortType()).hasValue(Types.IntegerType.get());
        assertInference(Type.shortType().boxedType()).hasValue(Types.IntegerType.get());
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
    void booleanArrayType() {
        assertInference(Type.booleanType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.BooleanType.get()));
        assertInference(Type.booleanType().boxedType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.BooleanType.get()));
    }

    @Test
    void byteArrayType() {
        assertInference(Type.byteType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.IntegerType.get()));
        assertInference(Type.byteType().boxedType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.IntegerType.get()));
    }

    @Test
    void shortArrayType() {
        assertInference(Type.shortType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.IntegerType.get()));
        assertInference(Type.shortType().boxedType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.IntegerType.get()));
    }

    @Test
    void intArrayType() {
        assertInference(Type.intType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.IntegerType.get()));
        assertInference(Type.intType().boxedType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.IntegerType.get()));
    }

    @Test
    void longArrayType() {
        assertInference(Type.longType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.LongType.get()));
        assertInference(Type.longType().boxedType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.LongType.get()));
    }

    @Test
    void floatArrayType() {
        assertInference(Type.floatType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.FloatType.get()));
        assertInference(Type.floatType().boxedType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.FloatType.get()));
    }

    @Test
    void doubleArrayType() {
        assertInference(Type.doubleType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.DoubleType.get()));
        assertInference(Type.doubleType().boxedType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.DoubleType.get()));
    }

    @Test
    void stringArrayType() {
        assertInference(Type.stringType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.StringType.get()));
    }

    @Test
    void instantArrayType() {
        assertInference(Type.instantType().arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.TimestampType.withZone()));
    }

    @Test
    void localDateTimeArrayType() {
        assertInference(Type.find(LocalDateTime.class).arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.TimestampType.withoutZone()));
    }

    @Test
    void localDateArrayType() {
        assertInference(Type.find(LocalDate.class).arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.DateType.get()));
    }

    @Test
    void localTimeArrayType() {
        assertInference(Type.find(LocalTime.class).arrayType())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.TimeType.get()));
    }

    @Test
    void bigDecimalArrayType() {
        assertInference(Type.find(BigDecimal.class).arrayType()).isEmpty();
    }

    @Test
    void someCustomTypeArrayType() {
        assertInference(Type.find(SomeCustomType.class).arrayType()).isEmpty();
    }

    @Test
    void byteVectorType() {
        assertInference(ByteVector.type())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.IntegerType.get()));
    }

    @Test
    void shortVectorType() {
        assertInference(ShortVector.type())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.IntegerType.get()));
    }

    @Test
    void intVectorType() {
        assertInference(IntVector.type())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.IntegerType.get()));
    }

    @Test
    void longVectorType() {
        assertInference(LongVector.type())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.LongType.get()));
    }

    @Test
    void floatVectorType() {
        assertInference(FloatVector.type())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.FloatType.get()));
    }

    @Test
    void doubleVectorType() {
        assertInference(DoubleVector.type())
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.DoubleType.get()));
    }

    @Test
    void stringVectorType() {
        assertInference(ObjectVector.type(Type.stringType()))
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.StringType.get()));
    }

    @Test
    void instantVectorType() {
        assertInference(ObjectVector.type(Type.instantType()))
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.TimestampType.withZone()));
    }

    @Test
    void localDateTimeVectorType() {
        assertInference(ObjectVector.type((GenericType<?>) Type.find(LocalDateTime.class)))
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.TimestampType.withoutZone()));
    }

    @Test
    void localDateVectorType() {
        assertInference(ObjectVector.type((GenericType<?>) Type.find(LocalDate.class)))
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.DateType.get()));
    }

    @Test
    void localTimeVectorType() {
        assertInference(ObjectVector.type((GenericType<?>) Type.find(LocalTime.class)))
                .hasValue(Types.ListType.ofOptional(FIELD_ID, Types.TimeType.get()));
    }

    @Test
    void bigDecimalVectorType() {
        assertInference(ObjectVector.type((GenericType<?>) Type.find(BigDecimal.class))).isEmpty();
    }

    @Test
    void someCustomTypeVectorType() {
        assertInference(ObjectVector.type(CustomType.of(SomeCustomType.class))).isEmpty();
    }

    private OptionalAssert<org.apache.iceberg.types.Type> assertInference(Class<?> clazz) {
        return assertInference(Type.find(clazz));
    }

    private OptionalAssert<org.apache.iceberg.types.Type> assertInference(Type<?> type) {
        return assertThat(TypeInference.of(type, inferenceVisitor));
    }

    public static class SomeCustomType {

    }
}
