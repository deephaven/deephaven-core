//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.qst.type.*;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TypeCompatibility}.
 */
class TypeCompatibilityTest {

    private static final int DUMMY_LIST_ID = 99;

    private static Types.ListType listOf(final org.apache.iceberg.types.Type.PrimitiveType element) {
        return Types.ListType.ofOptional(DUMMY_LIST_ID, element);
    }

    private static void ok(final Type<?> dhType, final org.apache.iceberg.types.Type icebergType) {
        assertThat(TypeCompatibility.isCompatible(dhType, icebergType)).isTrue();
    }

    private static void bad(final Type<?> dhType, final org.apache.iceberg.types.Type icebergType) {
        assertThat(TypeCompatibility.isCompatible(dhType, icebergType)).isFalse();
    }

    @Test
    void booleanType() {
        ok(Type.booleanType(), Types.BooleanType.get());
        bad(Type.booleanType(), Types.IntegerType.get());
    }

    @Test
    void byteType() {
        ok(Type.byteType(), Types.IntegerType.get());
        ok(Type.byteType(), Types.LongType.get());
        bad(Type.byteType(), Types.DoubleType.get());
    }

    @Test
    void charType() {
        ok(Type.charType(), Types.IntegerType.get());
        ok(Type.charType(), Types.LongType.get());
        bad(Type.charType(), Types.DoubleType.get());
    }

    @Test
    void shortType() {
        ok(Type.shortType(), Types.IntegerType.get());
        ok(Type.shortType(), Types.LongType.get());
        bad(Type.shortType(), Types.DoubleType.get());
    }

    @Test
    void intType() {
        ok(Type.intType(), Types.IntegerType.get());
        ok(Type.intType(), Types.LongType.get());
        bad(Type.intType(), Types.DoubleType.get());
    }

    @Test
    void longType() {
        ok(Type.longType(), Types.IntegerType.get());
        ok(Type.longType(), Types.LongType.get());
        bad(Type.longType(), Types.DoubleType.get());
    }

    @Test
    void floatType() {
        ok(Type.floatType(), Types.IntegerType.get());
        ok(Type.floatType(), Types.LongType.get());
        ok(Type.floatType(), Types.FloatType.get());
        ok(Type.floatType(), Types.DoubleType.get());
        bad(Type.floatType(), Types.BooleanType.get());

    }

    @Test
    void doubleType() {
        ok(Type.doubleType(), Types.IntegerType.get());
        ok(Type.doubleType(), Types.LongType.get());
        ok(Type.doubleType(), Types.FloatType.get());
        ok(Type.doubleType(), Types.DoubleType.get());
        bad(Type.doubleType(), Types.BooleanType.get());
    }

    @Test
    void stringType() {
        ok(Type.stringType(), Types.StringType.get());
        bad(Type.stringType(), Types.BooleanType.get());
    }

    @Test
    void timestampWithZoneType() {
        ok(Type.instantType(), Types.TimestampType.withZone());
        bad(Type.instantType(), Types.TimestampType.withoutZone());
    }

    @Test
    void timestampWithoutZoneType() {
        ok(Type.find(LocalDateTime.class), Types.TimestampType.withoutZone());
        bad(Type.find(LocalDateTime.class), Types.TimestampType.withZone());
    }

    @Test
    void dateType() {
        ok(Type.find(LocalDate.class), Types.DateType.get());
        bad(Type.find(LocalDate.class), Types.TimestampType.withZone());
    }

    @Test
    void timeType() {
        ok(Type.find(LocalTime.class), Types.TimeType.get());
        bad(Type.find(LocalTime.class), Types.TimestampType.withZone());
    }

    @Test
    void decimalType() {
        ok(Type.find(BigDecimal.class), Types.DecimalType.of(10, 4));
        bad(Type.find(BigDecimal.class), Types.DoubleType.get());
    }

    /** Array types **/

    @Test
    void booleanArrayType() {
        ok(Type.booleanType().arrayType(), listOf(Types.BooleanType.get()));
        bad(Type.booleanType().arrayType(), listOf(Types.IntegerType.get()));
    }

    @Test
    void byteArrayType() {
        ok(Type.byteType().arrayType(), listOf(Types.IntegerType.get()));
        ok(Type.byteType().arrayType(), Types.BinaryType.get());
        ok(Type.byteType().arrayType(), Types.FixedType.ofLength(16));
        bad(Type.byteType().arrayType(), listOf(Types.BooleanType.get()));
    }

    @Test
    void charArrayType() {
        ok(Type.charType().arrayType(), listOf(Types.IntegerType.get()));
        ok(Type.charType().arrayType(), listOf(Types.LongType.get()));
        bad(Type.charType().arrayType(), listOf(Types.DoubleType.get()));
    }

    @Test
    void shortArrayType() {
        ok(Type.shortType().arrayType(), listOf(Types.IntegerType.get()));
        ok(Type.shortType().arrayType(), listOf(Types.LongType.get()));
        bad(Type.shortType().arrayType(), listOf(Types.DoubleType.get()));
    }

    @Test
    void intArrayType() {
        ok(Type.intType().arrayType(), listOf(Types.IntegerType.get()));
        ok(Type.intType().arrayType(), listOf(Types.LongType.get()));
        bad(Type.intType().arrayType(), listOf(Types.DoubleType.get()));
    }

    @Test
    void longArrayType() {
        ok(Type.longType().arrayType(), listOf(Types.IntegerType.get()));
        ok(Type.longType().arrayType(), listOf(Types.LongType.get()));
        bad(Type.longType().arrayType(), listOf(Types.DoubleType.get()));
    }

    @Test
    void floatArrayType() {
        ok(Type.floatType().arrayType(), listOf(Types.FloatType.get()));
        ok(Type.floatType().arrayType(), listOf(Types.DoubleType.get()));
        ok(Type.floatType().arrayType(), listOf(Types.IntegerType.get()));
        bad(Type.floatType().arrayType(), listOf(Types.BooleanType.get()));
    }

    @Test
    void doubleArrayType() {
        ok(Type.doubleType().arrayType(), listOf(Types.DoubleType.get()));
        ok(Type.doubleType().arrayType(), listOf(Types.IntegerType.get()));
        bad(Type.doubleType().arrayType(), listOf(Types.BooleanType.get()));
    }

    @Test
    void stringArrayType() {
        ok(Type.stringType().arrayType(), listOf(Types.StringType.get()));
        bad(Type.stringType().arrayType(), listOf(Types.BooleanType.get()));
    }

    @Test
    void timestampWithZoneArrayType() {
        ok(Type.instantType().arrayType(), listOf(Types.TimestampType.withZone()));
        bad(Type.instantType().arrayType(), listOf(Types.TimestampType.withoutZone()));
    }

    @Test
    void timestampWithoutZoneArrayType() {
        ok(Type.find(LocalDateTime.class).arrayType(), listOf(Types.TimestampType.withoutZone()));
        bad(Type.find(LocalDateTime.class).arrayType(), listOf(Types.TimestampType.withZone()));
    }

    @Test
    void dateArrayType() {
        ok(Type.find(LocalDate.class).arrayType(), listOf(Types.DateType.get()));
        bad(Type.find(LocalDate.class).arrayType(), listOf(Types.TimestampType.withZone()));
    }

    @Test
    void timeArrayType() {
        ok(Type.find(LocalTime.class).arrayType(), listOf(Types.TimeType.get()));
        bad(Type.find(LocalTime.class).arrayType(), listOf(Types.TimestampType.withZone()));
    }

    @Test
    void decimalArrayType() {
        ok(Type.find(BigDecimal.class).arrayType(), listOf(Types.DecimalType.of(10, 4)));
        bad(Type.find(BigDecimal.class).arrayType(), listOf(Types.DoubleType.get()));
    }

    /** Vector types **/

    @Test
    void booleanVectorType() {
        ok(ObjectVector.type(Type.booleanType().boxedType()), listOf(Types.BooleanType.get()));
        bad(ObjectVector.type(Type.booleanType().boxedType()), listOf(Types.IntegerType.get()));
    }

    @Test
    void byteVectorType() {
        ok(ByteVector.type(), listOf(Types.IntegerType.get()));
        ok(ObjectVector.type(Type.byteType().boxedType()), listOf(Types.IntegerType.get()));

        ok(ByteVector.type(), listOf(Types.LongType.get()));
        ok(ObjectVector.type(Type.byteType().boxedType()), listOf(Types.LongType.get()));

        bad(ByteVector.type(), listOf(Types.BooleanType.get()));
    }

    @Test
    void charVectorType() {
        ok(CharVector.type(), listOf(Types.IntegerType.get()));
        ok(ObjectVector.type(Type.charType().boxedType()), listOf(Types.IntegerType.get()));

        ok(CharVector.type(), listOf(Types.LongType.get()));
        ok(ObjectVector.type(Type.charType().boxedType()), listOf(Types.LongType.get()));

        bad(ObjectVector.type(Type.charType().boxedType()), listOf(Types.DoubleType.get()));
    }

    @Test
    void shortVectorType() {
        ok(ShortVector.type(), listOf(Types.IntegerType.get()));
        ok(ObjectVector.type(Type.shortType().boxedType()), listOf(Types.IntegerType.get()));

        ok(ShortVector.type(), listOf(Types.LongType.get()));
        ok(ObjectVector.type(Type.shortType().boxedType()), listOf(Types.LongType.get()));

        bad(ShortVector.type(), listOf(Types.BooleanType.get()));
    }

    @Test
    void intVectorType() {
        ok(IntVector.type(), listOf(Types.IntegerType.get()));
        ok(ObjectVector.type(Type.intType().boxedType()), listOf(Types.IntegerType.get()));

        ok(IntVector.type(), listOf(Types.LongType.get()));
        ok(ObjectVector.type(Type.intType().boxedType()), listOf(Types.LongType.get()));

        bad(IntVector.type(), listOf(Types.DoubleType.get()));
    }

    @Test
    void longVectorType() {
        ok(LongVector.type(), listOf(Types.IntegerType.get()));
        ok(ObjectVector.type(Type.longType().boxedType()), listOf(Types.IntegerType.get()));

        ok(LongVector.type(), listOf(Types.LongType.get()));
        ok(ObjectVector.type(Type.longType().boxedType()), listOf(Types.LongType.get()));

        bad(LongVector.type(), listOf(Types.DoubleType.get()));
    }

    @Test
    void floatVectorType() {
        ok(FloatVector.type(), listOf(Types.FloatType.get()));
        ok(ObjectVector.type(Type.floatType().boxedType()), listOf(Types.FloatType.get()));

        ok(FloatVector.type(), listOf(Types.IntegerType.get()));
        ok(ObjectVector.type(Type.floatType().boxedType()), listOf(Types.IntegerType.get()));

        ok(FloatVector.type(), listOf(Types.LongType.get()));
        ok(ObjectVector.type(Type.floatType().boxedType()), listOf(Types.LongType.get()));

        ok(FloatVector.type(), listOf(Types.DoubleType.get()));
        ok(ObjectVector.type(Type.floatType().boxedType()), listOf(Types.DoubleType.get()));

        bad(FloatVector.type(), listOf(Types.BooleanType.get()));
    }

    @Test
    void doubleVectorType() {
        ok(DoubleVector.type(), listOf(Types.DoubleType.get()));
        ok(ObjectVector.type(Type.doubleType().boxedType()), listOf(Types.DoubleType.get()));

        ok(DoubleVector.type(), listOf(Types.IntegerType.get()));
        ok(ObjectVector.type(Type.doubleType().boxedType()), listOf(Types.IntegerType.get()));

        ok(DoubleVector.type(), listOf(Types.LongType.get()));
        ok(ObjectVector.type(Type.doubleType().boxedType()), listOf(Types.LongType.get()));

        bad(DoubleVector.type(), listOf(Types.BooleanType.get()));
    }

    @Test
    void stringVectorType() {
        ok(ObjectVector.type(Type.stringType()), listOf(Types.StringType.get()));
        bad(ObjectVector.type(Type.stringType()), listOf(Types.BooleanType.get()));
    }

    @Test
    void timestampWithZoneVectorType() {
        ok(ObjectVector.type(Type.instantType()), listOf(Types.TimestampType.withZone()));
        bad(ObjectVector.type(Type.instantType()), listOf(Types.TimestampType.withoutZone()));
    }

    @Test
    void timestampWithoutZoneVectorType() {
        ok(ObjectVector.type((GenericType<?>) Type.find(LocalDateTime.class)),
                listOf(Types.TimestampType.withoutZone()));
        bad(ObjectVector.type((GenericType<?>) Type.find(LocalDateTime.class)), listOf(Types.TimestampType.withZone()));
    }

    @Test
    void dateVectorType() {
        ok(ObjectVector.type((GenericType<?>) Type.find(LocalDate.class)), listOf(Types.DateType.get()));
        bad(ObjectVector.type((GenericType<?>) Type.find(LocalDate.class)), listOf(Types.TimestampType.withZone()));
    }

    @Test
    void timeVectorType() {
        ok(ObjectVector.type((GenericType<?>) Type.find(LocalTime.class)), listOf(Types.TimeType.get()));
        bad(ObjectVector.type((GenericType<?>) Type.find(LocalTime.class)), listOf(Types.TimestampType.withZone()));
    }

    @Test
    void decimalVectorType() {
        ok(ObjectVector.type((GenericType<?>) Type.find(BigDecimal.class)), listOf(Types.DecimalType.of(10, 4)));
        bad(ObjectVector.type((GenericType<?>) Type.find(BigDecimal.class)), listOf(Types.DoubleType.get()));
    }
}
