//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.GenericVectorType;
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
import io.deephaven.vector.Vector;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnDefinitionConstructionTest {
    private static final String CN = "Foo";

    @Test
    public void ofBoolean() {
        checkPrimitiveType(Type.booleanType(), ColumnDefinition.ofBoolean(CN));
    }

    @Test
    public void ofByte() {
        checkPrimitiveType(Type.byteType(), ColumnDefinition.ofByte(CN));
    }

    @Test
    public void ofChar() {
        checkPrimitiveType(Type.charType(), ColumnDefinition.ofChar(CN));
    }

    @Test
    public void ofShort() {
        checkPrimitiveType(Type.shortType(), ColumnDefinition.ofShort(CN));
    }

    @Test
    public void ofInt() {
        checkPrimitiveType(Type.intType(), ColumnDefinition.ofInt(CN));
    }

    @Test
    public void ofLong() {
        checkPrimitiveType(Type.longType(), ColumnDefinition.ofLong(CN));
    }

    @Test
    public void ofFloat() {
        checkPrimitiveType(Type.floatType(), ColumnDefinition.ofFloat(CN));
    }

    @Test
    public void ofDouble() {
        checkPrimitiveType(Type.doubleType(), ColumnDefinition.ofDouble(CN));
    }

    @Test
    public void ofString() {
        checkSimpleGenericType(Type.stringType(), ColumnDefinition.ofString(CN));
    }

    @Test
    public void ofTime() {
        checkSimpleGenericType(Type.instantType(), ColumnDefinition.ofTime(CN));
    }

    @Test
    public void ofCustomType() {
        checkSimpleGenericType(MyCustomType.type(), ColumnDefinition.of(CN, MyCustomType.type()));
    }

    @Test
    public void booleanArray() {
        checkNativeArray(Type.booleanType().arrayType());
    }

    @Test
    public void boxedBooleanArray() {
        checkNativeArray(Type.booleanType().boxedType().arrayType());
    }

    @Test
    public void byteArray() {
        checkNativeArray(Type.byteType().arrayType());
    }

    @Test
    public void charArray() {
        checkNativeArray(Type.charType().arrayType());
    }

    @Test
    public void shortArray() {
        checkNativeArray(Type.shortType().arrayType());
    }

    @Test
    public void intArray() {
        checkNativeArray(Type.intType().arrayType());
    }

    @Test
    public void longArray() {
        checkNativeArray(Type.longType().arrayType());
    }

    @Test
    public void floatArray() {
        checkNativeArray(Type.floatType().arrayType());
    }

    @Test
    public void doubleArray() {
        checkNativeArray(Type.doubleType().arrayType());
    }

    @Test
    public void stringArray() {
        checkNativeArray(Type.stringType().arrayType());
    }

    @Test
    public void customTypeArray() {
        checkNativeArray(MyCustomType.type().arrayType());
    }

    @Test
    public void instantArray() {
        checkNativeArray(Type.instantType().arrayType());
    }

    @Test
    public void byteVector() {
        checkPrimitiveVector(ByteVector.type());
    }

    @Test
    public void charVector() {
        checkPrimitiveVector(CharVector.type());
    }

    @Test
    public void shortVector() {
        checkPrimitiveVector(ShortVector.type());
    }

    @Test
    public void intVector() {
        checkPrimitiveVector(IntVector.type());
    }

    @Test
    public void longVector() {
        checkPrimitiveVector(LongVector.type());
    }

    @Test
    public void floatVector() {
        checkPrimitiveVector(FloatVector.type());
    }

    @Test
    public void doubleVector() {
        checkPrimitiveVector(DoubleVector.type());
    }

    @Test
    public void stringVector() {
        checkGenericVector(ObjectVector.type(Type.stringType()));
    }

    @Test
    public void instantVector() {
        checkGenericVector(ObjectVector.type(Type.instantType()));
    }

    @Test
    public void customTypeVector() {
        checkGenericVector(ObjectVector.type(MyCustomType.type()));
    }

    private static void checkPrimitiveType(
            final PrimitiveType<?> type,
            final ColumnDefinition<?> expected) {
        assertThat(ColumnDefinition.of(CN, type)).isEqualTo(expected);
        assertThat(ColumnDefinition.of(CN, (Type<?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, expected.getDataType())).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, expected.getDataType(), null)).isEqualTo(expected);
    }

    private static void checkSimpleGenericType(
            final GenericType<?> type,
            final ColumnDefinition<?> expected) {
        assertThat(ColumnDefinition.of(CN, type)).isEqualTo(expected);
        assertThat(ColumnDefinition.of(CN, (Type<?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, expected.getDataType())).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, expected.getDataType(), null)).isEqualTo(expected);
    }

    private static <T> void checkNativeArray(final NativeArrayType<T, ?> type) {
        final ColumnDefinition<T> expected = ColumnDefinition.of(CN, type);
        assertThat(ColumnDefinition.of(CN, (ArrayType<?, ?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.of(CN, (GenericType<?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.of(CN, (Type<?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, type.clazz())).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, type.clazz(), type.componentType().clazz()))
                .isEqualTo(expected);
    }

    private static <T extends Vector<T>> void checkPrimitiveVector(final PrimitiveVectorType<T, ?> type) {
        final ColumnDefinition<T> expected = ColumnDefinition.of(CN, type);
        assertThat(ColumnDefinition.of(CN, (ArrayType<?, ?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.of(CN, (GenericType<?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.of(CN, (Type<?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, type.clazz())).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, type.clazz(), type.componentType().clazz()))
                .isEqualTo(expected);
    }

    private static <CT> void checkGenericVector(final GenericVectorType<ObjectVector<CT>, CT> type) {
        final ColumnDefinition<ObjectVector<CT>> expected = ColumnDefinition.of(CN, type);
        assertThat(ColumnDefinition.of(CN, (ArrayType<?, ?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.of(CN, (GenericType<?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.of(CN, (Type<?>) type)).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, type.clazz(), type.componentType().clazz()))
                .isEqualTo(expected);
    }

    private static final class MyCustomType {
        private static final CustomType<MyCustomType> TYPE = Type.ofCustom(MyCustomType.class);

        public static CustomType<MyCustomType> type() {
            return TYPE;
        }
    }
}
