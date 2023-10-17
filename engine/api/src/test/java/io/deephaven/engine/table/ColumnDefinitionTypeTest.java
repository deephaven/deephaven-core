/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.GenericTypeBase;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.Type;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.ShortVectorDirect;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class ColumnDefinitionTypeTest {

    private static final String FOO = "Foo";

    public interface FooCustom {

    }

    @Test
    public void testPrimitives() {
        checkTransitiveType(Type.booleanType().boxedType());
        checkTransitiveType(Type.byteType());
        checkTransitiveType(Type.charType());
        checkTransitiveType(Type.shortType());
        checkTransitiveType(Type.intType());
        checkTransitiveType(Type.longType());
        checkTransitiveType(Type.floatType());
        checkTransitiveType(Type.doubleType());

        checkTransformedType(Type.booleanType(), Type.booleanType().boxedType());
        checkTransformedType(Type.byteType().boxedType(), Type.byteType());
        checkTransformedType(Type.charType().boxedType(), Type.charType());
        checkTransformedType(Type.shortType().boxedType(), Type.shortType());
        checkTransformedType(Type.intType().boxedType(), Type.intType());
        checkTransformedType(Type.longType().boxedType(), Type.longType());
        checkTransformedType(Type.floatType().boxedType(), Type.floatType());
        checkTransformedType(Type.doubleType().boxedType(), Type.doubleType());
    }

    @Test
    public void testGenerics() {
        checkTransitiveType(Type.stringType());
        checkTransitiveType(Type.instantType());
        checkTransitiveType(Type.ofCustom(FooCustom.class));
    }

    @Test
    public void testPrimitiveArrays() {
        PrimitiveType.instances().forEach(primitiveType -> checkTransitiveType(primitiveType.arrayType()));
    }

    @Test
    public void testGenericArrays() {
        BoxedType.instances().map(GenericType::arrayType).forEach(ColumnDefinitionTypeTest::checkTransitiveType);

        checkTransitiveType(Type.stringType().arrayType());
        checkTransitiveType(Type.instantType().arrayType());
        checkTransitiveType(Type.ofCustom(FooCustom.class).arrayType());

        PrimitiveType.instances().map(Type::arrayType).map(GenericTypeBase::arrayType)
                .forEach(ColumnDefinitionTypeTest::checkTransitiveType);

        checkTransitiveType(Type.stringType().arrayType().arrayType());
        checkTransitiveType(Type.instantType().arrayType().arrayType());
        checkTransitiveType(Type.ofCustom(FooCustom.class).arrayType().arrayType());
    }

    @Test
    public void testPrimitiveVectors() {
        checkTransitiveType(ByteVector.type());
        checkTransitiveType(CharVector.type());
        checkTransitiveType(ShortVector.type());
        checkTransitiveType(IntVector.type());
        checkTransitiveType(LongVector.type());
        checkTransitiveType(FloatVector.type());
        checkTransitiveType(DoubleVector.type());
    }

    @Test
    public void testGenericVectors() {
        checkTransitiveType(ObjectVector.type(Type.stringType()));
        checkTransitiveType(ObjectVector.type(Type.instantType()));
        checkTransitiveType(ObjectVector.type(Type.ofCustom(FooCustom.class)));

        BoxedType.instances().map(ObjectVector::type).forEach(ColumnDefinitionTypeTest::checkTransitiveType);
    }

    @Test
    public void testOfBooleanType() {
        checkType(Type.booleanType().boxedType(), ColumnDefinition.ofBoolean(FOO));
    }

    @Test
    public void testOfCharType() {
        checkType(Type.charType(), ColumnDefinition.ofChar(FOO));
    }

    @Test
    public void testOfByteType() {
        checkType(Type.byteType(), ColumnDefinition.ofByte(FOO));
    }

    @Test
    public void testOfShortType() {
        checkType(Type.shortType(), ColumnDefinition.ofShort(FOO));
    }

    @Test
    public void testOfIntType() {
        checkType(Type.intType(), ColumnDefinition.ofInt(FOO));
    }

    @Test
    public void testOfLongType() {
        checkType(Type.longType(), ColumnDefinition.ofLong(FOO));
    }

    @Test
    public void testOfFloatType() {
        checkType(Type.floatType(), ColumnDefinition.ofFloat(FOO));
    }

    @Test
    public void testOfDoubleType() {
        checkType(Type.doubleType(), ColumnDefinition.ofDouble(FOO));
    }

    @Test
    public void testOfStringType() {
        checkType(Type.stringType(), ColumnDefinition.ofString(FOO));
    }

    @Test
    public void testOfTimeType() {
        checkType(Type.instantType(), ColumnDefinition.ofTime(FOO));
    }

    @Test
    public void testOfVector() {
        checkType(CharVector.type(), ColumnDefinition.ofVector(FOO, CharVector.class));
        checkType(ByteVector.type(), ColumnDefinition.ofVector(FOO, ByteVector.class));
        checkType(ShortVector.type(), ColumnDefinition.ofVector(FOO, ShortVector.class));
        checkType(IntVector.type(), ColumnDefinition.ofVector(FOO, IntVector.class));
        checkType(LongVector.type(), ColumnDefinition.ofVector(FOO, LongVector.class));
        checkType(FloatVector.type(), ColumnDefinition.ofVector(FOO, FloatVector.class));
        checkType(DoubleVector.type(), ColumnDefinition.ofVector(FOO, DoubleVector.class));
        checkType(ObjectVector.type(Type.ofCustom(Object.class)), ColumnDefinition.ofVector(FOO, ObjectVector.class));
    }

    @Test
    public void testFromGenericType() {
        checkType(ObjectVector.type(Type.stringType()),
                ColumnDefinition.fromGenericType(FOO, ObjectVector.class, String.class));
        checkType(ObjectVector.type(Type.instantType()),
                ColumnDefinition.fromGenericType(FOO, ObjectVector.class, Instant.class));
        checkType(ObjectVector.type(Type.ofCustom(FooCustom.class)),
                ColumnDefinition.fromGenericType(FOO, ObjectVector.class, FooCustom.class));
        BoxedType.instances().forEach(boxedType -> checkType(ObjectVector.type(boxedType),
                ColumnDefinition.fromGenericType(FOO, ObjectVector.class, boxedType.clazz())));
    }

    @Test
    public void testExplicitVectorTypes() {
        checkType(PrimitiveVectorType.of(CharVectorDirect.class, Type.charType()),
                ColumnDefinition.ofVector(FOO, CharVectorDirect.class));
        checkType(PrimitiveVectorType.of(ByteVectorDirect.class, Type.byteType()),
                ColumnDefinition.ofVector(FOO, ByteVectorDirect.class));
        checkType(PrimitiveVectorType.of(ShortVectorDirect.class, Type.shortType()),
                ColumnDefinition.ofVector(FOO, ShortVectorDirect.class));
        checkType(PrimitiveVectorType.of(IntVectorDirect.class, Type.intType()),
                ColumnDefinition.ofVector(FOO, IntVectorDirect.class));
        checkType(PrimitiveVectorType.of(LongVectorDirect.class, Type.longType()),
                ColumnDefinition.ofVector(FOO, LongVectorDirect.class));
        checkType(PrimitiveVectorType.of(FloatVectorDirect.class, Type.floatType()),
                ColumnDefinition.ofVector(FOO, FloatVectorDirect.class));
        checkType(PrimitiveVectorType.of(DoubleVectorDirect.class, Type.doubleType()),
                ColumnDefinition.ofVector(FOO, DoubleVectorDirect.class));
    }

    private static void checkTransitiveType(Type<?> type) {
        checkType(type, cd(type));
    }

    private static void checkTransformedType(Type<?> cdInput, Type<?> expected) {
        checkType(expected, cd(cdInput));
    }

    private static void checkType(Type<?> expected, ColumnDefinition<?> actual) {
        assertEquals(expected, actual.getType());
    }

    private static ColumnDefinition<?> cd(Type<?> type) {
        return ColumnDefinition.of(FOO, type);
    }
}
