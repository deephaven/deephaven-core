//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.vectors.IntVectorColumnWrapper;
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
import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        // Array component types are never normalized: unlike a boolean column, whose data type is Boolean, a boolean[]
        // column keeps its primitive boolean elements
        checkNativeArray(Type.booleanType().arrayType());
        assertThat(ColumnDefinition.fromGenericType(CN, boolean[].class).getComponentType()).isEqualTo(boolean.class);
    }

    @Test
    public void boxedBooleanArray() {
        checkNativeArray(Type.booleanType().boxedType().arrayType());
    }

    @Test
    public void boxedByteArray() {
        checkNativeArray(Type.byteType().boxedType().arrayType());
    }

    @Test
    public void boxedCharArray() {
        checkNativeArray(Type.charType().boxedType().arrayType());
    }

    @Test
    public void boxedShortArray() {
        checkNativeArray(Type.shortType().boxedType().arrayType());
    }

    @Test
    public void boxedIntArray() {
        // Array data types are never normalized either: Integer[] stays Integer[], with component type Integer
        checkNativeArray(Type.intType().boxedType().arrayType());
        final ColumnDefinition<Integer[]> integers = ColumnDefinition.fromGenericType(CN, Integer[].class);
        assertThat(integers.getDataType()).isEqualTo(Integer[].class);
        assertThat(integers.getComponentType()).isEqualTo(Integer.class);
    }

    @Test
    public void boxedLongArray() {
        checkNativeArray(Type.longType().boxedType().arrayType());
    }

    @Test
    public void boxedFloatArray() {
        checkNativeArray(Type.floatType().boxedType().arrayType());
    }

    @Test
    public void boxedDoubleArray() {
        checkNativeArray(Type.doubleType().boxedType().arrayType());
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

    @Test
    public void customTypeVectorImplementation() {
        // CustomType only rejects Vector types in io.deephaven.vector, so a Vector implementation in another package is
        // a custom type; its component type must still be inferred, as fromGenericType does
        final ColumnDefinition<?> wrapper = ColumnDefinition.fromGenericType(CN, IntVectorColumnWrapper.class);
        assertThat(wrapper.getComponentType()).isEqualTo(int.class);
        final CustomType<IntVectorColumnWrapper> wrapperType = Type.ofCustom(IntVectorColumnWrapper.class);
        assertThat(ColumnDefinition.of(CN, wrapperType)).isEqualTo(wrapper);
        assertThat(ColumnDefinition.of(CN, (GenericType<?>) wrapperType)).isEqualTo(wrapper);
        assertThat(ColumnDefinition.of(CN, (Type<?>) wrapperType)).isEqualTo(wrapper);

        final CustomType<MyObjectVector> objectVectorType = Type.ofCustom(MyObjectVector.class);
        final ColumnDefinition<?> objects = ColumnDefinition.of(CN, objectVectorType);
        assertThat(objects.getDataType()).isEqualTo(MyObjectVector.class);
        assertThat(objects.getComponentType()).isEqualTo(Object.class);
        assertThat(objects).isEqualTo(ColumnDefinition.fromGenericType(CN, MyObjectVector.class));
    }

    /**
     * A {@link Vector} type outside {@code io.deephaven.vector}, which {@link CustomType} therefore accepts.
     */
    private interface MyObjectVector extends ObjectVector<String> {
    }

    @Test
    public void boxedTypesMapToSupportedTypes() {
        // The qst boxed types are the only way to name a boxed primitive through of(...), and they map to the
        // supported representation: Boolean for boolean, and the primitive for the other boxed types.
        assertThat(ColumnDefinition.of(CN, Type.booleanType().boxedType())).isEqualTo(ColumnDefinition.ofBoolean(CN));
        assertThat(ColumnDefinition.of(CN, Type.byteType().boxedType())).isEqualTo(ColumnDefinition.ofByte(CN));
        assertThat(ColumnDefinition.of(CN, Type.charType().boxedType())).isEqualTo(ColumnDefinition.ofChar(CN));
        assertThat(ColumnDefinition.of(CN, Type.shortType().boxedType())).isEqualTo(ColumnDefinition.ofShort(CN));
        assertThat(ColumnDefinition.of(CN, Type.intType().boxedType())).isEqualTo(ColumnDefinition.ofInt(CN));
        assertThat(ColumnDefinition.of(CN, Type.longType().boxedType())).isEqualTo(ColumnDefinition.ofLong(CN));
        assertThat(ColumnDefinition.of(CN, Type.floatType().boxedType())).isEqualTo(ColumnDefinition.ofFloat(CN));
        assertThat(ColumnDefinition.of(CN, Type.doubleType().boxedType())).isEqualTo(ColumnDefinition.ofDouble(CN));
    }

    @Test
    public void voidDataTypes() {
        for (final Class<?> dataType : new Class<?>[] {void.class, Void.class}) {
            checkInvalidDataType(dataType);
        }
    }

    @Test
    public void normalizedDataTypes() {
        checkNormalizedDataType(boolean.class, ColumnDefinition.ofBoolean(CN));
        checkNormalizedDataType(Byte.class, ColumnDefinition.ofByte(CN));
        checkNormalizedDataType(Character.class, ColumnDefinition.ofChar(CN));
        checkNormalizedDataType(Short.class, ColumnDefinition.ofShort(CN));
        checkNormalizedDataType(Integer.class, ColumnDefinition.ofInt(CN));
        checkNormalizedDataType(Long.class, ColumnDefinition.ofLong(CN));
        checkNormalizedDataType(Float.class, ColumnDefinition.ofFloat(CN));
        checkNormalizedDataType(Double.class, ColumnDefinition.ofDouble(CN));
    }

    @Test
    public void normalizedDataTypesAreNotCustomTypes() {
        // boolean and the boxed primitives are qst static types, so they cannot reach the constructor un-normalized
        // through CustomType
        for (final Class<?> dataType : new Class<?>[] {boolean.class, Byte.class, Character.class, Short.class,
                Integer.class, Long.class, Float.class, Double.class}) {
            assertThatThrownBy(() -> Type.ofCustom(dataType))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Use static type");
        }
    }

    @Test
    public void voidCustomTypes() {
        // void and Void are not qst static types, so CustomType accepts them; ColumnDefinition must reject them
        for (final Class<?> dataType : new Class<?>[] {void.class, Void.class}) {
            final CustomType<?> type = Type.ofCustom(dataType);
            assertThatThrownBy(() -> ColumnDefinition.of(CN, type))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ColumnDefinition.of(CN, (Type<?>) type))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void invalidColumnNames() {
        // Empty, query language reserved names, Java keywords and literals, and non-identifiers
        for (final String name : new String[] {"", "i", "ii", "k", "in", "not", "class", "_", "true", "null", "1abc",
                "has space", "a-b", "a.b"}) {
            checkInvalidColumnName(name);
        }
        assertThatThrownBy(() -> ColumnDefinition.ofInt(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void validColumnNames() {
        for (final String name : new String[] {"_x", "$x", "__RowDepth__", "I", "Ii", "inValue", "x1"}) {
            assertThat(ColumnDefinition.ofInt(name).getName()).isEqualTo(name);
            assertThat(ColumnDefinition.ofInt(CN).withName(name).getName()).isEqualTo(name);
        }
    }

    private static void checkInvalidColumnName(final String name) {
        assertThatThrownBy(() -> ColumnDefinition.ofInt(name))
                .isInstanceOf(NameValidator.InvalidNameException.class);
        assertThatThrownBy(() -> ColumnDefinition.ofString(name))
                .isInstanceOf(NameValidator.InvalidNameException.class);
        assertThatThrownBy(() -> ColumnDefinition.of(name, Type.intType()))
                .isInstanceOf(NameValidator.InvalidNameException.class);
        assertThatThrownBy(() -> ColumnDefinition.of(name, MyCustomType.type()))
                .isInstanceOf(NameValidator.InvalidNameException.class);
        assertThatThrownBy(() -> ColumnDefinition.of(name, Type.intType().arrayType()))
                .isInstanceOf(NameValidator.InvalidNameException.class);
        assertThatThrownBy(() -> ColumnDefinition.ofVector(name, IntVector.class))
                .isInstanceOf(NameValidator.InvalidNameException.class);
        assertThatThrownBy(() -> ColumnDefinition.fromGenericType(name, String.class))
                .isInstanceOf(NameValidator.InvalidNameException.class);
        assertThatThrownBy(() -> ColumnDefinition.ofInt(CN).withName(name))
                .isInstanceOf(NameValidator.InvalidNameException.class);
    }

    @Test
    public void inferredComponentTypes() {
        assertThat(ColumnDefinition.fromGenericType(CN, int[].class).getComponentType()).isEqualTo(int.class);
        assertThat(ColumnDefinition.fromGenericType(CN, String[][].class).getComponentType())
                .isEqualTo(String[].class);
        assertThat(ColumnDefinition.fromGenericType(CN, IntVector.class).getComponentType()).isEqualTo(int.class);
        assertThat(ColumnDefinition.fromGenericType(CN, ObjectVector.class).getComponentType())
                .isEqualTo(Object.class);
        assertThat(ColumnDefinition.ofVector(CN, DoubleVector.class, null).getComponentType()).isEqualTo(double.class);
        assertThat(ColumnDefinition.ofInt(CN).withDataType(String[].class).getComponentType())
                .isEqualTo(String.class);
        // Non-array, non-Vector data types have no component type unless one is given
        assertThat(ColumnDefinition.fromGenericType(CN, String.class).getComponentType()).isNull();
    }

    @Test
    public void validComponentTypes() {
        // An array's component type may be more specific than the array class's, and an ObjectVector's component type
        // is its element type, which ObjectVector only expresses through generics
        assertThat(ColumnDefinition.fromGenericType(CN, Object[].class, Instant.class).getComponentType())
                .isEqualTo(Instant.class);
        assertThat(ColumnDefinition.fromGenericType(CN, ObjectVector.class, String.class).getComponentType())
                .isEqualTo(String.class);
        assertThat(ColumnDefinition.ofVector(CN, ObjectVector.class, Instant.class).getComponentType())
                .isEqualTo(Instant.class);
        // Some callers depend on giving a component type for other data types, such as collections
        assertThat(ColumnDefinition.fromGenericType(CN, List.class, String.class).getComponentType())
                .isEqualTo(String.class);
    }

    @Test
    public void numberArrayComponentTypes() {
        // A reference example of how the component type relates to an array data type. A native array class already
        // carries its element type, here Number, so the component type is either that or a more specific type that
        // all elements are known to have. Compare objectVectorComponentTypes.

        // Inferred: the array's component type
        assertThat(ColumnDefinition.fromGenericType(CN, Number[].class).getComponentType()).isEqualTo(Number.class);
        assertThat(ColumnDefinition.fromGenericType(CN, Number[].class, null).getComponentType())
                .isEqualTo(Number.class);

        // More specific: accepted, and kept as given; component types are never normalized, so Integer stays boxed,
        // which is right since the elements are Integer objects
        final ColumnDefinition<Number[]> integers = ColumnDefinition.fromGenericType(CN, Number[].class, Integer.class);
        assertThat(integers.getDataType()).isEqualTo(Number[].class);
        assertThat(integers.getComponentType()).isEqualTo(Integer.class);

        // Primitive: rejected, as a primitive type is never assignable to a reference type; a Number[] cannot hold ints
        checkInvalidComponentType(Number[].class, int.class, "array");

        // Wider: rejected
        checkInvalidComponentType(Number[].class, Object.class, "array");

        // Unrelated: rejected, even though some elements could be both a Number and a Comparable
        checkInvalidComponentType(Number[].class, Comparable.class, "array");
    }

    @Test
    public void objectVectorComponentTypes() {
        // A reference example of how the component type relates to an ObjectVector data type. Unlike a native array
        // class, ObjectVector expresses its element type only through generics, so the component type supplies it:
        // ObjectVector with component type Integer is ObjectVector<Integer>, the Vector equivalent of Integer[], not an
        // Object[]-like column with a narrower component type. Compare numberArrayComponentTypes.

        // Inferred: Object, making it ObjectVector<Object>, the Vector equivalent of Object[]
        assertThat(ColumnDefinition.fromGenericType(CN, ObjectVector.class).getComponentType())
                .isEqualTo(Object.class);
        assertThat(ColumnDefinition.ofVector(CN, ObjectVector.class).getComponentType()).isEqualTo(Object.class);
        assertThat(ColumnDefinition.ofVector(CN, ObjectVector.class, null).getComponentType())
                .isEqualTo(Object.class);

        // ObjectVector<Integer>, the Vector equivalent of Integer[]; component types are never normalized, so Integer
        // stays boxed, which is right since the elements are Integer objects
        final ColumnDefinition<?> integers = ColumnDefinition.fromGenericType(CN, ObjectVector.class, Integer.class);
        assertThat(integers.getDataType()).isEqualTo(ObjectVector.class);
        assertThat(integers.getComponentType()).isEqualTo(Integer.class);
        assertThat(ColumnDefinition.ofVector(CN, ObjectVector.class, Integer.class)).isEqualTo(integers);

        // ObjectVector<Comparable>, the Vector equivalent of Comparable[]: any reference type can be an element type
        assertThat(ColumnDefinition.fromGenericType(CN, ObjectVector.class, Comparable.class).getComponentType())
                .isEqualTo(Comparable.class);

        // Primitive: rejected, as ObjectVector cannot hold primitives (there is no ObjectVector<int>); the Vector
        // equivalent of int[] is IntVector
        checkInvalidComponentType(ObjectVector.class, int.class, "Vector");
        assertThatThrownBy(() -> ColumnDefinition.ofVector(CN, ObjectVector.class, int.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid component type int for column " + CN + " with Vector data type "
                        + ObjectVector.class.getTypeName());
    }

    @Test
    public void voidElementTypes() {
        // void and Void cannot be element types, whatever the data type: an ObjectVector<Void> or a Void[] can hold
        // nothing but null, just like a Void column
        checkVoidElementType(() -> ColumnDefinition.fromGenericType(CN, ObjectVector.class, Void.class));
        checkVoidElementType(() -> ColumnDefinition.ofVector(CN, ObjectVector.class, Void.class));
        checkVoidElementType(() -> ColumnDefinition.ofInt(CN).withDataType(ObjectVector.class, Void.class));
        checkVoidElementType(() -> ColumnDefinition.fromGenericType(CN, ObjectVector.class, void.class));
        checkVoidElementType(() -> ColumnDefinition.fromGenericType(CN, Object[].class, Void.class));
        // Arrays of Void, as data types (with their inferred component types) or as component types
        checkVoidElementType(() -> ColumnDefinition.fromGenericType(CN, Void[].class));
        checkVoidElementType(() -> ColumnDefinition.fromGenericType(CN, Void[][].class));
        checkVoidElementType(() -> ColumnDefinition.fromGenericType(CN, Object[].class, Void[].class));
        checkVoidElementType(() -> ColumnDefinition.fromGenericType(CN, ObjectVector.class, Void[].class));
        checkVoidElementType(() -> ColumnDefinition.of(CN, Type.ofCustom(Void.class).arrayType()));
        // Even data types that otherwise accept any component type
        checkVoidElementType(() -> ColumnDefinition.fromGenericType(CN, List.class, Void.class));

        // Other reference types remain valid element types
        assertThat(ColumnDefinition.ofVector(CN, ObjectVector.class, Integer.class).getComponentType())
                .isEqualTo(Integer.class);
        assertThat(ColumnDefinition.fromGenericType(CN, Object[].class, Object[].class).getComponentType())
                .isEqualTo(Object[].class);
    }

    private static void checkVoidElementType(final ThrowableAssert.ThrowingCallable construct) {
        assertThatThrownBy(construct)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("void and Void are not valid element types");
    }

    @Test
    public void invalidArrayComponentTypes() {
        // A primitive array's component type is its primitive type, and a boxed array's is its boxed type: neither
        // accepts the other, as primitive and boxed classes are not assignable to each other
        checkInvalidComponentType(int[].class, Integer.class, "array");
        checkInvalidComponentType(Integer[].class, int.class, "array");
        checkInvalidComponentType(boolean[].class, Boolean.class, "array");
        checkInvalidComponentType(Boolean[].class, boolean.class, "array");
        checkInvalidComponentType(int[].class, long.class, "array");
        checkInvalidComponentType(Instant[].class, String.class, "array");
        // A wider component type than the array's is invalid
        checkInvalidComponentType(String[].class, Object.class, "array");
    }

    @Test
    public void invalidVectorComponentTypes() {
        checkInvalidComponentType(IntVector.class, Integer.class, "Vector");
        checkInvalidComponentType(IntVector.class, long.class, "Vector");
        checkInvalidComponentType(ObjectVector.class, int.class, "Vector");
        assertThatThrownBy(() -> ColumnDefinition.ofVector(CN, IntVector.class, long.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid component type long")
                .hasMessageContaining("Vector data type " + IntVector.class.getName());
    }

    @Test
    public void unrecognizedVectorType() {
        assertThatThrownBy(() -> ColumnDefinition.fromGenericType(CN, Vector.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unrecognized Vector type");
        assertThatThrownBy(() -> ColumnDefinition.fromGenericType(CN, Vector.class, Object.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unrecognized Vector type");
    }

    private static void checkInvalidComponentType(
            final Class<?> dataType,
            final Class<?> componentType,
            final String kind) {
        final String message = "Invalid component type " + componentType.getTypeName() + " for column " + CN
                + " with " + kind + " data type " + dataType.getTypeName();
        assertThatThrownBy(() -> ColumnDefinition.fromGenericType(CN, dataType, componentType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
        assertThatThrownBy(() -> ColumnDefinition.fromGenericType(CN, dataType, componentType,
                ColumnDefinition.ColumnType.Partitioning))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
        assertThatThrownBy(() -> ColumnDefinition.ofInt(CN).withDataType(dataType, componentType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    private static void checkNormalizedDataType(final Class<?> dataType, final ColumnDefinition<?> expected) {
        assertThat(ColumnDefinition.fromGenericType(CN, dataType)).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, dataType, null)).isEqualTo(expected);
        assertThat(ColumnDefinition.fromGenericType(CN, dataType, null, ColumnDefinition.ColumnType.Partitioning))
                .isEqualTo(expected.withPartitioning());
        assertThat(ColumnDefinition.ofString(CN).withDataType(dataType)).isEqualTo(expected);
        assertThat(ColumnDefinition.ofString(CN).withDataType(dataType, null)).isEqualTo(expected);
    }

    private static void checkInvalidDataType(final Class<?> dataType) {
        assertThatThrownBy(() -> ColumnDefinition.fromGenericType(CN, dataType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(dataType.getName());
        assertThatThrownBy(() -> ColumnDefinition.fromGenericType(CN, dataType, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnDefinition.fromGenericType(CN, dataType, null,
                ColumnDefinition.ColumnType.Partitioning))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnDefinition.ofInt(CN).withDataType(dataType))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnDefinition.ofInt(CN).withDataType(dataType, null))
                .isInstanceOf(IllegalArgumentException.class);
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
