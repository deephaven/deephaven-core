/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.List;

import static io.deephaven.qst.type.Type.booleanType;
import static io.deephaven.qst.type.Type.byteType;
import static io.deephaven.qst.type.Type.charType;
import static io.deephaven.qst.type.Type.doubleType;
import static io.deephaven.qst.type.Type.find;
import static io.deephaven.qst.type.Type.floatType;
import static io.deephaven.qst.type.Type.instantType;
import static io.deephaven.qst.type.Type.intType;
import static io.deephaven.qst.type.Type.knownTypes;
import static io.deephaven.qst.type.Type.longType;
import static io.deephaven.qst.type.Type.ofCustom;
import static io.deephaven.qst.type.Type.shortType;
import static io.deephaven.qst.type.Type.stringType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class TypeTest {

    @Test
    void numberOfStaticTypes() {
        // A reminder that when the number of static types increases, we should
        // add tests in this class for it specifically
        assertThat(knownTypes()).hasSize(18);
        assertThat(knownTypes().stream().distinct()).hasSize(18);

        assertThat(PrimitiveType.instances()).hasSize(8);
        assertThat(PrimitiveType.instances().distinct()).hasSize(8);

        assertThat(BoxedType.instances()).hasSize(8);
        assertThat(BoxedType.instances().distinct()).hasSize(8);
    }


    @Test
    void findBooleans() {
        check(boolean.class, Boolean.class, booleanType(), BoxedBooleanType.of());
    }

    @Test
    void findBytes() {
        check(byte.class, Byte.class, byteType(), BoxedByteType.of());
    }

    @Test
    void findChars() {
        check(char.class, Character.class, charType(), BoxedCharType.of());
    }

    @Test
    void findShorts() {
        check(short.class, Short.class, shortType(), BoxedShortType.of());
    }

    @Test
    void findInts() {
        check(int.class, Integer.class, intType(), BoxedIntType.of());
    }

    @Test
    void findLongs() {
        check(long.class, Long.class, longType(), BoxedLongType.of());
    }

    @Test
    void findFloats() {
        check(float.class, Float.class, floatType(), BoxedFloatType.of());
    }

    @Test
    void findDoubles() {
        check(double.class, Double.class, doubleType(), BoxedDoubleType.of());
    }

    @Test
    void findString() {
        assertThat(find(String.class)).isEqualTo(stringType());
        assertThat(find(String[].class)).isEqualTo(stringType().arrayType());
    }

    @Test
    void findInstant() {
        assertThat(find(Instant.class)).isEqualTo(instantType());
        assertThat(find(Instant[].class)).isEqualTo(instantType().arrayType());
    }

    @Test
    void findCustom() {
        assertThat(find(Custom.class)).isEqualTo(ofCustom(Custom.class));
        assertThat(find(Custom[].class)).isEqualTo(ofCustom(Custom.class).arrayType());
    }

    @Test
    void booleanArrayType() {
        assertThat(find(boolean[].class)).isEqualTo(booleanType().arrayType());
        assertThat(find(Boolean[].class)).isEqualTo(BoxedBooleanType.of().arrayType());
    }

    @Test
    void byteArrayType() {
        assertThat(find(byte[].class)).isEqualTo(byteType().arrayType());
        assertThat(find(Byte[].class)).isEqualTo(BoxedByteType.of().arrayType());
    }

    @Test
    void charArrayType() {
        assertThat(find(char[].class)).isEqualTo(charType().arrayType());
        assertThat(find(Character[].class)).isEqualTo(BoxedCharType.of().arrayType());
    }

    @Test
    void shortArrayType() {
        assertThat(find(short[].class)).isEqualTo(shortType().arrayType());
        assertThat(find(Short[].class)).isEqualTo(BoxedShortType.of().arrayType());
    }

    @Test
    void intArrayType() {
        assertThat(find(int[].class)).isEqualTo(intType().arrayType());
        assertThat(find(Integer[].class)).isEqualTo(BoxedIntType.of().arrayType());
    }

    @Test
    void longArrayType() {
        assertThat(find(long[].class)).isEqualTo(longType().arrayType());
        assertThat(find(Long[].class)).isEqualTo(BoxedLongType.of().arrayType());
    }

    @Test
    void floatArrayType() {
        assertThat(find(float[].class)).isEqualTo(floatType().arrayType());
        assertThat(find(Float[].class)).isEqualTo(BoxedFloatType.of().arrayType());
    }

    @Test
    void doubleArrayType() {
        assertThat(find(double[].class)).isEqualTo(doubleType().arrayType());
        assertThat(find(Double[].class)).isEqualTo(BoxedDoubleType.of().arrayType());

    }

    @Test
    void nestedPrimitive2x() {
        assertThat(find(int[][].class)).isEqualTo(intType().arrayType().arrayType());
        assertThat(find(Integer[][].class)).isEqualTo(BoxedIntType.of().arrayType().arrayType());
    }

    @Test
    void nestedPrimitive3x() {
        assertThat(find(int[][][].class)).isEqualTo(intType().arrayType().arrayType().arrayType());
        assertThat(find(Integer[][][].class)).isEqualTo(BoxedIntType.of().arrayType().arrayType().arrayType());
    }

    @Test
    void nestedStatic2x() {
        assertThat(find(String[][].class)).isEqualTo(stringType().arrayType().arrayType());
    }

    @Test
    void nestedStatic3x() {
        assertThat(find(String[][][].class)).isEqualTo(stringType().arrayType().arrayType().arrayType());
    }

    @Test
    void nestedCustom2x() {
        assertThat(find(Custom[][].class)).isEqualTo(CustomType.of(Custom.class).arrayType().arrayType());
    }

    @Test
    void nestedCustom3x() {
        assertThat(find(Custom[][][].class)).isEqualTo(CustomType.of(Custom.class).arrayType().arrayType().arrayType());
    }

    @Test
    void nonEqualityCheck() {
        final List<Type<?>> staticTypes = knownTypes();
        final int L = staticTypes.size();
        for (int i = 0; i < L - 1; ++i) {
            for (int j = i + 1; j < L; ++j) {
                assertThat(staticTypes.get(i)).isNotEqualTo(staticTypes.get(j));
                assertThat(staticTypes.get(j)).isNotEqualTo(staticTypes.get(i));
            }
        }
    }

    @Test
    void primitiveVectorTypesAreEmpty()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        // Primitive vector types are not on the classpath as QST dependency, make sure
        // they are not found.
        try {
            PrimitiveVectorType.types();
            failBecauseExceptionWasNotThrown(ClassNotFoundException.class);
        } catch (ClassNotFoundException e) {
            // expected
        }
    }

    interface Custom {

    }

    private static <T> void check(
            Class<T> primitive,
            Class<T> boxed,
            PrimitiveType<T> expectedPrimitive,
            BoxedType<T> expectedBoxed) {
        assertThat(find(primitive)).isEqualTo(expectedPrimitive);
        assertThat(expectedPrimitive.clazz()).isEqualTo(primitive);
        assertThat(expectedPrimitive.boxedType()).isEqualTo(expectedBoxed);
        assertThat(expectedPrimitive.arrayType().componentType()).isEqualTo(expectedPrimitive);
        assertThat(expectedPrimitive.arrayType().clazz()).isEqualTo(Array.newInstance(primitive, 0).getClass());

        assertThat(find(boxed)).isEqualTo(expectedBoxed);
        assertThat(expectedBoxed.clazz()).isEqualTo(boxed);
        assertThat(expectedBoxed.primitiveType()).isEqualTo(expectedPrimitive);
        assertThat(expectedBoxed.arrayType().componentType()).isEqualTo(expectedBoxed);
        assertThat(expectedBoxed.arrayType().clazz()).isEqualTo(Array.newInstance(boxed, 0).getClass());
    }
}
