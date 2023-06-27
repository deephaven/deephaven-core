/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import java.lang.reflect.InvocationTargetException;
import org.junit.jupiter.api.Test;

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
    }

    @Test
    void findBooleans() {
        assertThat(find(boolean.class)).isEqualTo(booleanType());
        assertThat(find(Boolean.class)).isEqualTo(booleanType().boxedType());
    }

    @Test
    void findBytes() {
        assertThat(find(byte.class)).isEqualTo(byteType());
        assertThat(find(Byte.class)).isEqualTo(byteType().boxedType());
    }

    @Test
    void findChars() {
        assertThat(find(char.class)).isEqualTo(charType());
        assertThat(find(Character.class)).isEqualTo(charType().boxedType());
    }

    @Test
    void findShorts() {
        assertThat(find(short.class)).isEqualTo(shortType());
        assertThat(find(Short.class)).isEqualTo(shortType().boxedType());
    }

    @Test
    void findInts() {
        assertThat(find(int.class)).isEqualTo(intType());
        assertThat(find(Integer.class)).isEqualTo(intType().boxedType());
    }

    @Test
    void findLongs() {
        assertThat(find(long.class)).isEqualTo(longType());
        assertThat(find(Long.class)).isEqualTo(longType().boxedType());
    }

    @Test
    void findFloats() {
        assertThat(find(float.class)).isEqualTo(floatType());
        assertThat(find(Float.class)).isEqualTo(floatType().boxedType());
    }

    @Test
    void findDoubles() {
        assertThat(find(double.class)).isEqualTo(doubleType());
        assertThat(find(Double.class)).isEqualTo(doubleType().boxedType());
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
        assertThat(find(Boolean[].class)).isEqualTo(booleanType().boxedType().arrayType());
    }

    @Test
    void byteArrayType() {
        assertThat(find(byte[].class)).isEqualTo(byteType().arrayType());
        assertThat(find(Byte[].class)).isEqualTo(byteType().boxedType().arrayType());
    }

    @Test
    void charArrayType() {
        assertThat(find(char[].class)).isEqualTo(charType().arrayType());
        assertThat(find(Character[].class)).isEqualTo(charType().boxedType().arrayType());
    }

    @Test
    void shortArrayType() {
        assertThat(find(short[].class)).isEqualTo(shortType().arrayType());
        assertThat(find(Short[].class)).isEqualTo(shortType().boxedType().arrayType());
    }

    @Test
    void intArrayType() {
        assertThat(find(int[].class)).isEqualTo(intType().arrayType());
        assertThat(find(Integer[].class)).isEqualTo(intType().boxedType().arrayType());
    }

    @Test
    void longArrayType() {
        assertThat(find(long[].class)).isEqualTo(longType().arrayType());
        assertThat(find(Long[].class)).isEqualTo(longType().boxedType().arrayType());
    }

    @Test
    void floatArrayType() {
        assertThat(find(float[].class)).isEqualTo(floatType().arrayType());
        assertThat(find(Float[].class)).isEqualTo(floatType().boxedType().arrayType());
    }

    @Test
    void doubleArrayType() {
        assertThat(find(double[].class)).isEqualTo(doubleType().arrayType());
        assertThat(find(Double[].class)).isEqualTo(doubleType().boxedType().arrayType());

    }

    @Test
    void nestedPrimitive2x() {
        assertThat(find(int[][].class)).isEqualTo(intType().arrayType().arrayType());
        assertThat(find(Integer[][].class)).isEqualTo(intType().boxedType().arrayType().arrayType());
    }

    @Test
    void nestedPrimitive3x() {
        assertThat(find(int[][][].class)).isEqualTo(intType().arrayType().arrayType().arrayType());
        assertThat(find(Integer[][][].class)).isEqualTo(intType().boxedType().arrayType().arrayType().arrayType());
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
}
