package io.deephaven.qst.array;

import io.deephaven.qst.type.Type;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrayBuilderTest {

    @Test
    void booleanArray() {
        check(Type.booleanType(), false, null, true);
    }

    @Test
    void byteArray() {
        check(Type.byteType(), (byte) 1, null, (byte) 3);
    }

    @Test
    void charArray() {
        check(Type.charType(), '1', null, '3');
    }

    @Test
    void shortArray() {
        check(Type.shortType(), (short) 1, null, (short) 3);
    }

    @Test
    void intArray() {
        check(Type.intType(), 1, null, 3);
    }

    @Test
    void longArray() {
        check(Type.longType(), 1L, null, 3L);
    }

    @Test
    void floatArray() {
        check(Type.floatType(), 1f, null, 3f);
    }

    @Test
    void doubleTest() {
        check(Type.doubleType(), 1d, null, 3d);
    }

    @Test
    void stringTest() {
        check(Type.stringType(), "1", null, "3");
    }

    @Test
    void instantTest() {
        check(Type.instantType(), Instant.ofEpochMilli(1), null, Instant.ofEpochMilli(3));
    }

    @Test
    void customTest() {
        check(Type.ofCustom(Custom.class), Custom.A, null, Custom.B);
    }

    enum Custom {
        A, B
    }

    private static <T> void check(GenericType<T> type, T... expected) {
        {
            Array<T> array = Array.builder(type, expected.length).add(expected).build();
            assertThat(array).containsExactlyElementsOf(Arrays.asList(expected));
        }

        {
            Array<T> array = GenericArray.builder(type).add(expected).build();
            assertThat(array).containsExactlyElementsOf(Arrays.asList(expected));
        }
    }

    private static <T> void check(PrimitiveType<T> type, T... expected) {
        {
            Array<T> array = Array.builder(type, expected.length).add(expected).build();
            assertThat(array).containsExactlyElementsOf(Arrays.asList(expected));
        }

        {
            Array<T> array = PrimitiveArray.builder(type, expected.length).add(expected).build();
            assertThat(array).containsExactlyElementsOf(Arrays.asList(expected));
        }
    }
}
