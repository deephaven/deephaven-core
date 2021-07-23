package io.deephaven.qst.array;

import io.deephaven.qst.type.Type;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class GenericArrayBuilderTest {

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
        GenericArray<T> array = GenericArray.builder(type).add(expected).build();
        assertThat(array).containsExactlyElementsOf(Arrays.asList(expected));
    }
}
