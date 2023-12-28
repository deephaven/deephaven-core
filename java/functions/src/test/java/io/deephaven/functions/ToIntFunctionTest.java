package io.deephaven.functions;

import org.junit.jupiter.api.Test;

import static io.deephaven.functions.ToIntFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToIntFunctionTest {

    @Test
    void map_() {
        final ToIntFunction<String> bytesLength = map(String::getBytes, x -> x.length);
        assertThat(bytesLength.applyAsInt("foo")).isEqualTo(3);
        assertThat(bytesLength.applyAsInt("food")).isEqualTo(4);
    }
}
