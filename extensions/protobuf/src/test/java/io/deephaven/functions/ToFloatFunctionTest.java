package io.deephaven.functions;

import org.junit.jupiter.api.Test;

import static io.deephaven.functions.ToFloatFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToFloatFunctionTest {

    @Test
    void map_() {
        final ToFloatFunction<String> bytesLength = map(String::getBytes, x -> (float) x.length);
        assertThat(bytesLength.applyAsFloat("foo")).isEqualTo(3.0f);
        assertThat(bytesLength.applyAsFloat("food")).isEqualTo(4.0f);
    }
}
