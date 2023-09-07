package io.deephaven.functions;

import org.junit.jupiter.api.Test;

import static io.deephaven.functions.ToLongFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToLongFunctionTest {

    @Test
    void map_() {
        final ToLongFunction<String> bytesLength = map(String::getBytes, x -> (long) x.length);
        assertThat(bytesLength.applyAsLong("foo")).isEqualTo(3L);
        assertThat(bytesLength.applyAsLong("food")).isEqualTo(4L);
    }
}
