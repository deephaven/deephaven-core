package io.deephaven.functions;

import org.junit.jupiter.api.Test;

import static io.deephaven.functions.ToDoubleFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToDoubleFunctionTest {

    @Test
    void map_() {
        final ToDoubleFunction<String> bytesLength = map(String::getBytes, x -> (double) x.length);
        assertThat(bytesLength.applyAsDouble("foo")).isEqualTo(3.0);
        assertThat(bytesLength.applyAsDouble("food")).isEqualTo(4.0);
    }
}
