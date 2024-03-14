//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import org.junit.jupiter.api.Test;

import static io.deephaven.function.ToDoubleFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToDoubleFunctionTest {

    @Test
    void map_() {
        final ToDoubleFunction<String> bytesLength = map(String::getBytes, x -> (double) x.length);
        assertThat(bytesLength.applyAsDouble("foo")).isEqualTo(3.0);
        assertThat(bytesLength.applyAsDouble("food")).isEqualTo(4.0);
    }
}
