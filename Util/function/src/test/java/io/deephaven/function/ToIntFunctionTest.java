//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import org.junit.jupiter.api.Test;

import static io.deephaven.function.ToIntFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToIntFunctionTest {

    @Test
    void map_() {
        final ToIntFunction<String> bytesLength = map(String::getBytes, x -> x.length);
        assertThat(bytesLength.applyAsInt("foo")).isEqualTo(3);
        assertThat(bytesLength.applyAsInt("food")).isEqualTo(4);
    }
}
