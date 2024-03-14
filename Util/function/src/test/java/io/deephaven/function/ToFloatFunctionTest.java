//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import org.junit.jupiter.api.Test;

import static io.deephaven.function.ToFloatFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToFloatFunctionTest {

    @Test
    void map_() {
        final ToFloatFunction<String> bytesLength = map(String::getBytes, x -> (float) x.length);
        assertThat(bytesLength.applyAsFloat("foo")).isEqualTo(3.0f);
        assertThat(bytesLength.applyAsFloat("food")).isEqualTo(4.0f);
    }
}
